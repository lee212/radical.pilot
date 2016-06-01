
import os
import sys
import copy
import time
import pprint
import signal

import setproctitle    as spt
import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

from ..          import constants      as rpc
from ..          import states         as rps

from .misc       import hostip
from .prof_utils import timestamp      as rpu_timestamp

from .queue      import Queue          as rpu_Queue
from .queue      import QUEUE_ZMQ      as rpu_QUEUE_ZMQ
from .queue      import QUEUE_OUTPUT   as rpu_QUEUE_OUTPUT
from .queue      import QUEUE_INPUT    as rpu_QUEUE_INPUT
from .queue      import QUEUE_BRIDGE   as rpu_QUEUE_BRIDGE

from .pubsub     import Pubsub         as rpu_Pubsub
from .pubsub     import PUBSUB_ZMQ     as rpu_PUBSUB_ZMQ
from .pubsub     import PUBSUB_PUB     as rpu_PUBSUB_PUB
from .pubsub     import PUBSUB_SUB     as rpu_PUBSUB_SUB
from .pubsub     import PUBSUB_BRIDGE  as rpu_PUBSUB_BRIDGE


# ==============================================================================
#
class Component(mp.Process):
    """
    This class provides the basic structure for any RP component which operates
    on stateful things.  It provides means to:

      - define input channels on which to receive new things in certain states
      - define work methods which operate on the things to advance their state
      - define output channels to which to send the things after working on them
      - define notification channels over which messages with other components
        can be exchanged (publish/subscriber channels)

    All low level communication is handled by the base class -- deriving classes
    will register the respective channels, valid state transitions, and work
    methods.  When a 'thing' is received, the component is assumed to have full
    ownership over it, and that no other component will change the 'thing's
    state during that time.

    The main event loop of the component -- run() -- is executed as a separate
    process.  Components inheriting this class should be fully self sufficient,
    and should specifically attempt not to use shared resources.  That will
    ensure that multiple instances of the component can coexist for higher
    overall system throughput.  Should access to shared resources be necessary,
    it will require some locking mechanism across process boundaries.

    This approach should ensure that

      - 'thing's are always in a well defined state;
      - components are simple and focus on the semantics of 'thing' state
        progression;
      - no state races can occur on 'thing' state progression;
      - only valid state transitions can be enacted (given correct declaration
        of the component's semantics);
      - the overall system is performant and scalable.

    Inheriting classes may overload the methods:

        initialize
        initialize_child
        finalize
        finalize_child

    These method should be used to

      - set up the component state for operation
      - register input/output/notification channels
      - register work methods
      - register callbacks to be invoked on state notification
      - tear down the same on closing

    Inheriting classes MUST call the constructor:

        class StagingComponent(rpu.Component):
            def __init__(self, cfg, session):
                rpu.Component.__init__(self, cfg, session)

    Further, the class must implement the registered work methods, with
    a signature of:

        work(self, things)

    The method is expected to change the state of the 'thing's given.  'Thing's
    will not be pushed to outgoing channels automatically -- to do so, the work
    method has to call (see call documentation for other options):

        self.advance(thing)

    Until that method is called, the component is considered the sole owner of
    the 'thing's.  After that method is called, the 'thing's are considered
    disowned by the component.  If, however, components return from the work
    methods without calling advance on the given 'thing's, then the component
    keeps ownership of the 'thing's to advance it asynchronously at a later
    point in time.  That implies that a component can collect ownership over an
    arbitrary number of 'thing's over time, and they can be advanced at the
    component's descretion.
    """

    # FIXME:
    #  - make state transitions more formal

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):
        """
        This constructor MUST be called by inheriting classes, as it specifies
        the operation mode of the component: components can spawn a child
        process, or not.

        If a child will be spawned later, then the child process state can be
        initialized by overloading the`initialize_child()` method.
        Initialization for component the parent process is similarly done via
        `initializale_parent()`, which will be called no matter if the component
        spawns a child or not.

        Note that this policy should be strictly followed by all derived
        classes, as we will otherwise carry state over the process fork.  That
        can become nasty if the state included any form of locking (like, for
        profiling or locking).

        The symmetric teardown methods are called `finalize_child()` and
        `finalize_parent()`, for child and parent process, repsectively.

        Constructors of inheriting components *may* call start() in their
        constructor.
        """

        # NOTE: a fork will not duplicate any threads of the parent process --
        #       but it will duplicate any locks which are shared between the
        #       parent process and its threads -- and those locks might be in
        #       any state at this point.  As such, each child has to make
        #       sure to never, ever, use any of the inherited locks, but instead
        #       to create it's own set of locks in self.initialize_child
        #       / self.initialize_parent!

        self._cfg     = copy.deepcopy(cfg)
        self._session = session

        # we always need an UID
        if not hasattr(self, 'uid'):
            raise ValueError('class which inherits Component needs a uid')

        # state we carry over the fork
        self._started       = False
        self._debug         = cfg.get('debug', 'DEBUG')
        self._owner         = cfg.get('owner', session.uid)
        self._ctype         = "%s.%s" % (self.__class__.__module__,
                                         self.__class__.__name__)
        self._number        = cfg.get('number', 0)
        self._name          = cfg.get('name.%s' %  self._number,
                                      '%s.%s'   % (self._ctype, self._number))

        if self._owner == self.uid:
            self._owner = 'root'

        # don't create log and profiler, yet -- we do that all on start, and
        # inbetween nothing should happen anyway
        # NOTE: is this a sign that we should call start right here?

        # initialize the Process base class for later fork.
        mp.Process.__init__(self, name=self.uid)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return "%s <%s> [%s]" % (self.uid, self.__class__.__name__, self._owner)


    # --------------------------------------------------------------------------
    #
    def _heartbeat_monitor_cb(self, topic, msg):

        self._log.debug('command incoming: %s', msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'heartbeat':
            sender = arg['sender']
            if sender == self._cfg['heart']:
                self._log.debug('heartbeat monitored (%s)', sender)
                self._heartbeat = time.time()
            else:
                self._log.debug('heartbeat ignored (%s)', sender)
        else:
            self._log.debug('command ignored: %s', cmd)


    # --------------------------------------------------------------------------
    #
    def _cancel_monitor_cb(self, topic, msg):
        """
        We listen on the control channel for cancel requests, and append any
        found UIDs to our cancel list.
        """
        
        # FIXME: We do not check for types of things to cancel - the UIDs are
        #        supposed to be unique.  That abstraction however breaks as we
        #        currently have no abstract 'cancel' command, but instead use
        #        'cancel_units'.

        self._log.debug('command incoming: %s', msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_units':

            uids = arg['uids']

            if not isinstance(uids, list):
                uids = [uids]

            self._log.debug('register for cancellation: %s', uids)

            with self._cancel_lock:
                self._cancel_list += uids
        else:
            self._log.debug('command ignored: %s', cmd)


    # --------------------------------------------------------------------------
    #
    def _heartbeat_checker_cb(self):

        last = time.time() - self._heartbeat
        tout = self._heartbeat_timeout

        if last > tout:
          # print " ### %s heartbeat FAIL (self.owner)" % self.uid
            self._log.error('heartbeat check failed (%s / %s)', last, tout)
            raise RuntimeError('heartbeat check failed (%s / %s)' % (last, tout))

        else:
          # print " --- %s heartbeat OK" % self.uid
            self._log.debug('heartbeat check ok (%s / %s)', last, tout)

        return False # always sleep


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):

        return copy.deepcopy(self._cfg)


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def owner(self):
        return self._owner


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._name


    # --------------------------------------------------------------------------
    #
    @property
    def ctype(self):
        return self._ctype


    # --------------------------------------------------------------------------
    #
    @property
    def is_parent(self):
        return self._is_parent


    # --------------------------------------------------------------------------
    #
    @property
    def is_child(self):
        return not self.is_parent


    # --------------------------------------------------------------------------
    #
    @property
    def has_child(self):
        return self.is_parent and self.pid


    # --------------------------------------------------------------------------
    #
    def initialize_common(self):
        """
        This method may be overloaded by the components.  It is called once in
        the context of the parent process, *and* once in the context of the
        child process, after start().
        """
        self._log.debug('base initialize_common (NOOP)')

    # --------------------------------------------------------------------------
    #
    def _initialize_common(self):
        """
        This private method contains initialization for both parent a child
        process, which gets the component into a proper functional state.

        This method must be called *after* fork (this is asserted).
        """

        assert(not self._started)

        self._inputs        = dict()       # queues to get things from
        self._outputs       = dict()       # queues to send things to
        self._workers       = dict()       # methods to work on things
        self._publishers    = dict()       # channels to send notifications to
        self._subscribers   = dict()       # callbacks to receive notifications
        self._idlers        = dict()       # callbacks to call in intervals

        self._finalized     = False        # finalization guard

        self._cb_lock       = mt.RLock()   # guard threaded callback invokations

        # get debugging, logging, profiling set up
        self._dh   = ru.DebugHelper(name=self.uid)
        self._log  = self._session._get_logger(self.uid, self._debug)
        self._prof = self._session._get_profiler(self.uid)

        self._prof.prof('initialize', uid=self.uid)
        self._log.info('initialize %s',   self.uid)

        # all components need at least be able to talk to a control pubsub
        assert('bridges' in self._cfg)
        assert(rpc.LOG_PUBSUB     in self._cfg['bridges'])
        assert(rpc.STATE_PUBSUB   in self._cfg['bridges'])
        assert(rpc.CONTROL_PUBSUB in self._cfg['bridges'])
        assert(self._cfg['bridges'][rpc.LOG_PUBSUB    ]['addr_in'])
        assert(self._cfg['bridges'][rpc.STATE_PUBSUB  ]['addr_in'])
        assert(self._cfg['bridges'][rpc.CONTROL_PUBSUB]['addr_in'])

        # components can always publish logs, state updates and send control messages
        self.register_publisher(rpc.LOG_PUBSUB)
        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # make sure we watch all threads, flush all profiles
        self.register_timed_cb(self._thread_watcher_cb, timer= 0.1)
        self.register_timed_cb(self._profile_flush_cb,  timer=60.0)

        # give any derived class the opportunity to perform initialization in
        # parent *and* child context
        self.initialize_common()


    # --------------------------------------------------------------------------
    #
    def _initialize_parent(self):
        """
        parent initialization of component base class goes here
        """

        # we don't have a parent, we *are* the parent
        self._is_parent  = True
        self._uid        = self._parent_uid
        self._parent_uid = None

        # initialize state
        self._initialize_common()

        # give any derived class the opportunity to perform initialization in
        # the parent context
        self.initialize_parent()

        # only *after* initialization can we send the 'alive' signal --
        # otherwise we'd invite race conditions (only after init we have
        # subscribed to channels, and only then any publishing on those channels
        # should start, otherwise we'll loose messages.
        self._signal_alive()

        # parent calls terminate on stop(), which we translate here into stop()
        def sigterm_handler(signum, frame):
            self._log.warn('caught sigterm')
            self.stop()
        signal.signal(signal.SIGTERM, sigterm_handler)
        def sighup_handler(signum, frame):
            self._log.warn('caught sighup')
            self.stop()
        signal.signal(signal.SIGHUP, sighup_handler)

    # --------------------------------------------------------------------------
    #
    def initialize_parent(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon start(), and should be used to
        set up component state before things arrive.
        """
        self._log.debug('base initialize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _initialize_child(self):
        """
        child initialization of component base class goes here
        """

        # we don't have a child, we *are* the child
        self._is_parent = False
        self._child_uid = None

        # set process name
        spt.setproctitle('rp.%s' % self.uid)

        # initialize state
        self._initialize_common()

        self._log.info('cfg: %s' % pprint.pformat(self._cfg))

        # give any derived class the opportunity to perform initialization in
        # the child context
        self.initialize_child()

        # The heartbeat monotoring is performed in the child, which is
        # registering two callbacks:
        #   - an CONTROL_PUBSUB _heartbeat_monitor_cb which listens for
        #     heartbeats with 'src == self.owner', and records the time of
        #     heartbeat in self._heartbeat
        #   - an idle _heartbeat_checker_cb which checks the timer in frequent
        #     intervals, and which will call self.stop() if the last heartbeat
        #     is longer that self._heartbeat_timeout seconds ago
        #
        # Note that heartbeats are also used to keep sub-agents alive.
        # FIXME: this is not yet done
        assert(self._cfg.get('heart'))
        assert(self._cfg.get('heartbeat_interval'))

        # set up for eventual heartbeat send/recv
        self._heartbeat          = time.time()  # startup =~ heartbeat
        self._heartbeat_timeout  = self._cfg['heartbeat_timeout']
        self._heartbeat_interval = self._cfg['heartbeat_interval']
        self.register_timed_cb(self._heartbeat_checker_cb,
                               timer=self._heartbeat_interval)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._heartbeat_monitor_cb)

        # set controller callback to handle cancellation requests
        self._cancel_list = list()
        self._cancel_lock = mt.RLock()
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._cancel_monitor_cb)

        # parent calls terminate on stop(), which we translate here into stop()
        def sigterm_handler(signum, frame):
            self._log.warn('caught sigterm')
            self.stop()
        signal.signal(signal.SIGTERM, sigterm_handler)
        def sighup_handler(signum, frame):
            self._log.warn('caught sighup')
            self.stop()
        signal.signal(signal.SIGHUP, sighup_handler)

        # only *after* initialization can we send the 'alive' signal --
        # otherwise we'd invite race conditions (only after init we have
        # subscribed to channels, and only then any publishing on those channels
        # should start, otherwise we'll loose messages.
        self._signal_alive()


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon start(), and should be used to
        set up component state before things arrive.
        """
        self._log.debug('base initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _signal_alive(self):

        # parent *or* child will send an alive message.  child will send it if
        # it exists, parent otherwise.
        if self.is_parent and not self.has_child:
            self._log.debug('parent sends alive')
            self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'alive',
                                              'arg' : {'sender' : self.uid,
                                                       'owner'  : self.owner}})
        elif self.is_child:
            self._log.debug('child sends alive')
            self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'alive',
                                              'arg' : {'sender' : self.uid,
                                                       'owner'  : self.owner}})
        else:
            self._log.debug('no alive sent (%s : %s : %s)', self.is_child,
                    self.has_child, self.is_parent)


    # --------------------------------------------------------------------------
    #
    def _finalize_common(self):

        self._log.debug('base _finalize_common')

        if self._finalized:
            # some other thread is already taking care of finalization
            return

        self._finalized = True

        self.finalize_common()

        self_thread = mt.current_thread()

        # signal all threads to terminate
        for s in self._subscribers:
            self._log.debug('%s -> term %s' % (self.uid, s))
            self._subscribers[s]['term'].set()
        for i in self._idlers:
            self._log.debug('%s -> term %s' % (self.uid, i))
            self._idlers[i]['term'].set()

        # unlock the cb lock, for the case that we have a (locked) callback
        # somewhere upward in the callstack.  Since all threads have the term
        # event now set, they should fall out of their event loops at this
        # point.  We do not always have the lock though...
        try:
            self._cb_lock.release()
        except:
            pass

        # collect the threads
        for s in self._subscribers:
            t = self._subscribers[s]['thread']
            if t != self_thread:
                self._log.debug('%s -> join %s' % (self.uid, s))
                t.join()
                self._log.debug('%s >> join %s' % (self.uid, s))
        for i in self._idlers:
            t = self._idlers[i]['thread']
            if t != self_thread:
                self._log.debug('%s -> join %s' % (self.uid, i))
                t.join()
                self._log.debug('%s >> join %s' % (self.uid, i))

        # NOTE: this relies on us not to change the name of MainThread
        if self_thread.name == 'MainThread':
            self._log.debug('%s close prof' % self.uid)
            try:
                self._prof.prof("stopped", uid=self._uid)
                self._prof.close()
            except Exception:
                pass

        self._log.debug('base _finalize_common done')


    # --------------------------------------------------------------------------
    #
    def finalize_common(self):
        """
        This method may be overloaded by the components.  It is called once in
        the context of the parent *and* the child process, upon stop().  It
        should be used to tear down component state after things have been
        processed.
        """
        self._log.debug('base finalize_common (NOOP)')

        # FIXME: finaliers should unrergister all callbacks/idlers/subscribers


    # --------------------------------------------------------------------------
    #
    def _finalize_parent(self):

        self._log.debug('_finalize_parent')

        self.finalize_parent()
        self._finalize_common()

        self._log.debug('_finalize_parent done')


    # --------------------------------------------------------------------------
    #
    def finalize_parent(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon stop(), and should be used to
        tear down component state after things have been processed.
        """
        self._log.debug('base finalize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _finalize_child(self):

        self._log.debug('_finalize_child')

        self.finalize_child()
        self._finalize_common()

        self._log.debug('_finalize_child done')


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon stop(), and should be used to
        tear down component state after things have been processed.
        """
        self._log.debug('base finalize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def start(self, spawn=True):
        """
        This method will start the child process.  *After* doing so, it will
        call the parent's initialize, so that this is only executed in the
        parent's process context (before fork).  Start will execute the run loop
        in the child process context, and in that context call
        initialize_child() before entering the loop.

        start() essentially performs:

            pid = fork()

            if pid:
                self._initialize_parent()
            else:
                self._initialize_child()
                run()

        This is not really correct though, as we don't have control over the
        fork itself (that happens in the base class).  So we do:

        def run():
            self._initialize_child()
            ...

        which takes care of the child initialization, and here we take care of
        the parent initialization after we called the base class' `start()`
        method, ie. after fork.
        """

        # fork child process -- and provide the right uid

        if spawn:
            self._parent_uid = self._uid
            self._child_uid  = self._uid + '.child'
            self._uid        = self._child_uid
            mp.Process.start(self)
        else:
            self._parent_uid = self._uid
            self._child_uid  = None


      # try:
            # this is now the parent process context
        self._initialize_parent()
      # except Exception as e:
      #     self._log.exception ('initialization failed')
      #     ru.cancel_main_thread()
      #     raise


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):
        # we only really join when the comoinent child pricess has been started
        if self.pid:
            self._log.debug('%s join   (%s)' % (self.uid, self.pid))
            mp.Process.join(self, timeout)
            self._log.debug('%s joined (%s)' % (self.uid, self.pid))


    # --------------------------------------------------------------------------
    #
    def stop(self):
        """
        Shut down the process hosting the event loop.  If the parent calls
        stop(), the child is simply terminated (no child finalizers are
        called).  If the child calls stop() itself, child finalizers are called
        before calling exit.

        stop() can be called multiple times, and can be called from the
        MainThread, or from sub thread (such as callback invocations) -- but it
        should notes that, if called from a callback, it may not always be able
        to tear down all threads, specifically not the callback thread itself
        and the MainThread.  Safest is calling it once from each the parent's
        and child's MainThread.  Since the finalizers are only called on the
        first invocation to stop(), finalizers can happen in callback threads!

        stop() basically performs:

            tear down all subscriber threads
            if parent:
                finalize_parent()
                self.terminate()
            else:
                finalize_child()
                sys.exit()
        """

        self._log.debug('stop %s (%s : %s : %s) [%s]' % (self.uid, os.getpid(),
                        self.pid, mt.current_thread().name,
                        ru.get_caller_name()))

        # parent and child finalization will have all comoonents and bridges
        # available
        if self._is_parent:

            self._log.debug('stop as parent')

            # Signal the child -- if one exists
            if self.has_child:
                self._log.info("stop    %s" % self.pid)
                self.terminate()
                self._log.info("stopped %s" % self.pid)
                self._log.info("join    %s" % self.pid)
                self.join()
                self._log.info("joined  %s" % self.pid)

            self._finalize_parent()

            # FIXME: self._finalize is set now.  We should add finalization
            #        guards around all methods.

        else:
            # The child exits here.
            self._log.debug('stop as child (exit)')
            sys.exit()


    # --------------------------------------------------------------------------
    #
    def poll(self):
        """
        This is a wrapper around is_alive() which mimics the behavior of the same
        call in the subprocess.Popen class with the same name.  It does not
        return an exitcode though, but 'None' if the process is still
        alive, and always '0' otherwise
        """
        if self.is_alive():
            return None
        else:
            return 0


    # --------------------------------------------------------------------------
    #
    def register_input(self, states, input, worker):
        """
        Using this method, the component can be connected to a queue on which
        things are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing arrival.

        This method will further associate a thing state with a specific worker.
        Upon thing arrival, the thing state will be used to lookup the respective
        worker, and the thing will be handed over.  Workers should call
        self.advance(thing), in order to push the thing toward the next component.
        If, for some reason, that is not possible before the worker returns, the
        component will retain ownership of the thing, and should call advance()
        asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next thing once the worker method returns.
        """

        if not isinstance(states, list):
            states = [states]


        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        # get address for the queue
        addr = self._cfg['bridges'][input]['addr_out']
        self._log.debug("using addr %s for input %s" % (addr, input))

        q = rpu_Queue.create(self._session, rpu_QUEUE_ZMQ, input, rpu_QUEUE_OUTPUT, addr=addr)
        self._inputs['name'] = {'queue'  : q,
                                'states' : states}

        self._log.debug('registered input %s', name)

        # we want exactly one worker associated with a state -- but a worker can
        # be responsible for multiple states
        for state in states:

            if state in self._workers:
                self._log.warn("%s replaces worker for %s (%s)" \
                        % (self.uid, state, self._workers[state]))
            self._workers[state] = worker

            self._log.debug('registered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def unregister_input(self, states, input, worker):
        """
        This methods is the inverse to the 'register_input()' method.
        """

        if not isinstance(states, list):
            states = [states]

        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name not in self._inputs:
            raise ValueError('input %s not registered' % name)
        del(self._inputs[name])
        self._log.debug('unregistered input %s', name)

        for state in states:
            if state not in self._workers:
                raise ValueError('worker %s not registered for %s' % worker.__name__, state)
            del(self._workers[state])
            self._log.debug('unregistered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def register_output(self, states, output=None):
        """
        Using this method, the component can be connected to a queue to which
        things are sent after being worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing departure.

        If a state but no output is specified, we assume that the state is
        final, and the thing is then considered 'dropped' on calling advance() on
        it.  The advance() will trigger a state notification though, and then
        mark the drop in the log.  No other component should ever again work on
        such a final thing.  It is the responsibility of the component to make
        sure that the thing is in fact in a final state.
        """

        if not isinstance(states, list):
            states = [states]

        for state in states:

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn("%s replaces output for %s : %s -> %s" \
                        % (self.uid, state, self._outputs[state], output))

            if not output:
                # this indicates a final state
                self._outputs[state] = None
            else:
                # get address for the queue
                addr = self._cfg['bridges'][output]['addr_in']
                self._log.debug("using addr %s for output %s" % (addr, output))

                # non-final state, ie. we want a queue to push to
                q = rpu_Queue.create(self._session, rpu_QUEUE_ZMQ, output, rpu_QUEUE_INPUT, addr=addr)
                self._outputs[state] = q

                self._log.debug('registered output    : %s : %s : %s' \
                     % (state, output, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_output(self, states):
        """
        this removes any outputs registerd for the given states.
        """

        if not isinstance(states, list):
            states = [states]

        for state in states:
            if not state in self._outputs:
                raise ValueError('state %s hasno output to unregister' % state)
            del(self._outputs[state])
            self._log.debug('unregistered output for %s', state)


    # --------------------------------------------------------------------------
    #
    def register_timed_cb(self, cb, cb_data=None, timer=None):
        """
        Idle callbacks are invoked at regular intervals -- they are guaranteed
        to *not* be called more frequently than 'timer' seconds, no promise is
        made on a minimal call frequency.  The intent for these callbacks is to
        run lightweight work in semi-regular intervals.  
        """

        name = "%s.idler.%s" % (self.uid, cb.__name__)

        with self._cb_lock:
            if name in self._idlers:
                raise ValueError('cb %s already registered' % cb.__name__)

        if None != timer:
            timer = float(timer)

        # create a separate thread per idle cb
        # NOTE: idle timing is a tricky beast: if we sleep for too long, then we
        #       have to wait that long on stop() for the thread to get active
        #       again and terminate/join.  So we always sleep for 1sec, and
        #       manually check if timer has passed before activating the
        #       callback.
        # ----------------------------------------------------------------------
        def _idler(terminate, callback, callback_data, to):
          # print 'thread %10s : %s' % (ru.gettid(), mt.current_thread().name)
            try:
                last = 0.0  # never been called
                while not terminate.is_set():
                    now = time.time()
                    if to == None or (now-last) > to:
                        with self._cb_lock:
                            if callback_data != None:
                                callback(cb_data=callback_data)
                            else:
                                callback()
                        last = now
                    if to:
                        time.sleep(0.1)
            except Exception as e:
                self._log.exception("idler failed %s", mt.current_thread().name)
        # ----------------------------------------------------------------------

        # create a idler thread
        e = mt.Event()
        t = mt.Thread(target=_idler, args=[e,cb,cb_data,timer], name=name)
        t.start()

        with self._cb_lock:
            self._idlers[name] = {'term'   : e,  # termination signal
                                  'thread' : t}  # thread handle

        self._log.debug('%s registered idler %s' % (self.uid, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_timed_cb(self, pubsub, cb):
        """
        This method is reverts the register_timed_cb() above: it
        removes an idler from the component, and will terminate the
        respective thread.
        """

        name = "%s.idler.%s" % (self.uid, cb.__name__)

        with self._cb_lock:
            if name not in self._idlers:
                raise ValueError('%s is not registered' % name)

            entry = self._idlers[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._idlers[name])

        self._log.debug("unregistered idler %s", name)


    # --------------------------------------------------------------------------
    #
    def register_publisher(self, pubsub):
        """
        Using this method, the component can registered itself to be a publisher
        of notifications on the given pubsub channel.
        """

        if pubsub in self._publishers:
            raise ValueError('publisher for %s already regisgered' % pubsub)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            self._log.error('no addr: %s' % pprint.pformat(self._cfg['bridges']))
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_in']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        q = rpu_Pubsub.create(self._session, rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_PUB, addr=addr)
        self._publishers[pubsub] = q

        self._log.debug('registered publisher : %s : %s' % (pubsub, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_publisher(self, pubsub):
        """
        This removes the registration of a pubsub channel for publishing.
        """

        if pubsub not in self._publishers:
            raise ValueError('publisher for %s is not registered' % pubsub)

        del(self._publishers[pubsub])
        self._log.debug('unregistered publisher %s', pubsub)


    # --------------------------------------------------------------------------
    #
    def register_subscriber(self, pubsub, cb, cb_data=None):
        """
        This method is complementary to the register_publisher() above: it
        registers a subscription to a pubsub channel.  If a notification
        is received on thag channel, the registered callback will be
        invoked.  The callback MUST have one of the signatures:

          callback(topic, msg)
          callback(topic, msg, cb_data)

        where 'topic' is set to the name of the pubsub channel.

        The subscription will be handled in a separate thread, which implies
        that the callback invocation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invocation.
        """

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_out']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        # subscription is racey for the *first* subscriber: the bridge gets the
        # subscription request, and forwards it to the publishers -- and only
        # then can we expect the publisher to send any messages *at all* on that
        # channel.  Since we do not know if we are the first subscriber, we'll
        # have to wait a little to let the above happen, before we go further
        # and create any publishers.
        # FIXME: this barrier should only apply if we in fact expect a publisher
        #        to be created right after -- but it fits here better logically.
      # time.sleep(0.1)

        # ----------------------------------------------------------------------
        def _subscriber(q, terminate, callback, callback_data):
            try:
                while not terminate.is_set():
                    topic, msg = q.get_nowait(1000) # timout in ms
                    if topic and msg:
                        if not isinstance(msg,list):
                            msg = [msg]
                        for m in msg:
                            self._log.debug("<= %s: %s" % (callback.__name__, [topic, m]))
                            with self._cb_lock:
                                if callback_data != None:
                                    callback(topic=topic, msg=m, cb_data=callback_data)
                                else:
                                    callback(topic=topic, msg=m)
            except Exception as e:
                self._log.exception("subscriber failed %s" % mt.current_thread().name)
        # ----------------------------------------------------------------------

        with self._cb_lock:
            if name in self._subscribers:
                raise ValueError('cb %s already registered for %s' % (cb.__name__, pubsub))

            # create a pubsub subscriber (the pubsub name doubles as topic)
            q = rpu_Pubsub.create(self._session, rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_SUB, addr=addr)
            q.subscribe(pubsub)

            e = mt.Event()
            t = mt.Thread(target=_subscriber, args=[q,e,cb,cb_data], name=name)
            t.start()

            self._subscribers[name] = {'term'   : e,  # termination signal
                                       'thread' : t}  # thread handle

        self._log.debug('%s registered %s subscriber %s' % (self.uid, pubsub, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_subscriber(self, pubsub, cb):
        """
        This method is reverts the register_subscriber() above: it
        removes a subscription from a pubsub channel, and will terminate the
        respective thread.
        """

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)

        with self._cb_lock:
            if name not in self._subscribers:
                raise ValueError('%s is not subscribed to %s' % (cb.__name__, pubsub))

            entry = self._subscribers[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._subscribers[name])

        self._log.debug("unregistered %s", name)


    # --------------------------------------------------------------------------
    #
    def _thread_watcher_cb(self):

        with self._cb_lock:
            for s in self._subscribers:
                t = self._subscribers[s]['thread']
                if not t.is_alive():
                    self._log.error('subscriber %s died', t.name)
                    raise RuntimeError('subscriber %s died' % t.name)

            for i in self._idlers:
                t = self._idlers[i]['thread']
                if not t.is_alive():
                    self._log.error('idler %s died', t.name)
                    raise RuntimeError('idler %s died' % t.name)


    # --------------------------------------------------------------------------
    #
    def _profile_flush_cb(self):

        self._log.handlers[0].flush()
        self._prof.flush()


    # --------------------------------------------------------------------------
    #
    def run(self):
        """
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new things from all input queues
        (round-robin).  For each thing received, it will route that thing to the
        respective worker method.  Once the thing is worked upon, the next
        attempt on getting a thing is up.
        """

        try:
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            # this is now the child process context
            self._initialize_child()

            # The main event loop will repeatedly iterate over all input
            # channels.  It can only be terminated by
            #   - exceptions
            #   - sys.exit()
            #   - kill from the parent (which becomes a sys.exit(), too)
            while True:

                # if no action occurs in this iteration, idle
                if not self._inputs:
                    time.sleep(0.1)

                for name in self._inputs:
                    input  = self._inputs[name]['queue']
                    states = self._inputs[name]['states']

                    # FIXME: a simple, 1-thing caching mechanism would likely
                    #        remove the req/res overhead completely (for any
                    #        non-trivial worker).
                    things = input.get_nowait(1000) # timeout in microseconds

                    if not things:
                        continue

                    if not isinstance(things, list):
                        things = [things]

                    self._log.debug(' === input bulk %s things on %s' % (len(things), name))

                    # the worker target depends on the state of things, so we 
                    # need to sort the things into buckets by state before 
                    # pushing them
                    buckets = dict()
                    for thing in things:
                        state = thing['state']
                        if not state in buckets:
                            buckets[state] = list()
                        buckets[state].append(thing)

                    # We now can push bulks of things to the workers

                    for state,things in buckets.iteritems():

                        assert(state in states)
                        assert(state in self._workers)

                        try:
                            to_cancel = list()
                            for thing in things:
                                uid   = thing['uid']
                                ttype = thing['type']
                                state = thing['state']

                                # FIXME: this can become expensive over time
                                #        if the cancel list is never cleaned
                                if uid in self._cancel_list:
                                    with self._cancel_lock:
                                        self._cancel_list.remove(uid)
                                    to_cancel.append(thing)

                                self._log.debug('got %s (%s)', ttype, thing)
                                self._prof.prof(event='get', state=state, uid=uid, msg=input.name)
                                self._prof.prof(event='work start', state=state, uid=uid)

                            if to_cancel:
                                self.advance(to_cancel, rps.CANCELED, publish=True, push=False)

                            with self._cb_lock:
                                self._log.debug(' === work on bulk [%s]', len(things))
                                self._workers[state](things)

                            for thing in things:
                                self._prof.prof(event='work done ', state=state, uid=uid)

                        except Exception as e:
                            # this is not fatal -- only the 'things' fail, not
                            # the component

                            self._log.exception("worker %s failed", self._workers[state])
                            self.advance(things, rps.FAILED, publish=True, push=False)

                            for thing in things:
                                self._prof.prof(event='failed', msg=str(e), 
                                                uid=thing['uid'], state=state)

        except Exception as e:
            # We should see that exception only on process termination -- and of
            # course on incorrect implementations of component workers.  We
            # could in principle detect the latter within the loop -- - but
            # since we don't know what to do with the things it operated on, we
            # don't bother...
            self._log.exception('loop exception')
        
        except SystemExit:
            self._log.exception("loop exit")
        
        except KeyboardInterrupt:
            self._log.exception("loop cancel")
        
        except:
            # Can be any other signal or interrupt.
            self._log.exception('loop interruption')
        
        finally:
            self._log.info('loop stops')
            self._finalize_child()
            self.stop()


    # --------------------------------------------------------------------------
    #
    def advance(self, things, state=None, publish=True, push=False,
                timestamp=None):
        """
        Things which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state model.  This method will update the thing state, and push it into
        the output queue registered as target for that state.

        things:  list of things to advance
        state:   new state to set for the things
        publish: determine if state update notifications should be issued
        push:    determine if things should be pushed to outputs
        prof:    determine if state advance creates a profile event
                 (publish, and push are always profiled)

        'Things' are expected to be a dictionary, and to have 'state', 'uid' and
        optionally 'type' set.

        If 'thing' contains an '$all' key, the complete dict is published;
        otherwise, *only the state* is published.

        This is evaluated in self.publish.
        """

        if not timestamp:
            timestamp = rpu_timestamp()

        if not isinstance(things, list):
            things = [things]

        self._log.debug(' === advance bulk size: %s', len(things))

        # assign state, sort things by state
        buckets = dict()
        for thing in things:

            uid   = thing['uid']
            ttype = thing['type']

            if ttype not in ['unit', 'pilot']:
                raise TypeError("thing has unknown type (%s)" % uid)

            if state:
                # state advance done here
                thing['state'] = state
            else:
                # state advance happened before
                state = thing['state']

            self._prof.prof('advance', uid=uid, state=state, timestamp=timestamp)

            if not state in buckets:
                buckets[state] = list()
            buckets[state].append(thing)

        # should we publish state information on the state pubsub?
        if publish:

            to_publish = list()

            # Things in final state are published in full
            for state,things in buckets.iteritems():
                if state in rps.FINAL:
                    for thing in things:
                        thing['$all'] = True

            # is '$all' is set, we update the complete thing_dict.  In all
            # other cases, we only send 'uid', 'type' and 'state'.
            for thing in things:
                if '$all' in thing:
                    del(thing['$all'])
                    to_publish.append(thing)
                else:
                    to_publish.append({'uid'   : thing['uid'],
                                       'type'  : thing['type'],
                                       'state' : state})

            self.publish(rpc.STATE_PUBSUB, {'cmd': 'update', 'arg': to_publish})
            ts = rpu_timestamp()
            for thing in things:
                self._prof.prof('publish', uid=thing['uid'], state=thing['state'], timestamp=ts)


        # should we push things downstream, to the next component
        if push:

            # the push target depends on the state of things, so we need to sort
            # the things into buckets by state before pushing them
            # now we can push the buckets as bulks
            for state,things in buckets.iteritems():

                if state in rps.FINAL:
                    # things in final state are dropped
                    for thing in things:
                        self._log.debug('%s %s ===| %s' % ('push', thing['uid'], thing['state']))
                    continue

                if not self._outputs[state]:
                    # empty output -- drop thing
                    for thing in things:
                        self._log.debug('%s %s ~~~| %s' % ('push', thing['uid'], thing['state']))
                    continue

                if state not in self._outputs:
                    # unknown target state -- error
                    self._log.error("%s can't route state %s (%s)" \
                            % (self.uid, state, self._outputs.keys()))
                    continue

                output = self._outputs[state]

                # push the thing down the drain
                # FIXME: we should assert that the things are in a PENDING state.
                #        Better yet, enact the *_PENDING transition right here...
                self._log.debug(' === put bulk %s: %s', state, len(things))
                output.put(things)

                ts = rpu_timestamp()
                for thing in things:
                    
                    # never carry $all across component boundaries!
                    if '$all' in thing:
                        del(thing['$all'])

                    self._log.debug('%s %s ---> %s' % ('push', thing['uid'], state))
                    self._prof.prof('put', uid=thing['uid'], state=state,
                            msg=output.name, timestamp=ts)


    # --------------------------------------------------------------------------
    #
    def publish(self, pubsub, msg):
        """
        push information into a publication channel
        """

        if not isinstance(msg, list):
            msg = [msg]

        if pubsub not in self._publishers:
            self._log.error("can't route '%s' notification: %s" % (pubsub, msg))
            return

        if not self._publishers[pubsub]:
            self._log.error("no route for '%s' notification: %s" % (pubsub, msg))
            return

        self._publishers[pubsub].put(pubsub, msg)



# ==============================================================================
#
class Worker(Component):
    """
    A Worker is a Component which cannot change the state of the thing it
    handles.  Workers are emplyed as helper classes to mediate between
    components, between components and database, and between components and
    notification channels.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session=None):

        Component.__init__(self, cfg=cfg, session=session)


    # --------------------------------------------------------------------------
    #
    # we overload state changing methods from component and assert neutrality
    # FIXME: we should insert hooks around callback invocations, too
    #
    def advance(self, things, state=None, publish=True, push=False, prof=True):

        if state:
            raise RuntimeError("worker %s cannot advance state (%s)"
                    % (self.uid, state))

        Component.advance(self, things, state, publish, push, prof)


# ------------------------------------------------------------------------------

