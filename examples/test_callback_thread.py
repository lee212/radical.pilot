#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

import radical.pilot as rp
import radical.utils as ru

import threading, Queue

exec_q = Queue.Queue()
fail_q = Queue.Queue()



# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------

def unit_state_cb (unit, state) :

    if state == rp.DONE:
        print 'Uid: {0}, State: {1}'.format(unit.uid, state)

        exec_q.put(unit)


    elif state == rp.FAILED:
        print 'Uid: {0}, State: {1}'.format(unit.uid, state)

        fail_q.put(unit)
        


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.LogReporter(name='radical.pilot', level=verbose)
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(database_url='mongodb://rp:rp@ds051960.mlab.com:51960/tests')

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                'resource'      : resource,
                'runtime'       : 15,  # pilot runtime (min)
                'exit_on_error' : True,
                'project'       : config[resource]['project'],
                'queue'         : config[resource]['queue'],
                'access_schema' : config[resource]['schema'],
                'cores'         : config[resource]['cores'],
                }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        umgr.register_callback(unit_state_cb)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.

        n = 4   # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/sleep'
            cud.arguments = ['']
            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        umgr.submit_units(cuds)


        def exec_func():

            while True:

                unit = exec_q.get()
                new_unit = unit.description
                print 'Got unit ', unit.uid
                new_unit_info = umgr.submit_units(new_unit)
                print 'Submit unit ', new_unit_info.uid
                exec_q.task_done()
        

        def fail_func():
            
            while True:

                unit = fail_q.get()
                unit.arguments = ['5']
                exec_q.put(unit)
                fail_q.task_done()


        exec_thread = threading.Thread(target=exec_func,args=())
        exec_thread.daemon = True
        fail_thread = threading.Thread(target=fail_func, args=())
        fail_thread.daemon = True

        exec_thread.start()
        fail_thread.start()


        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')

        while len(umgr.list_units()) != 20:
            all_cu_uids = umgr.list_units()
            all_cus = umgr.get_units(all_cu_uids)

            pending_cus = []

            for unit in all_cus:
                if (unit.state!=rp.DONE)and(unit.state!=rp.CANCELED)and(unit.state!=rp.FAILED):
                    pending_cus.append(unit.uid)

            umgr.wait_units(pending_cus, timeout=60)
    

    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close(cleanup=False)

    report.header()


#-------------------------------------------------------------------------------

