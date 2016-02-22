
# Protocols used in RP

RP uses Queues to communicate Pilots and Units between state enacting
components.  RP also uses PubSub channels to communicate other types of
information, such as state updates, heartbeat information, shutdown commands,
etc.

This document describes what messages are sent in what format over what channel.


## Queues

The context in which queues are used is always well defined: for each    queue
type, only one specific source and sink exist in the code, with very few
exceptions.  There is thus no need to send meta data along with the messages,
and we only send 'things', ie. pilots or units, which are represented by plain
python dicts.  All 'things' are guaranteed to have the following fields:

    'uid':    string, unique ID
    'type':   string, entity type (session | umgr | unit | pmgr | pilot)
    'state':  string, state of the thing if stateful, 'None' otherwise


## PubSub channels

The communication over pubsub channels has a wider, more flexible, and more dynamic scope than the communication over queues.  We thus always add a certain amount of meta data, to keep inspection of arriving messages uniform.

All messages are structured into:

    'cmd':    which allows the selection of the expected activity type
    'arg':    which provides additional information for that activity

We will below list the set of valid 'cmd' fields for each pubsub channel, and
also define the structure of the 'arg' fields, where applicable.


### `STATE`

    'cmd' : 'state_update'
    'arg' : {'ttype' : 'pilot|unit',   # what kind of state is updated?
             'thing' : <dict>}         # the thing to update

where the 'thing' dict always has a 'uid' and 'state' field, ie. is the same
thing as pushed through queues.

Note: the communication on the STATE pubsub is not efficient: we should really
only send 'ttype', 'uid' and 'state'.  At the moment though the channel doubles
as pipe to the DB, and we usually want to push several other fields  to the DB,
thus the overload.  This may get separated in the future.


### `COMMAND`

    'cmd' : 'alive'
    'arg' : {'from'  : <string>}      # who is alive?
    
    
    'cmd' : 'shutdown'
    'arg' : {'from'  : <string>,      # who says so?
             'msg'   : <string>}      # why?
    
    
    'cmd' : 'cancel'
    'arg' : {'from'  : <string>,      # who says so?
             'ttype' : 'unit|pilot',  # what is to be canceled? 
             'uid'   : <string>}      # what thing exactly?
    
    
    'cmd' : 'add_pilot'
    'arg' : {'from'  : <string>,      # on what umgr?
             'uid'   : <string>}      # what pilot is added?
    
    
    'cmd' : 'remove_pilot'
    'arg' : {'from'  : <string>,      # from what umgr?
             'uid'   : <string>}      # what pilot is removed?


### `*_UNSCHEDULE, *_RESCHEDULE`

    'cmd'  : 'unschedule|reschedule'
    'arg'  : {'from'  : <string>,     # who says so?
              'ttype' : <string>,     # what is to be rescheduled?
              'thing' : <dict>}       # what thing exactly?


