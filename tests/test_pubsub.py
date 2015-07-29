#!/usr/bin/env python 

import os
import zmq
import time
import copy
import json
import random
import Queue
import threading           as mt
import multiprocessing     as mp
import radical.utils       as ru
import radical.pilot.utils as rpu


b  = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_BRIDGE, 'tcp://127.0.0.1:30000')

s1 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_SUB,    'tcp://127.0.0.1:30000')
s1.subscribe('state')

s2 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_SUB,    'tcp://127.0.0.1:30000')
s2.subscribe('state')

p1 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_PUB,    'tcp://127.0.0.1:30000')
p2 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_PUB,    'tcp://127.0.0.1:30000')

time.sleep (1)

N = 500
print "n   : %d" % N

start = time.time()
for i in range(N):
    p1.put('state', {'id' : "p1_%05d" % i})
    p2.put('state', {'id' : "p2_%05d" % i})
stop = time.time()
print "sent: %4.2f (%7.1f)" % (stop-start, 2*N/(stop-start))

start = time.time()
for i in range(2*N):
    msg_1 = s1.get()
    msg_2 = s2.get()
  # print "<= s1 %s" % msg_1
  # print "<= s2 %s" % msg_2
stop = time.time()
print "recv: %4.2f (%7.1f)" % (stop-start, 2*N/(stop-start))

b.close()

