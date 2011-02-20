# Copyright 2011 Andrei Savu <asavu@apache.org>
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os, sys
import zookeeper
import threading
import functools
import time

zookeeper.set_log_stream(sys.stdout)

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

class ZooKeeper(object):
    """ Basic adapter; always retry on ConnectionLossException """

    def __init__(self, quorum):
        self._quorum = quorum

        self._handle = None
        self._connected = False

        self._cv = threading.Condition()
        self._connect()

    def _connect(self):
        """ Open a connection to the quorum """
        def watcher(handle, type, state, path):
            self._cv.acquire()
            self._connected = True

            self._cv.notify()
            self._cv.release()

        self._cv.acquire()
        self._handle = zookeeper.init(self._quorum, watcher, 10000)

        self._cv.wait(10.0)
        if not self._connected:
            print >>sys.stderr, 'Unable to connecto the ZooKeeper cluster.'
            sys.exit(-1)
        self._cv.release()

    def _retry_on_connection_loss(self, fn):
        """ Retry calling the function on ConnectionLossException """
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            # XXX add a time limit. Important for producers!
            while True:
                try:
                    return fn(*args, **kwargs)
                except zookeeper.ConnectionLossException:
                    time.sleep(0.5)
        return wrapper

    def __getattr__(self, name):
        """ Pass-Through and retry on ConnectionLossException """
        value = getattr(zookeeper, name)
        if callable(value):
            return functools.partial(
                self._retry_on_connection_loss(value), 
                self._handle
            )
        else:
            return name

    def ensure_exists(self, name, data = ''):
        try:
            self.create(name, data, [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            pass # it's fine if the node already exists

class Producer(object):

    def __init__(self, zk):
        self._zk = zk

        self._zk.ensure_exists('/queue')
        self._zk.ensure_exists('/queue/items')

    def put(self, data):
        name = self._zk.create("/queue/items/item-", '', 
            [ZOO_OPEN_ACL_UNSAFE], 
            zookeeper.SEQUENCE
        )
        return self._zk.set2(name, data)

zk = ZooKeeper("localhost:2181,localhost:2182/zookeeper")

p = Producer(zk)
p.put('test')

