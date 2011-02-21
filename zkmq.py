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
        """ Open a connection to the quorum and for it to be established """
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
        """ Pass-Through with connection handle and retry on ConnectionLossException """
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
        name = self._zk.create("/queue/items/item-", "", 
            [ZOO_OPEN_ACL_UNSAFE], 
            zookeeper.SEQUENCE
        )
        return self._zk.set(name, data, 0)

class Consumer(object):

    def __init__(self, zk):
        self._zk = zk
        self._id = None

        map(self._zk.ensure_exists, ('/queue', '/queue/items', 
            '/queue/consumers', '/queue/partial'))
        self._register()

    def _register(self):
        self._id = self._zk.create("/queue/consumers/consumer-", '', 
            [ZOO_OPEN_ACL_UNSAFE], zookeeper.SEQUENCE)

        self._zk.create(self._fullpath('/active'), '',
            [ZOO_OPEN_ACL_UNSAFE], zookeeper.EPHEMERAL)
        self._zk.create(self._fullpath('/item'), '',
            [ZOO_OPEN_ACL_UNSAFE], 0)

    def _fullpath(self, sufix):
        return self._id + sufix

    def _move(self, src, dest):
        try:
            (data, stat) = self._zk.get(src, None)
            self._zk.set(dest, data)
            self._zk.delete(src, stat['version'])
            return data

        except zookeeper.NoNodeException:
            # a consumer already moved this znode
            self._zk.set(dest, '')
            return None

        except zookeeper.BadVersionException:
            # someone is modifying the queue in place. You can re-read or abort.
            raise

    def reserve(self):
        while True:
            children = sorted(self._zk.get_children('/queue/items', None))
            if len(children) == 0:
                return None
            for child in children:
                data = self._move('/queue/items/' + child, self._fullpath('/item'))
                if data: return data

    def done(self):
        self._zk.set(self._fullpath('/item'), '')

    def __repr__(self):
        return '<Consumer id=%r>' % self._id

if __name__ == '__main__':
    zk = ZooKeeper("localhost:2181,localhost:2182")

    p = Producer(zk)
    p.put('test value')

    c = Consumer(zk)
    print c
    print c.reserve()

    zk.close()

