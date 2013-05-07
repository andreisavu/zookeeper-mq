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

ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}

def retry_on(*excepts):
    """ Retry function execution if some known exception types are raised """
    def _decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception, e:
                    if not any([isinstance(e, _) for _ in excepts]):
                        raise
                    # else: retry forever
                    time.sleep(5)
        return wrapper
    return _decorator

class ZooKeeper(object):
    """ Basic adapter; always retry on ConnectionLossException """

    def __init__(self, quorum, timeout=10000):
        """Connect to ZooKeeper quorum

        timeout -- zookeeper session timeout in milliseconds
        """
        self._quorum = quorum
        self._timeout = timeout

        self._handle = None
        self._connected = False

        self._cv = threading.Condition()
        self._connect()

    def _connect(self):
        """ Open a connection to the quorum and for it to be established """
        def watcher(handle, type, state, path):
            self._cv.acquire()
            try:
                self._connected = True
                self._cv.notify()
            finally:
                self._cv.release()

        self._cv.acquire()
        try:
            self._handle = zookeeper.init(self._quorum, watcher, self._timeout)

            self._cv.wait(self._timeout/1000)
            if not self._connected:
                print >>sys.stderr, 'Unable to connecto the ZooKeeper cluster.'
                sys.exit(-1)
        finally:
            self._cv.release()

    def __getattr__(self, name):
        """ Pass-Through with connection handle and retry on ConnectionLossException """
        value = getattr(zookeeper, name)
        if callable(value):
            return functools.partial(
                retry_on(zookeeper.ConnectionLossException, \
                    zookeeper.OperationTimeoutException)(value), 
                self._handle
            )
        else:
            return name

    def ensure_exists(self, name, data = ''):
        try:
            self.create(name, data, [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            pass # it's fine if the node already exists

    @retry_on(zookeeper.NoNodeException)
    def create_sequence(self, name, data):
        """ A safe way of creating an ephemeral node. Worst case scenario 
        you will end-up creating multiple empty znodes """
        name = self.create(name, '', 
            [ZOO_OPEN_ACL_UNSAFE], 
            zookeeper.SEQUENCE
        )
        try:
            self.set(name, data, 0)
        except zookeeper.BadVersionException:
            pass # a previous write succeeded

class Producer(object):

    def __init__(self, zk):
        self._zk = zk

        self._zk.ensure_exists('/queue')
        self._zk.ensure_exists('/queue/items')

    def put(self, data, priority = 100):
        return self._zk.create_sequence('/queue/items/item-%03d-' % priority, str(data))

class Consumer(object):

    def __init__(self, zk, active = None):
        self._zk = zk
        self._id = None
        self.active = active

        map(self._zk.ensure_exists, ('/queue', '/queue/items', 
            '/queue/consumers', '/queue/partial'))
        self._register()

    def _register(self):
        self._id = self._zk.create("/queue/consumers/consumer-", '', 
            [ZOO_OPEN_ACL_UNSAFE], zookeeper.SEQUENCE)

        if self.active is None:
            self._zk.create(self._fullpath('/active'), '',
                [ZOO_OPEN_ACL_UNSAFE], zookeeper.EPHEMERAL)
        elif self.active:
            self._zk.create(self._fullpath('/active'), '',
                [ZOO_OPEN_ACL_UNSAFE], 0)

        self._zk.create(self._fullpath('/item'), '',
            [ZOO_OPEN_ACL_UNSAFE], 0)

    def _fullpath(self, suffix):
        return self._id + suffix

    @retry_on(zookeeper.BadVersionException)
    def _move(self, src, dest):
        try:
            (data, stat) = self._zk.get(src, None)
            if data:
                # create a temporary optimistic lock by forcing a version increase
                self._zk.set(src, data, stat['version'])
                self._zk.set(dest, data)
                self._zk.delete(src, stat['version'] + 1)
                return data

            elif stat['ctime'] < (time.time() - 300): # 5 minutes
                # a producer failed to enqueue an element
                # the consumer should just drop the empty item
                try:
                    self._zk.delete(src)
                except zookeeper.NoNodeException:
                    pass # someone else already removed the node

                return None

        except zookeeper.NoNodeException:
            # a consumer already reserved this znode
            self._zk.set(dest, '')
            return None

    def reserve(self, block = False):
        if block:
            return self._blocking_reserve()
        else:
            return self._simple_reserve()

    def _blocking_reserve(self):
        def queue_watcher(*args, **kwargs):
            self._zk._cv.acquire()
            try:
                self._zk._cv.notify()
            finally:
                self._zk._cv.release()

        while True:
            self._zk._cv.acquire()
            try:
                children = sorted(self._zk.get_children('/queue/items', queue_watcher))
                for child in children:
                    data = self._move('/queue/items/' + child, self._fullpath('/item'))
                    if data:
                        return data
                self._zk._cv.wait()
            finally:
                self._zk._cv.release()

    def _simple_reserve(self):
        while True:
            children = sorted(self._zk.get_children('/queue/items', None))
            if len(children) == 0:
                return None
            for child in children:
                data = self._move('/queue/items/' + child, self._fullpath('/item'))
                if data: return data

    def done(self):
        self._zk.set(self._fullpath('/item'), '')

    def close(self):
        map(self._zk.delete, (self._fullpath('/item'), 
            self._fullpath('/active'), self._id))
        self._id = None

    def __repr__(self):
        return '<Consumer id=%r>' % self._id

class GarbageCollector(object):

    def __init__(self, zk):
        self._zk = zk

        map(self._zk.ensure_exists, ('/queue', 
            '/queue/consumers', '/queue/partial'))
 
    def _get_dlock(self):
        try:
            self._zk.create('/queue/gc-lock', '', [ZOO_OPEN_ACL_UNSAFE], zookeeper.EPHEMERAL)
            return True

        except zookeeper.NodeExistsException:
            return False

    def _remove_dlock(self):
        try:
            self._zk.delete('/queue/gc-lock')
        except zookeeper.NoNodeException:
            pass 
        
    def collect(self):
        if self._get_dlock():
            try:
                children = self._zk.get_children('/queue/consumers', None)
                for child in children:
                    cpath = '/queue/consumers/' + child 
                    if not self._zk.exists(cpath + '/active'):
                        (data, stat) = self._zk.get(cpath, None)
                        if stat['ctime'] < (time.time() - 300): # 5 minutes
                            (data, stat) = self._zk.get(cpath + '/item', None)
                            if data:
                                self._zk.create_sequence('/queue/partial/item-', data)

                            try:
                                self._zk.delete(cpath + '/item')
                            except zookeeper.NoNodeException: pass # already removed

                            try:
                                self._zk.delete(cpath)
                            except zookeeper.NoNodeException: pass # already removed
            finally:
                self._remove_dlock()

if __name__ == '__main__':
    zk = ZooKeeper("localhost:2181,localhost:2182,localhost:2183,"\
        "localhost:2184,localhost:2185")

    p = Producer(zk)
    map(p.put, map(str, range(1,10)))

    c = Consumer(zk)
    print c

    # do a blocking reserve
    print c.reserve(block = True)
    c.done()

    # remove all the remaining elements without blocking
    while True:
        data = c.reserve()
        if not data: break
        print data
        c.done()
    c.close()

    # do garbage collection
    gc = GarbageCollector(zk)
    gc.collect()

    zk.close()

