#! /usr/bin/env python

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
import time

from zkmq import ZooKeeper, Producer, Consumer

def main():
    return parent_main() if os.fork() else child_main()

def create_zk():
    quorum = "localhost:2181,localhost:2182,localhost:2183,"\
        "localhost:2184,localhost:2185"

    if 'ZOOKEEPER_QUORUM' in os.environ:
        quorum = os.environ['ZOOKEEPER_QUORUM']

    return ZooKeeper(quorum)

def parent_main():
    zk = create_zk()

    try:
        p = Producer(zk)
        map(p.put, map(str, range(1, 501)))

        print >>sys.stderr, 'Sent all 500 elements ...'

    finally: 
        zk.close()

def child_main():
    zk = create_zk()
    c = Consumer(zk)

    try:
        count = 0
        while True:
            data = c.reserve(block = False)
            if not data:
                time.sleep(0.5)
                continue
            print data
            c.done()

            assert count + 1 == int(data)
            count = int(data)

            if int(data) == 500: break
    finally:
        c.close()
        zk.close()
        
if __name__ == '__main__':
    sys.exit(main())

