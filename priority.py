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

from zkmq import ZooKeeper, Producer, Consumer

def create_zk():
    quorum = "localhost:2181,localhost:2182,localhost:2183,"\
        "localhost:2184,localhost:2185"

    if 'ZOOKEEPER_QUORUM' in os.environ:
        quorum = os.environ['ZOOKEEPER_QUORUM']

    return ZooKeeper(quorum)

def main(zk):
    p = Producer(zk)

    p.put(1, priority = 0)
    p.put(2, priority = 50)
    p.put(3) # default to lowest priority

    c = Consumer(zk)
    try:
        elements = []
        while True:
            data = c.reserve()
            if data is None: break
            elements.append(int(data))

        assert elements == [1,2,3]
        print "Ok."

    finally:
        c.close()        

if __name__ == '__main__':
    zk = create_zk()
    try:
        main(zk)
    finally:
        zk.close()



