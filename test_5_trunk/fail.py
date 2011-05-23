#! /usr/bin/env python
""" Make node from a local ZooKeeper cluster fail """

import sys
import os
import re
import optparse
import random
import time
import signal
import socket

from subprocess import call

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    nodes = get_nodes()
    leader = True if len(sys.argv) == 2 and  sys.argv[1] == 'leader' else False
    while True:
        if leader:
            n = find_leader(nodes)
        else:
            n = random.choice(nodes)
        kill_node(n)
	time.sleep(5)
        start_node(n)
        wait_join_cluster(n)
        time.sleep(5)

def find_leader(nodes):
    for host, port in nodes:
        try:
            s = socket.socket()
            s.connect((host, int(port)))
            s.send('stat')
            if 'Mode: leader' in s.recv(1024):
                return (host, port)
        finally:
            s.close()
            
def kill_node(n):
    print 'stoping node %s ...' % (n,)
    host, port = n
    call(['./node_stop.sh %s' % port], shell=True)

def start_node(n):
    print 're-starting the node %s ...' % (n,)
    host, port = n
    call(['./node_start.sh %s' % port], shell=True) 

def wait_join_cluster(n):
    print 'waiting for node to join the cluster ...'
    # TODO wait for ruok - imok
    time.sleep(5)

def get_nodes():
    nodes = []
    for f in os.listdir(CURRENT_DIR):
        if os.path.isdir(f):
            m = re.match('^(\w+?):(\d+)$', f)
            if m is not None:
                nodes.append(m.group(1,2))
    return nodes

if __name__ == '__main__':
    sys.exit(main())

