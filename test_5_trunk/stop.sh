#!/bin/bash

./rest/rest.sh stop >/dev/null 2>&1

PIDFILE=./localhost:2181/zookeeper_server.pid

if [ -e "$PIDFILE" ]
then
    kill -9 $(cat $PIDFILE)
    rm $PIDFILE
else
    echo "Missing $PIDFILE, not stopping respective server"
fi

PIDFILE=./localhost:2182/zookeeper_server.pid

if [ -e "$PIDFILE" ]
then
    kill -9 $(cat $PIDFILE)
    rm $PIDFILE
else
    echo "Missing $PIDFILE, not stopping respective server"
fi

PIDFILE=./localhost:2183/zookeeper_server.pid

if [ -e "$PIDFILE" ]
then
    kill -9 $(cat $PIDFILE)
    rm $PIDFILE
else
    echo "Missing $PIDFILE, not stopping respective server"
fi

PIDFILE=./localhost:2184/zookeeper_server.pid

if [ -e "$PIDFILE" ]
then
    kill -9 $(cat $PIDFILE)
    rm $PIDFILE
else
    echo "Missing $PIDFILE, not stopping respective server"
fi

PIDFILE=./localhost:2185/zookeeper_server.pid

if [ -e "$PIDFILE" ]
then
    kill -9 $(cat $PIDFILE)
    rm $PIDFILE
else
    echo "Missing $PIDFILE, not stopping respective server"
fi

