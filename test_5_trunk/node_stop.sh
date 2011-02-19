#!/bin/bash

PIDFILE=./localhost:$1/zookeeper_server.pid

if [ -e "$PIDFILE" ]
then
    kill -9 $(cat $PIDFILE)
    rm $PIDFILE
else
    echo "Missing $PIDFILE, not stopping respective server"
fi


