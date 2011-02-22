#!/bin/bash

PIDFILE=./localhost:$1/zookeeper_server.pid

if [ -e "$PIDFILE" ]
then
    kill -9 $(cat $PIDFILE)
    rm $PIDFILE
    rm -f ./localhost:$1/data/version-2/*
else
    echo "Missing $PIDFILE, not stopping respective server"
fi


