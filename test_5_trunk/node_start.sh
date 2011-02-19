#! /bin/bash

java -cp log4j.jar:zookeeper.jar:. org.apache.zookeeper.server.quorum.QuorumPeerMain ./localhost:$1/zoo.cfg > ./localhost:$1/zoo.log 2>&1 &
echo -n $! > ./localhost:$1/zookeeper_server.pid


