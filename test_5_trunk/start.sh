#!/bin/bash

java -cp log4j.jar:zookeeper.jar:. org.apache.zookeeper.server.quorum.QuorumPeerMain ./localhost:2181/zoo.cfg > ./localhost:2181/zoo.log 2>&1 &
echo -n $! > ./localhost:2181/zookeeper_server.pid

java -cp log4j.jar:zookeeper.jar:. org.apache.zookeeper.server.quorum.QuorumPeerMain ./localhost:2182/zoo.cfg > ./localhost:2182/zoo.log 2>&1 &
echo -n $! > ./localhost:2182/zookeeper_server.pid

java -cp log4j.jar:zookeeper.jar:. org.apache.zookeeper.server.quorum.QuorumPeerMain ./localhost:2183/zoo.cfg > ./localhost:2183/zoo.log 2>&1 &
echo -n $! > ./localhost:2183/zookeeper_server.pid

java -cp log4j.jar:zookeeper.jar:. org.apache.zookeeper.server.quorum.QuorumPeerMain ./localhost:2184/zoo.cfg > ./localhost:2184/zoo.log 2>&1 &
echo -n $! > ./localhost:2184/zookeeper_server.pid

java -cp log4j.jar:zookeeper.jar:. org.apache.zookeeper.server.quorum.QuorumPeerMain ./localhost:2185/zoo.cfg > ./localhost:2185/zoo.log 2>&1 &
echo -n $! > ./localhost:2185/zookeeper_server.pid

./rest/rest.sh start >/dev/null 2>&1

