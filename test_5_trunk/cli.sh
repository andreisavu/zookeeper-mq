#!/bin/bash
java -cp zookeeper.jar:log4j.jar:jline.jar:. org.apache.zookeeper.ZooKeeperMain -server "$1"
