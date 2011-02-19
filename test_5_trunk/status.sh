#!/bin/bash
echo "localhost:2181 " $(echo stat | nc localhost 2181 | egrep "Mode: ")
echo "localhost:2182 " $(echo stat | nc localhost 2182 | egrep "Mode: ")
echo "localhost:2183 " $(echo stat | nc localhost 2183 | egrep "Mode: ")
echo "localhost:2184 " $(echo stat | nc localhost 2184 | egrep "Mode: ")
echo "localhost:2185 " $(echo stat | nc localhost 2185 | egrep "Mode: ")
