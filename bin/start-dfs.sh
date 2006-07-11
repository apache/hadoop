#!/bin/sh

# Start hadoop dfs daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# start dfs daemons
# start namenode after datanodes, to minimize time namenode is up w/o data
# note: datanodes will log connection errors until namenode starts
"$bin"/hadoop-daemon.sh start namenode
"$bin"/hadoop-daemons.sh start datanode
