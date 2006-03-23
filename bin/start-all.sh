#!/bin/bash

# Start all hadoop daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# start dfs daemons
# start namenode after datanodes, to minimize time namenode is up w/o data
# note: datanodes will log connection errors until namenode starts
"$bin"/hadoop-daemons.sh start datanode
"$bin"/hadoop-daemon.sh start namenode

# start mapred daemons
# start jobtracker first to minimize connection errors at startup
"$bin"/hadoop-daemon.sh start jobtracker
"$bin"/hadoop-daemons.sh start tasktracker
