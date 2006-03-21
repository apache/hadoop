#!/bin/bash

# Start all hadoop daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

"$bin"/hadoop-daemon.sh start namenode
"$bin"/hadoop-daemons.sh start datanode
"$bin"/hadoop-daemon.sh start jobtracker
"$bin"/hadoop-daemons.sh start tasktracker
