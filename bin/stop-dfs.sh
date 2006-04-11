#!/bin/bash

# Stop hadoop DFS daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

"$bin"/hadoop-daemon.sh stop namenode
"$bin"/hadoop-daemons.sh stop datanode

