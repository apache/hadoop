#!/bin/sh

# Start all hadoop daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# start dfs daemons
"$bin"/start-dfs.sh

# start mapred daemons
"$bin"/start-mapred.sh
