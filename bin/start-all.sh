#!/bin/sh

# Start all hadoop daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

# start dfs daemons
"$bin"/start-dfs.sh --config $HADOOP_CONF_DIR

# start mapred daemons
"$bin"/start-mapred.sh --config $HADOOP_CONF_DIR
