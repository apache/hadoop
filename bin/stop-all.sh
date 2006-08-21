#!/bin/sh

# Stop all hadoop daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

"$bin"/stop-mapred.sh --config $HADOOP_CONF_DIR
"$bin"/stop-dfs.sh --config $HADOOP_CONF_DIR
