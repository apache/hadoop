#!/bin/sh

# Stop hadoop map reduce daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

"$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR stop jobtracker
"$bin"/hadoop-daemons.sh --config $HADOOP_CONF_DIR stop tasktracker

