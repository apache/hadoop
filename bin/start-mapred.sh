#!/bin/sh

# Start hadoop map reduce daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

# start mapred daemons
# start jobtracker first to minimize connection errors at startup
"$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR start jobtracker
"$bin"/hadoop-daemons.sh --config $HADOOP_CONF_DIR start tasktracker
