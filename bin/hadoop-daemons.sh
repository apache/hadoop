#!/bin/sh
# 
# Run a Hadoop command on all slave hosts.

usage="Usage: hadoop-daemons.sh [--config confdir] [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/hadoop-config.sh

exec "$bin/slaves.sh" --config $HADOOP_CONF_DIR cd "$HADOOP_HOME" \; "$bin/hadoop-daemon.sh" --config $HADOOP_CONF_DIR "$@"
