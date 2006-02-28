#!/bin/bash
# 
# Run a Hadoop command on all slave hosts.

usage="Usage: hadoop-daemons.sh [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

exec "$bin/slaves.sh" "$bin/hadoop-daemon.sh" "$@"
