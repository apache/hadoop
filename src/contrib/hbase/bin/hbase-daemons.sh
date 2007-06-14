#!/bin/sh
# 
# Run a Hadoop hbase command on all slave hosts.
# Modelled after $HADOOP_HOME/bin/hadoop-daemons.sh

usage="Usage: hbase-daemons.sh [--config=<confdir>] [--hbaseconfig=<hbase-confdir>] [--hosts=regionserversfile] command [start|stop] args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/hbase-config.sh

exec "$bin/regionservers.sh" --config="${HADOOP_CONF_DIR}" --hbaseconfig="${HBASE_CONF_DIR}" cd "$HBASE_HOME" \; "$bin/hbase-daemon.sh" --config="${HADOOP_CONF_DIR}" --hbaseconfig="${HBASE_CONF_DIR}" "$@"
