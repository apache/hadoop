#!/bin/sh
# Modelled after $HADOOP_HOME/bin/start-hbase.sh.

# Start hadoop hbase daemons.
# Run this on master node.
usage="Usage: start-hbase.sh"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hbase-config.sh

# start hbase daemons
"$bin"/hbase-daemon.sh --config="${HADOOP_CONF_DIR}" --hbaseconfig="${HBASE_CONF_DIR}" master start
"$bin"/hbase-daemons.sh --config="${HADOOP_CONF_DIR}" --hbaseconfig="${HBASE_CONF_DIR}" regionserver start
