#!/bin/sh
# Modelled after $HADOOP_HOME/bin/stop-hbase.sh.

# Stop hadoop hbase daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hbase-config.sh

"$bin"/hbase-daemon.sh --config="${HADOOP_CONF_DIR}" --hbaseconfig="${HBASE_CONF_DIR}" master stop
