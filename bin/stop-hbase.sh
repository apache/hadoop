#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Modelled after $HADOOP_HOME/bin/stop-hbase.sh.

# Stop hadoop hbase daemons.  Run this on master node.

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hbase-config.sh

# variables needed for stop command
if [ "$HBASE_LOG_DIR" = "" ]; then
  export HBASE_LOG_DIR="$HBASE_HOME/logs"
fi
mkdir -p "$HBASE_LOG_DIR"

if [ "$HBASE_IDENT_STRING" = "" ]; then
  export HBASE_IDENT_STRING="$USER"
fi

export HBASE_LOGFILE=hbase-$HBASE_IDENT_STRING-master-$HOSTNAME.log
logout=$HBASE_LOG_DIR/hbase-$HBASE_IDENT_STRING-master-$HOSTNAME.out  
loglog="${HBASE_LOG_DIR}/${HBASE_LOGFILE}"
pid=${HBASE_PID_DIR:-/tmp}/hbase-$HBASE_IDENT_STRING-master.pid

echo -n stopping hbase
echo "`date` Stopping hbase (via master)" >> $loglog

nohup nice -n ${HBASE_NICENESS:-0} "$HBASE_HOME"/bin/hbase \
   --config "${HBASE_CONF_DIR}" \
   master stop "$@" > "$logout" 2>&1 < /dev/null &

while kill -0 `cat $pid` > /dev/null 2>&1; do
  echo -n "."
  sleep 1;
done

# distributed == false means that the HMaster will kill ZK when it exits
distMode=`$bin/hbase org.apache.hadoop.hbase.HBaseConfTool hbase.cluster.distributed`
if [ "$distMode" == 'true' ] 
then
  # TODO: store backup masters in ZooKeeper and have the primary send them a shutdown message
  # stop any backup masters
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
    --hosts "${HBASE_BACKUP_MASTERS}" stop master-backup

  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" stop zookeeper
fi
