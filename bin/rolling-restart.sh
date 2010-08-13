#!/usr/bin/env bash
#
#/**
# * Copyright 2010 The Apache Software Foundation
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
#
# Run a shell command on all regionserver hosts.
#
# Environment Variables
#
#   HBASE_REGIONSERVERS    File naming remote hosts.
#     Default is ${HADOOP_CONF_DIR}/regionservers
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HBASE_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HADOOP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HADOOP_SLAVE_TIMEOUT Seconds to wait for timing out a remote command.
#   HADOOP_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage="Usage: $0 [--config <hbase-confdir>] commands..."

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hbase-config.sh

# start hbase daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi

# quick function to get a value from the HBase config file
distMode=`$bin/hbase org.apache.hadoop.hbase.HBaseConfTool hbase.cluster.distributed`
if [ "$distMode" == 'false' ]; then
  "$bin"/hbase-daemon.sh restart master
else 
  # stop all masters before re-start to avoid races for master znode
  "$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" stop master 
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
    --hosts "${HBASE_BACKUP_MASTERS}" stop master-backup

  # make sure the master znode has been deleted before continuing
  zparent=`$bin/hbase org.apache.hadoop.hbase.HBaseConfTool zookeeper.znode.parent`
  if [ "$zparent" == "null" ]; then zparent="/hbase"; fi
  zmaster=`$bin/hbase org.apache.hadoop.hbase.HBaseConfTool zookeeper.znode.master`
  if [ "$zmaster" == "null" ]; then zmaster="master"; fi
  zmaster=$zparent/$zmaster
  echo -n "Waiting for Master ZNode to expire"
  while bin/hbase zkcli stat $zmaster >/dev/null 2>&1; do
    echo -n "."
    sleep 1
  done
  echo #force a newline

  # all masters are down, now restart
  "$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" start master
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
    --hosts "${HBASE_BACKUP_MASTERS}" start master-backup

  # unlike the masters, roll all regionservers one-at-a-time
  export HBASE_SLAVE_PARALLEL=false
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
    --hosts "${HBASE_REGIONSERVERS}" restart regionserver

fi

