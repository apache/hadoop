#!/usr/bin/env bash
#
#/**
# * Copyright 2009 The Apache Software Foundation
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
# Run a shell command on all zookeeper hosts.
#
# Environment Variables
#
#   HBASE_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HBASE_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HBASE_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage="Usage: zookeepers [--config <hbase-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hbase-config.sh

if [ "$HBASE_MANAGES_ZK" = "" ]; then
  HBASE_MANAGES_ZK=true
fi

if [ "$HBASE_MANAGES_ZK" = "true" ]; then
  hosts=`"$bin"/hbase org.apache.hadoop.hbase.zookeeper.ZKServerTool | grep '^ZK host:' | sed 's,^ZK host:,,'`
  cmd=$"${@// /\\ }"
  for zookeeper in $hosts; do
   ssh $HBASE_SSH_OPTS $zookeeper $cmd 2>&1 | sed "s/^/$zookeeper: /" &
   if [ "$HBASE_SLAVE_SLEEP" != "" ]; then
     sleep $HBASE_SLAVE_SLEEP
   fi
  done
fi

wait
