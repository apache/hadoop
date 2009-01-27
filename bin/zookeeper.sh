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
# Run a shell command on all regionserver hosts.
#
# Environment Variables
#
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HBASE_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HADOOP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HADOOP_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage="Usage: zookeeper [--config <hbase-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hbase-config.sh

if [ -f "${HBASE_CONF_DIR}/hbase-env.sh" ]; then
  . "${HBASE_CONF_DIR}/hbase-env.sh"
fi

if [ "$HBASE_MANAGES_ZK" = "" ]; then
  HBASE_MANAGES_ZK=true
fi

if [ "$HBASE_MANAGES_ZK" = "true" ]; then
 ssh $HBASE_SSH_OPTS 127.0.0.1 $"${@// /\\ }" \
   2>&1 | sed "s/^/$zookeeper: /" &
 if [ "$HBASE_SLAVE_SLEEP" != "" ]; then
   sleep $HBASE_SLAVE_SLEEP
 fi
fi

wait
