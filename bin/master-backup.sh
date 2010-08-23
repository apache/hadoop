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
# Run a shell command on all backup master hosts.
#
# Environment Variables
#
#   HBASE_BACKUP_MASTERS File naming remote hosts.
#     Default is ${HADOOP_CONF_DIR}/backup-masters
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HBASE_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HADOOP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HADOOP_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage="Usage: $0 [--config <hbase-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hbase-config.sh

# If the master backup file is specified in the command line,
# then it takes precedence over the definition in 
# hbase-env.sh. Save it here.
HOSTLIST=$HBASE_BACKUP_MASTERS

if [ "$HOSTLIST" = "" ]; then
  if [ "$HBASE_BACKUP_MASTERS" = "" ]; then
    export HOSTLIST="${HBASE_CONF_DIR}/backup-masters"
  else
    export HOSTLIST="${HBASE_BACKUP_MASTERS}"
  fi
fi


args=${@// /\\ }
args=${args/master-backup/master}

if [ -f $HOSTLIST ]; then
  for hmaster in `cat "$HOSTLIST"`; do
   ssh $HBASE_SSH_OPTS $hmaster $"$args --backup" \
     2>&1 | sed "s/^/$hmaster: /" &
   if [ "$HBASE_SLAVE_SLEEP" != "" ]; then
     sleep $HBASE_SLAVE_SLEEP
   fi
  done
fi 

wait
