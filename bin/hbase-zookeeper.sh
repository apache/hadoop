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
# Run a hbase command on all slave hosts.
# Modelled after $HADOOP_HOME/bin/hadoop-daemons.sh

usage="Usage: hbase-daemons.sh [--config <hbase-confdir>] \
 [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/hbase-config.sh

exec "$bin/zookeeper.sh" --config "${HBASE_CONF_DIR}" \
 cd "${HBASE_HOME}" \; \
 "$bin/hbase-daemon.sh" --config "${HBASE_CONF_DIR}" "$@"
