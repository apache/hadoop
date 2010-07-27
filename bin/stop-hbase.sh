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

"$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" stop master
distMode=`$bin/hbase org.apache.hadoop.hbase.HBaseConfTool hbase.cluster.distributed`
if [ "$distMode" == 'true' ] 
then
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" stop zookeeper
fi
