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

# Modelled after $HADOOP_HOME/bin/start-hbase.sh.

# Start hadoop hbase daemons.
# Run this on master node.
usage="Usage: start-hbase.sh"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hbase-config.sh

# start hbase daemons
# TODO: PUT BACK !!! "${HADOOP_HOME}"/bin/hadoop dfsadmin -safemode wait
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi

"$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" start zookeeper
"$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" start master 
"$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
  --hosts "${HBASE_REGIONSERVERS}" start regionserver
