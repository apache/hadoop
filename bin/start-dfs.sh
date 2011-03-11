#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Start hadoop dfs daemons.
# Optinally upgrade or rollback dfs state.
# Run this on master node.

usage="Usage: start-dfs.sh [-upgrade|-rollback]"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin/hdfs-config.sh"

# get arguments
if [ $# -ge 1 ]; then
	nameStartOpt="$1"
	shift
	case "$nameStartOpt" in
	  (-upgrade)
	  	;;
	  (-rollback) 
	  	dataStartOpt="$nameStartOpt"
	  	;;
	  (*)
		  echo $usage
		  exit 1
	    ;;
	esac
fi

#---------------------------------------------------------
# namenodes

NAMENODES=$($HADOOP_HOME/bin/hdfs getconf -namenodes)

echo "Starting namenodes on [$NAMENODES]"

"$HADOOP_COMMON_HOME/bin/hadoop-daemons.sh" \
  --config "$HADOOP_CONF_DIR" \
  --hostnames "$NAMENODES" \
  --script "$bin/hdfs" start namenode $nameStartOpt

#---------------------------------------------------------
# datanodes (using defalut slaves file)

if [ -n "$HADOOP_SECURE_DN_USER" ]; then
  echo \
    "Attempting to start secure cluster, skipping datanodes. " \
    "Run start-secure-dns.sh as root to complete startup."
else
  "$HADOOP_COMMON_HOME/bin/hadoop-daemons.sh" \
    --config "$HADOOP_CONF_DIR" \
    --script "$bin/hdfs" start datanode $dataStartOpt
fi

#---------------------------------------------------------
# secondary namenodes (if any)

# if there are no secondary namenodes configured it returns
# 0.0.0.0 or empty string
SECONDARY_NAMENODES=$($HADOOP_HOME/bin/hdfs getconf -secondarynamenodes 2>&-)
SECONDARY_NAMENODES=${SECONDARY_NAMENODES:='0.0.0.0'}

if [ "$SECONDARY_NAMENODES" = '0.0.0.0' ] ; then
  echo \
    "Secondary namenodes are not configured. " \
    "Cannot start secondary namenodes."
else
  echo "Starting secondary namenodes [$SECONDARY_NAMENODES]"

  "$HADOOP_COMMON_HOME/bin/hadoop-daemons.sh" \
    --config "$HADOOP_CONF_DIR" \
    --hostnames "$SECONDARY_NAMENODES" \
    --script "$bin/hdfs" start secondarynamenode
fi

# eof
