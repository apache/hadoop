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

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $HADOOP_LIBEXEC_DIR/hdfs-config.sh

#---------------------------------------------------------
# namenodes

NAMENODES=$($HADOOP_PREFIX/bin/hdfs getconf -namenodes)

echo "Stopping namenodes on [$NAMENODES]"

"$HADOOP_PREFIX/sbin/hadoop-daemons.sh" \
  --config "$HADOOP_CONF_DIR" \
  --hostnames "$NAMENODES" \
  --script "$bin/hdfs" stop namenode

#---------------------------------------------------------
# datanodes (using default slaves file)

if [ -n "$HADOOP_SECURE_DN_USER" ]; then
  echo \
    "Attempting to stop secure cluster, skipping datanodes. " \
    "Run stop-secure-dns.sh as root to complete shutdown."
else
  "$HADOOP_PREFIX/sbin/hadoop-daemons.sh" \
    --config "$HADOOP_CONF_DIR" \
    --script "$bin/hdfs" stop datanode
fi

#---------------------------------------------------------
# secondary namenodes (if any)

SECONDARY_NAMENODES=$($HADOOP_PREFIX/bin/hdfs getconf -secondarynamenodes 2>/dev/null)

if [ -n "$SECONDARY_NAMENODES" ]; then
  echo "Stopping secondary namenodes [$SECONDARY_NAMENODES]"

  "$HADOOP_PREFIX/sbin/hadoop-daemons.sh" \
      --config "$HADOOP_CONF_DIR" \
      --hostnames "$SECONDARY_NAMENODES" \
      --script "$bin/hdfs" stop secondarynamenode
fi

#---------------------------------------------------------
# quorumjournal nodes (if any)

SHARED_EDITS_DIR=$($HADOOP_PREFIX/bin/hdfs getconf -confKey dfs.namenode.shared.edits.dir 2>&-)

case "$SHARED_EDITS_DIR" in
qjournal://*)
  JOURNAL_NODES=$(echo "$SHARED_EDITS_DIR" | sed 's,qjournal://\([^/]*\)/.*,\1,g; s/;/ /g; s/:[0-9]*//g')
  echo "Stopping journal nodes [$JOURNAL_NODES]"
  "$HADOOP_PREFIX/sbin/hadoop-daemons.sh" \
      --config "$HADOOP_CONF_DIR" \
      --hostnames "$JOURNAL_NODES" \
      --script "$bin/hdfs" stop journalnode ;;
esac

#---------------------------------------------------------
# ZK Failover controllers, if auto-HA is enabled
AUTOHA_ENABLED=$($HADOOP_PREFIX/bin/hdfs getconf -confKey dfs.ha.automatic-failover.enabled)
if [ "$(echo "$AUTOHA_ENABLED" | tr A-Z a-z)" = "true" ]; then
  echo "Stopping ZK Failover Controllers on NN hosts [$NAMENODES]"
  "$HADOOP_PREFIX/sbin/hadoop-daemons.sh" \
    --config "$HADOOP_CONF_DIR" \
    --hostnames "$NAMENODES" \
    --script "$bin/hdfs" stop zkfc
fi
# eof
