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

function hadoop_usage
{
  echo "Usage: start-dfs.sh [-upgrade|-rollback] [-clusterId]"
}

this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)

# let's locate libexec...
if [[ -n "${HADOOP_HOME}" ]]; then
  HADOOP_DEFAULT_LIBEXEC_DIR="${HADOOP_HOME}/libexec"
else
  HADOOP_DEFAULT_LIBEXEC_DIR="${bin}/../libexec"
fi

HADOOP_LIBEXEC_DIR="${HADOOP_LIBEXEC_DIR:-$HADOOP_DEFAULT_LIBEXEC_DIR}"
# shellcheck disable=SC2034
HADOOP_NEW_CONFIG=true
if [[ -f "${HADOOP_LIBEXEC_DIR}/hdfs-config.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/hdfs-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/hdfs-config.sh." 2>&1
  exit 1
fi


# get arguments
if [[ $# -ge 1 ]]; then
  startOpt="$1"
  shift
  case "$startOpt" in
    -upgrade)
      nameStartOpt="$startOpt"
    ;;
    -rollback)
      dataStartOpt="$startOpt"
    ;;
    *)
      hadoop_exit_with_usage 1
    ;;
  esac
fi


#Add other possible options
nameStartOpt="$nameStartOpt $*"

#---------------------------------------------------------
# namenodes

NAMENODES=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -namenodes 2>/dev/null)

if [[ -z "${NAMENODES}" ]]; then
  NAMENODES=$(hostname)
fi

echo "Starting namenodes on [${NAMENODES}]"

"${HADOOP_HDFS_HOME}/bin/hdfs" \
    --slaves \
    --config "${HADOOP_CONF_DIR}" \
    --hostnames "${NAMENODES}" \
    --daemon start \
    namenode ${nameStartOpt}

#---------------------------------------------------------
# datanodes (using default slaves file)

if [[ -n "${HADOOP_SECURE_DN_USER}" ]] &&
   [[ -z "${HADOOP_SECURE_COMMAND}" ]]; then
    hadoop_error "ERROR: Attempting to start secure cluster, skipping datanodes. "
    hadoop_error "ERROR: Run start-secure-dns.sh as root or configure "
    hadoop_error "ERROR: \${HADOOP_SECURE_COMMAND} to complete startup."
else

  echo "Starting datanodes"

  "${HADOOP_HDFS_HOME}/bin/hdfs" \
    --slaves \
    --config "${HADOOP_CONF_DIR}" \
    --daemon start \
    datanode ${dataStartOpt}
fi

#---------------------------------------------------------
# secondary namenodes (if any)

SECONDARY_NAMENODES=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -secondarynamenodes 2>/dev/null)

if [[ -n "${SECONDARY_NAMENODES}" ]]; then

  if [[ "${NAMENODES}" =~ , ]]; then

    hadoop_error "ERROR: Highly available NameNode is configured."
    hadoop_error "ERROR: Skipping SecondaryNameNode."

  else

    if [[ "${SECONDARY_NAMENODES}" == "0.0.0.0" ]]; then
      SECONDARY_NAMENODES=$(hostname)
    fi

    echo "Starting secondary namenodes [${SECONDARY_NAMENODES}]"

    "${HADOOP_HDFS_HOME}/bin/hdfs" \
      --slaves \
      --config "${HADOOP_CONF_DIR}" \
      --hostnames "${SECONDARY_NAMENODES}" \
      --daemon start \
      secondarynamenode
  fi
fi

#---------------------------------------------------------
# quorumjournal nodes (if any)

SHARED_EDITS_DIR=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -confKey dfs.namenode.shared.edits.dir 2>&-)

case "${SHARED_EDITS_DIR}" in
  qjournal://*)
    JOURNAL_NODES=$(echo "${SHARED_EDITS_DIR}" | sed 's,qjournal://\([^/]*\)/.*,\1,g; s/;/ /g; s/:[0-9]*//g')
    echo "Starting journal nodes [${JOURNAL_NODES}]"

    "${HADOOP_HDFS_HOME}/bin/hdfs" \
      --slaves \
      --config "${HADOOP_CONF_DIR}" \
      --hostnames "${JOURNAL_NODES}" \
      --daemon start \
      journalnode
  ;;
esac

#---------------------------------------------------------
# ZK Failover controllers, if auto-HA is enabled
AUTOHA_ENABLED=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -confKey dfs.ha.automatic-failover.enabled | tr '[:upper:]' '[:lower:]')
if [[ "${AUTOHA_ENABLED}" = "true" ]]; then
  echo "Starting ZK Failover Controllers on NN hosts [${NAMENODES}]"

  "${HADOOP_HDFS_HOME}/bin/hdfs" \
    --slaves \
    --config "${HADOOP_CONF_DIR}" \
    --hostnames "${NAMENODES}" \
    --daemon start \
    zkfc
fi

# eof
