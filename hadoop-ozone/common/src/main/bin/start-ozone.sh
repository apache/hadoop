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

# Start hadoop hdfs and ozone daemons.
# Run this on master node.
## @description  usage info
## @audience     private
## @stability    evolving
## @replaceable  no
function hadoop_usage
{
  echo "Usage: start-ozone.sh"
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
  # shellcheck disable=SC1090
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

SECURITY_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getozoneconf -confKey hadoop.security.authentication | tr '[:upper:]' '[:lower:]' 2>&-)
SECURITY_AUTHORIZATION_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getozoneconf -confKey hadoop.security.authorization | tr '[:upper:]' '[:lower:]' 2>&-)

if [[ ${SECURITY_ENABLED} == "kerberos" || ${SECURITY_AUTHORIZATION_ENABLED} == "true" ]]; then
  echo "Ozone is not supported in a security enabled cluster."
  exit 1
fi

#---------------------------------------------------------
# Check if ozone is enabled
OZONE_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getozoneconf -confKey ozone.enabled | tr '[:upper:]' '[:lower:]' 2>&-)
if [[ "${OZONE_ENABLED}" != "true" ]]; then
  echo "Operation is not supported because ozone is not enabled."
  exit -1
fi

#---------------------------------------------------------
# Start hdfs before starting ozone daemons

#---------------------------------------------------------
# namenodes

NAMENODES=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -namenodes 2>/dev/null)

if [[ -z "${NAMENODES}" ]]; then
  NAMENODES=$(hostname)
fi

echo "Starting namenodes on [${NAMENODES}]"
hadoop_uservar_su hdfs namenode "${HADOOP_HDFS_HOME}/bin/hdfs" \
    --workers \
    --config "${HADOOP_CONF_DIR}" \
    --hostnames "${NAMENODES}" \
    --daemon start \
    namenode ${nameStartOpt}

HADOOP_JUMBO_RETCOUNTER=$?

#---------------------------------------------------------
# datanodes (using default workers file)

echo "Starting datanodes"
hadoop_uservar_su hdfs datanode "${HADOOP_HDFS_HOME}/bin/ozone" \
    --workers \
    --config "${HADOOP_CONF_DIR}" \
    --daemon start \
    datanode ${dataStartOpt}
(( HADOOP_JUMBO_RETCOUNTER=HADOOP_JUMBO_RETCOUNTER + $? ))

#---------------------------------------------------------
# secondary namenodes (if any)

SECONDARY_NAMENODES=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -secondarynamenodes 2>/dev/null)

if [[ -n "${SECONDARY_NAMENODES}" ]]; then

  if [[ "${NAMENODES}" =~ , ]]; then

    hadoop_error "WARNING: Highly available NameNode is configured."
    hadoop_error "WARNING: Skipping SecondaryNameNode."

  else

    if [[ "${SECONDARY_NAMENODES}" == "0.0.0.0" ]]; then
      SECONDARY_NAMENODES=$(hostname)
    fi

    echo "Starting secondary namenodes [${SECONDARY_NAMENODES}]"

    hadoop_uservar_su hdfs secondarynamenode "${HADOOP_HDFS_HOME}/bin/hdfs" \
      --workers \
      --config "${HADOOP_CONF_DIR}" \
      --hostnames "${SECONDARY_NAMENODES}" \
      --daemon start \
      secondarynamenode
    (( HADOOP_JUMBO_RETCOUNTER=HADOOP_JUMBO_RETCOUNTER + $? ))
  fi
fi

#---------------------------------------------------------
# quorumjournal nodes (if any)

JOURNAL_NODES=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -journalNodes 2>&-)

if [[ "${#JOURNAL_NODES}" != 0 ]]; then
  echo "Starting journal nodes [${JOURNAL_NODES}]"

  hadoop_uservar_su hdfs journalnode "${HADOOP_HDFS_HOME}/bin/hdfs" \
    --workers \
    --config "${HADOOP_CONF_DIR}" \
    --hostnames "${JOURNAL_NODES}" \
    --daemon start \
    journalnode
   (( HADOOP_JUMBO_RETCOUNTER=HADOOP_JUMBO_RETCOUNTER + $? ))
fi

#---------------------------------------------------------
# ZK Failover controllers, if auto-HA is enabled
AUTOHA_ENABLED=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -confKey dfs.ha.automatic-failover.enabled | tr '[:upper:]' '[:lower:]')
if [[ "${AUTOHA_ENABLED}" = "true" ]]; then
  echo "Starting ZK Failover Controllers on NN hosts [${NAMENODES}]"

  hadoop_uservar_su hdfs zkfc "${HADOOP_HDFS_HOME}/bin/hdfs" \
    --workers \
    --config "${HADOOP_CONF_DIR}" \
    --hostnames "${NAMENODES}" \
    --daemon start \
    zkfc
  (( HADOOP_JUMBO_RETCOUNTER=HADOOP_JUMBO_RETCOUNTER + $? ))
fi

#---------------------------------------------------------
# Ozone ozonemanager nodes
OM_NODES=$("${HADOOP_HDFS_HOME}/bin/ozone" getozoneconf -ozonemanagers 2>/dev/null)
echo "Starting Ozone Manager nodes [${OM_NODES}]"
if [[ "${OM_NODES}" == "0.0.0.0" ]]; then
  OM_NODES=$(hostname)
fi

hadoop_uservar_su hdfs om "${HADOOP_HDFS_HOME}/bin/ozone" \
  --workers \
  --config "${HADOOP_CONF_DIR}" \
  --hostnames "${OM_NODES}" \
  --daemon start \
  om

HADOOP_JUMBO_RETCOUNTER=$?

#---------------------------------------------------------
# Ozone storagecontainermanager nodes
SCM_NODES=$("${HADOOP_HDFS_HOME}/bin/ozone" getozoneconf -storagecontainermanagers 2>/dev/null)
echo "Starting storage container manager nodes [${SCM_NODES}]"
hadoop_uservar_su hdfs scm "${HADOOP_HDFS_HOME}/bin/ozone" \
  --workers \
  --config "${HADOOP_CONF_DIR}" \
  --hostnames "${SCM_NODES}" \
  --daemon start \
  scm

(( HADOOP_JUMBO_RETCOUNTER=HADOOP_JUMBO_RETCOUNTER + $? ))

exit ${HADOOP_JUMBO_RETCOUNTER}
