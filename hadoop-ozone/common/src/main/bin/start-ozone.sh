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
if [[ -f "${HADOOP_LIBEXEC_DIR}/ozone-config.sh" ]]; then
  # shellcheck disable=SC1090
  . "${HADOOP_LIBEXEC_DIR}/ozone-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/ozone-config.sh." 2>&1
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

SECURITY_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getconf -confKey hadoop.security.authentication | tr '[:upper:]' '[:lower:]' 2>&-)
SECURITY_AUTHORIZATION_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getconf -confKey hadoop.security.authorization | tr '[:upper:]' '[:lower:]' 2>&-)

#if [[ ${SECURITY_ENABLED} == "kerberos" || ${SECURITY_AUTHORIZATION_ENABLED}
# == "true" ]]; then
#  echo "Ozone is not supported in a security enabled cluster."
#  exit 1
#fi

#SECURITY_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getozoneconf -confKey hadoop.security.authentication | tr '[:upper:]' '[:lower:]' 2>&-)
#SECURITY_AUTHORIZATION_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getozoneconf -confKey hadoop.security.authorization | tr '[:upper:]' '[:lower:]' 2>&-)
#if [[ ${SECURITY_ENABLED} == "kerberos" || ${SECURITY_AUTHORIZATION_ENABLED} == "true" ]]; then
#  echo "Ozone is not supported in a security enabled cluster."
#  exit 1
#fi

#---------------------------------------------------------
# Check if ozone is enabled
OZONE_ENABLED=$("${HADOOP_HDFS_HOME}/bin/ozone" getconf -confKey ozone.enabled | tr '[:upper:]' '[:lower:]' 2>&-)
if [[ "${OZONE_ENABLED}" != "true" ]]; then
  echo "Operation is not supported because ozone is not enabled."
  exit -1
fi

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
# Ozone ozonemanager nodes
OM_NODES=$("${HADOOP_HDFS_HOME}/bin/ozone" getconf -ozonemanagers 2>/dev/null)
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
SCM_NODES=$("${HADOOP_HDFS_HOME}/bin/ozone" getconf -storagecontainermanagers 2>/dev/null)
echo "Starting storage container manager nodes [${SCM_NODES}]"
hadoop_uservar_su hdfs scm "${HADOOP_HDFS_HOME}/bin/ozone" \
  --workers \
  --config "${HADOOP_CONF_DIR}" \
  --hostnames "${SCM_NODES}" \
  --daemon start \
  scm

(( HADOOP_JUMBO_RETCOUNTER=HADOOP_JUMBO_RETCOUNTER + $? ))

exit ${HADOOP_JUMBO_RETCOUNTER}
