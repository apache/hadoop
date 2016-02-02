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


MYNAME="${BASH_SOURCE-$0}"

function hadoop_usage
{
  hadoop_generate_usage "${MYNAME}" false
}

bin=$(cd -P -- "$(dirname -- "${MYNAME}")" >/dev/null && pwd -P)

# let's locate libexec...
if [[ -n "${HADOOP_PREFIX}" ]]; then
  HADOOP_DEFAULT_LIBEXEC_DIR="${HADOOP_PREFIX}/libexec"
else
  HADOOP_DEFAULT_LIBEXEC_DIR="${bin}/../libexec"
fi

HADOOP_LIBEXEC_DIR="${HADOOP_LIBEXEC_DIR:-$HADOOP_DEFAULT_LIBEXEC_DIR}"
# shellcheck disable=SC2034
HADOOP_NEW_CONFIG=true
if [[ -f "${HADOOP_LIBEXEC_DIR}/yarn-config.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/yarn-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/yarn-config.sh." 2>&1
  exit 1
fi

# stop resourceManager
HARM=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -confKey yarn.resourcemanager.ha.enabled 2>&-)
if [[ ${HARM} = "false" ]]; then
  echo "Stopping resourcemanager"
  "${HADOOP_YARN_HOME}/bin/yarn" \
      --config "${HADOOP_CONF_DIR}" \
      --daemon stop \
      resourcemanager
else
  logicals=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -confKey yarn.resourcemanager.ha.rm-ids 2>&-)
  logicals=${logicals//,/ }
  for id in ${logicals}
  do
      rmhost=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -confKey "yarn.resourcemanager.hostname.${id}" 2>&-)
      RMHOSTS="${RMHOSTS} ${rmhost}"
  done
  echo "Stopping resourcemanagers on [${RMHOSTS}]"
  "${HADOOP_YARN_HOME}/bin/yarn" \
      --config "${HADOOP_CONF_DIR}" \
      --daemon stop \
      --slaves \
      --hostnames "${RMHOSTS}" \
      resourcemanager
fi

# stop nodemanager
echo "Stopping nodemanagers"
"${HADOOP_YARN_HOME}/bin/yarn" \
    --config "${HADOOP_CONF_DIR}" \
    --slaves \
    --daemon stop \
    nodemanager

# stop proxyserver
PROXYSERVER=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -confKey  yarn.web-proxy.address 2>&- | cut -f1 -d:)
if [[ -n ${PROXYSERVER} ]]; then
  echo "Stopping proxy server [${PROXYSERVER}]"
  "${HADOOP_YARN_HOME}/bin/yarn" \
      --config "${HADOOP_CONF_DIR}" \
      --slaves \
      --hostnames "${PROXYSERVER}" \
      --daemon stop \
      proxyserver
fi

