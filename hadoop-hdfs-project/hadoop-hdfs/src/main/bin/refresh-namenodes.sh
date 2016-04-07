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


# ------------------------------------------------------------------
# This script refreshes all namenodes, it's a simple wrapper
# for dfsadmin to support multiple namenodes.

# let's locate libexec...
if [[ -n "${HADOOP_HOME}" ]]; then
  HADOOP_DEFAULT_LIBEXEC_DIR="${HADOOP_HOME}/libexec"
else
  this="${BASH_SOURCE-$0}"
  bin=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)
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

namenodes=$("${HADOOP_HDFS_HOME}/bin/hdfs" getconf -nnRpcAddresses)
if [[ "$?" != '0' ]] ; then
  errorFlag='1' ;
else
  for namenode in ${namenodes} ; do
    echo "Refreshing namenode [${namenode}]"
    "${HADOOP_HDFS_HOME}/bin/hdfs" dfsadmin \
    -fs hdfs://${namenode} -refreshNodes
    if [[ "$?" != '0' ]]; then
      errorFlag='1'
    fi
  done
fi

if [[ "${errorFlag}" = '1' ]] ; then
  echo "Error: refresh of namenodes failed, see error messages above."
  exit 1
else
  echo "Refresh of namenodes done."
fi


# eof
