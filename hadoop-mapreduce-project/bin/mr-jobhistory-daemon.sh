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

function hadoop_usage
{
  echo "Usage: mr-jobhistory-daemon.sh [--config confdir] (start|stop|status) <hadoop-command> <args...>"
}

# let's locate libexec...
if [[ -n "${HADOOP_PREFIX}" ]]; then
  HADOOP_DEFAULT_LIBEXEC_DIR="${HADOOP_PREFIX}/libexec"
else
  this="${BASH_SOURCE-$0}"
  bin=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)
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

daemonmode=$1
shift

hadoop_error "WARNING: Use of this script to ${daemonmode} the MR JobHistory daemon is deprecated."
hadoop_error "WARNING: Attempting to execute replacement \"mapred --daemon ${daemonmode}\" instead."

exec "${HADOOP_MAPRED_HOME}/bin/mapred" \
--config "${HADOOP_CONF_DIR}" --daemon "${daemonmode}" "$@"
