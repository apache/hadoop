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


# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   HADOOP_SLAVES    File naming remote hosts.
#     Default is ${HADOOP_CONF_DIR}/slaves.
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_PREFIX}/conf.
#   HADOOP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HADOOP_SSH_OPTS Options passed to ssh when running remote commands.
##

function hadoop_usage {
  echo "Usage: slaves.sh [--config confdir] command..."
}

# let's locate libexec...
if [[ -n "${HADOOP_PREFIX}" ]]; then
  DEFAULT_LIBEXEC_DIR="${HADOOP_PREFIX}/libexec"
else
  this="${BASH_SOURCE-$0}"
  bin=$(cd -P -- "$(dirname -- "${this}")" >dev/null && pwd -P)
  DEFAULT_LIBEXEC_DIR="${bin}/../libexec"
fi

HADOOP_LIBEXEC_DIR="${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}"
# shellcheck disable=SC2034
HADOOP_NEW_CONFIG=true
if [[ -f "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh." 2>&1
  exit 1
fi

# if no args specified, show usage
if [[ $# -le 0 ]]; then
  hadoop_exit_with_usage 1
fi

hadoop_connect_to_hosts "$@"

