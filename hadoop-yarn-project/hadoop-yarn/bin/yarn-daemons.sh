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
  echo "Usage: yarn-daemons.sh [--config confdir] [--hosts hostlistfile] (start|stop|status) <yarn-command> <args...>"
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
if [[ -f "${HADOOP_LIBEXEC_DIR}/yarn-config.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/yarn-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/yarn-config.sh." 2>&1
  exit 1
fi

yarnscript="${HADOOP_YARN_HOME}/bin/yarn"

daemonmode=$1
shift

hadoop_error "WARNING: Use of this script to ${daemonmode} YARN daemons is deprecated."
hadoop_error "WARNING: Attempting to execute replacement \"yarn --workers --daemon ${daemonmode}\" instead."

#
# Original input was usually:
#  yarn-daemons.sh (shell options) (start|stop) nodemanager (daemon options)
# we're going to turn this into
#  yarn --workers --daemon (start|stop) (rest of options)
#
for (( i = 0; i < ${#HADOOP_USER_PARAMS[@]}; i++ ))
do
  if [[ "${HADOOP_USER_PARAMS[$i]}" =~ ^start$ ]] ||
     [[ "${HADOOP_USER_PARAMS[$i]}" =~ ^stop$ ]] ||
     [[ "${HADOOP_USER_PARAMS[$i]}" =~ ^status$ ]]; then
    unset HADOOP_USER_PARAMS[$i]
  fi
done

${yarnscript} --workers --daemon "${daemonmode}" "${HADOOP_USER_PARAMS[@]}"

