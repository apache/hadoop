#!/usr/bin/env bash
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

## @audience     public
## @stability    stable
function hadoop_usage()
{
  echo "Usage: estimator.sh"
 #hadoop-daemon.sh. need both start and stop, status (query the status). run as background process.
}

## @audience     public
## @stability    stable
function calculate_classpath
{
  hadoop_add_client_opts
  hadoop_add_to_classpath_tools hadoop-resourceestimator
}

## @audience     public
## @stability    stable
function resourceestimatorcmd_case
{
  # shellcheck disable=SC2034
  HADOOP_SUBCMD_SUPPORTDAEMONIZATION="true"
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME='org.apache.hadoop.resourceestimator.service.ResourceEstimatorServer'
}

# let's locate libexec...
if [[ -n "${HADOOP_HOME}" ]]; then
  HADOOP_DEFAULT_LIBEXEC_DIR="${HADOOP_HOME}/libexec"
else
  this="${BASH_SOURCE-$0}"
  bin=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)
  HADOOP_DEFAULT_LIBEXEC_DIR="${bin}/../../../../../libexec"
fi

HADOOP_LIBEXEC_DIR="${HADOOP_LIBEXEC_DIR:-$HADOOP_DEFAULT_LIBEXEC_DIR}"
# shellcheck disable=SC2034
HADOOP_NEW_CONFIG=true
if [[ -f "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]]; then
  # shellcheck source=./hadoop-common-project/hadoop-common/src/main/bin/hadoop-config.sh
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh." 2>&1
  exit 1
fi

# get arguments
HADOOP_SUBCMD=$1
shift

HADOOP_SUBCMD_ARGS=("$@")

resourceestimatorcmd_case "${HADOOP_SUBCMD}" "${HADOOP_SUBCMD_ARGS[@]}"

calculate_classpath
hadoop_generic_java_subcmd_handler
