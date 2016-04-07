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

function hadoop_usage()
{
  echo "Usage: rumen2sls.sh <OPTIONS>"
  echo "                 --rumen-file=<RUMEN_FILE>"
  echo "                 --output-dir=<SLS_OUTPUT_DIR>"
  echo "                 [--output-prefix=<PREFIX>] (default is sls)"
  echo
}

function parse_args()
{
  for i in "$@"; do
    case $i in
      --rumen-file=*)
        rumenfile=${i#*=}
      ;;
      --output-dir=*)
        outputdir=${i#*=}
      ;;
      --output-prefix=*)
        outputprefix=${i#*=}
      ;;
      *)
        hadoop_error "ERROR: Invalid option ${i}"
        hadoop_exit_with_usage 1
      ;;
    esac
  done

  if [[ -z "${rumenfile}" ]] ; then
    hadoop_error "ERROR: --rumen-file must be specified."
    hadoop_exit_with_usage 1
  fi

  if [[ -z "${outputdir}" ]] ; then
    hadoop_error "ERROR: --output-dir must be specified."
    hadoop_exit_with_usage 1
  fi
}

function calculate_classpath()
{
  hadoop_add_to_classpath_tools hadoop-rumen
}

function run_sls_generator()
{
  if [[ -z "${outputprefix}" ]] ; then
    outputprefix="sls"
  fi

  hadoop_add_param args -input "-input ${rumenfile}"
  hadoop_add_param args -outputJobs "-outputJobs ${outputdir}/${outputprefix}-jobs.json"
  hadoop_add_param args -outputNodes "-outputNodes ${outputdir}/${outputprefix}-nodes.json"

  hadoop_debug "Appending HADOOP_CLIENT_OPTS onto HADOOP_OPTS"
  HADOOP_OPTS="${HADOOP_OPTS} ${HADOOP_CLIENT_OPTS}"

  hadoop_finalize
  # shellcheck disable=SC2086
  hadoop_java_exec rumen2sls org.apache.hadoop.yarn.sls.RumenToSLSConverter ${args}
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
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh." 2>&1
  exit 1
fi

if [ $# = 0 ]; then
  hadoop_exit_with_usage 1
fi

parse_args "${@}"
calculate_classpath
run_sls_generator

