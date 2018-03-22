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
  echo "Usage: slsrun.sh <OPTIONS> "
  echo "                 --tracetype=<SYNTH | SLS | RUMEN>"
  echo "                 --tracelocation=<FILE1,FILE2,...>"
  echo "                 (deprecated --input-rumen=<FILE1,FILE2,...>  | --input-sls=<FILE1,FILE2,...>)"
  echo "                 --output-dir=<SLS_SIMULATION_OUTPUT_DIRECTORY>"
  echo "                 [--nodes=<SLS_NODES_FILE>]"
  echo "                 [--track-jobs=<JOBID1,JOBID2,...>]"
  echo "                 [--print-simulation]"
}

function parse_args()
{
  for i in "$@"; do
    case $i in
      --input-rumen=*)
        inputrumen=${i#*=}
      ;;
      --input-sls=*)
        inputsls=${i#*=}
      ;;
      --tracetype=*)
        tracetype=${i#*=}
      ;;
      --tracelocation=*)
        tracelocation=${i#*=}
      ;;
      --output-dir=*)
        outputdir=${i#*=}
      ;;
      --nodes=*)
        nodes=${i#*=}
      ;;
      --track-jobs=*)
        trackjobs=${i#*=}
      ;;
      --print-simulation)
        printsimulation="true"
      ;;
      *)
        hadoop_error "ERROR: Invalid option ${i}"
        hadoop_exit_with_usage 1
      ;;
    esac
  done

  if [[ -z "${inputrumen}" && -z "${inputsls}" && -z "${tracetype}" ]] ; then
    hadoop_error "ERROR: Either --input-rumen, --input-sls, or --tracetype (with --tracelocation) must be specified."
  fi

  if [[ -n "${inputrumen}" && -n "${inputsls}" && -n "${tracetype}" ]] ; then
    hadoop_error "ERROR: Only specify one of --input-rumen, --input-sls, or --tracetype (with --tracelocation)"
  fi

  if [[ -z "${outputdir}" ]] ; then
    hadoop_error "ERROR: The output directory --output-dir must be specified."
    hadoop_exit_with_usage 1
  fi
}

function calculate_classpath
{
  hadoop_add_to_classpath_tools hadoop-sls
}

function run_simulation() {

  local args

   if [[ "${inputsls}" != "" ]] ; then
        hadoop_add_param args -inputsls "-inputsls ${inputsls}"
   fi
   if [[ "${inputrumen}" != "" ]] ; then
        hadoop_add_param args -inputrumen "-inputrumen ${inputrumen}"
   fi
   if [[ "${tracetype}" != "" ]] ; then
        hadoop_add_param args -tracetype "-tracetype ${tracetype}"
        hadoop_add_param args -tracelocation "-tracelocation ${tracelocation}"
   fi

  hadoop_add_param args -output "-output ${outputdir}"

  if [[ -n "${nodes}" ]] ; then
    hadoop_add_param args -nodes "-nodes ${nodes}"
  fi

  if [[ -n "${trackjobs}" ]] ; then
    hadoop_add_param args -trackjobs "-trackjobs ${trackjobs}"
  fi

  if [[ "${printsimulation}" == "true" ]] ; then
    hadoop_add_param args -printsimulation "-printsimulation"
  fi

  hadoop_add_client_opts

  hadoop_finalize
  # shellcheck disable=SC2086
  hadoop_java_exec sls org.apache.hadoop.yarn.sls.SLSRunner ${args}
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
  # shellcheck disable=SC1090
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh." 2>&1
  exit 1
fi

if [[ $# = 0 ]]; then
  hadoop_exit_with_usage 1
fi

parse_args "${@}"
calculate_classpath
run_simulation
