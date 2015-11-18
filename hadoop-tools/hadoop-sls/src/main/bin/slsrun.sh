#!/bin/bash
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

###############################################################################
printUsage() {
  echo "Usage: slsrun.sh <OPTIONS>"
  echo "                 --input-rumen|--input-sls=<FILE1,FILE2,...>"
  echo "                 --output-dir=<SLS_SIMULATION_OUTPUT_DIRECTORY>"
  echo "                 [--nodes=<SLS_NODES_FILE>]"
  echo "                 [--track-jobs=<JOBID1,JOBID2,...>]"
  echo "                 [--print-simulation]"
  echo                  
}
###############################################################################
parseArgs() {
  for i in $*
  do
    case $i in
    --input-rumen=*)
      inputrumen=${i#*=}
      ;;
    --input-sls=*)
      inputsls=${i#*=}
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
      echo "Invalid option"
      echo
      printUsage
      exit 1
      ;;
    esac
  done

  if [[ "${inputrumen}" == "" && "${inputsls}" == "" ]] ; then
    echo "Either --input-rumen or --input-sls must be specified"
    echo
    printUsage
    exit 1
  fi

  if [[ "${outputdir}" == "" ]] ; then
    echo "The output directory --output-dir must be specified"
    echo
    printUsage
    exit 1
  fi
}

###############################################################################
calculateClasspath() {
  HADOOP_BASE=`which hadoop`
  HADOOP_BASE=`dirname $HADOOP_BASE`
  DEFAULT_LIBEXEC_DIR=${HADOOP_BASE}/../libexec
  HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $HADOOP_LIBEXEC_DIR/hadoop-config.sh
  SLS_HTML="${HADOOP_PREFIX}/share/hadoop/tools/sls/html"
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${TOOL_PATH}:${SLS_HTML}"
}
###############################################################################
runSimulation() {
  if [[ "${inputsls}" == "" ]] ; then
    args="-inputrumen ${inputrumen}"
  else
    args="-inputsls ${inputsls}"
  fi

  args="${args} -output ${outputdir}"

  if [[ "${nodes}" != "" ]] ; then
    args="${args} -nodes ${nodes}"
  fi
  
  if [[ "${trackjobs}" != "" ]] ; then
    args="${args} -trackjobs ${trackjobs}"
  fi
  
  if [[ "${printsimulation}" == "true" ]] ; then
    args="${args} -printsimulation"
  fi

  hadoop org.apache.hadoop.yarn.sls.SLSRunner ${args}
}
###############################################################################

calculateClasspath
parseArgs "$@"
runSimulation

exit 0
