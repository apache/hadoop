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
  echo "Usage: rumen2sls.sh <OPTIONS>"
  echo "                 --rumen-file=<RUMEN_FILE>"
  echo "                 --output-dir=<SLS_OUTPUT_DIR>"
  echo "                 [--output-prefix=<PREFIX>] (default is sls)"
  echo
}
###############################################################################
parseArgs() {
  for i in $*
  do
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
      echo "Invalid option"
      echo
      printUsage
      exit 1
      ;;
    esac
  done
  if [[ "${rumenfile}" == "" || "${outputdir}" == "" ]] ; then
    echo "Both --rumen-file ${rumenfile} and --output-dir \
          ${outputfdir} must be specified"
    echo
    printUsage
    exit 1
  fi
}
###############################################################################
calculateBasedir() {
  # resolve links - $0 may be a softlink
  PRG="${1}"

  while [ -h "${PRG}" ]; do
    ls=`ls -ld "${PRG}"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
      PRG="$link"
    else
      PRG=`dirname "${PRG}"`/"$link"
    fi
  done

  BASEDIR=`dirname ${PRG}`
  BASEDIR=`cd ${BASEDIR}/..;pwd`
}
###############################################################################
calculateClasspath() {
  HADOOP_BASE=`which hadoop`
  HADOOP_BASE=`dirname $HADOOP_BASE`
  DEFAULT_LIBEXEC_DIR=${HADOOP_BASE}/../libexec
  HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $HADOOP_LIBEXEC_DIR/hadoop-config.sh
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${TOOL_PATH}"
}
###############################################################################
runSLSGenerator() {
  if [[ "${outputprefix}" == "" ]] ; then
    outputprefix="sls"
  fi

  slsJobs=${outputdir}/${outputprefix}-jobs.json
  slsNodes=${outputdir}/${outputprefix}-nodes.json

  args="-input ${rumenfile} -outputJobs ${slsJobs}";
  args="${args} -outputNodes ${slsNodes}";

  hadoop org.apache.hadoop.yarn.sls.RumenToSLSConverter ${args}
}
###############################################################################

calculateBasedir $0
calculateClasspath
parseArgs "$@"
runSLSGenerator

echo
echo "SLS simulation files available at: ${outputdir}"
echo

exit 0