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

#shellcheck disable=SC2034
PATCH_BRANCH_DEFAULT=master
#shellcheck disable=SC2034
ISSUE_RE='^HBASE-[0-9]+$'
#shellcheck disable=SC2034
HOW_TO_CONTRIBUTE=""

# All supported Hadoop versions that we want to test the compilation with
HADOOP2_VERSIONS="2.4.1 2.5.2 2.6.0"

# Override the maven options
MAVEN_OPTS="${MAVEN_OPTS:-"-Xmx3100M"}"

function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  case ${testtype} in
    javac)
      personality_enqueue_module . -DskipTests
      return
      ;;
    mvninstall)
      extra="-DskipTests -DHBasePatchProcess"
      if [[ ${repostatus} == branch ]]; then
        personality_enqueue_module . "${extra}"
        return
      fi
      return
      ;;
    asflicense)
      # this is very fast and provides the full path if we do it from
      # the root of the source
      personality_enqueue_module . -DHBasePatchProcess
      return
    ;;
    unit)
      if [[ ${TEST_PARALLEL} == "true" ]] ; then
        extra="-Pparallel-tests -DHBasePatchProcess"
        if [[ -n ${TEST_THREADS:-} ]]; then
          extra="${extra} -DtestsThreadCount=${TEST_THREADS}"
        fi
      fi
    ;;
    *)
      extra="-DskipTests -DHBasePatchProcess"
    ;;
  esac

  for module in ${CHANGED_MODULES}; do

    # skip findbugs on hbase-shell
    if [[ ${module} == hbase-shell
      && ${testtype} == findbugs ]]; then
      continue
    else
      # shellcheck disable=SC2086
      personality_enqueue_module ${module} ${extra}
    fi
  done
}

###################################################

add_plugin hadoopcheck

function hadoopcheck_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hadoopcheck
  fi
}

function hadoopcheck_postapply
{
  local HADOOP2_VERSION
  local logfile
  local count
  local result=0

  big_console_header "Compiling against various Hadoop versions"

  export MAVEN_OPTS="${MAVEN_OPTS}"
  for HADOOP2_VERSION in ${HADOOP2_VERSIONS}; do
    logfile="${PATCH_DIR}/patch-javac-${HADOOP2_VERSION}.txt"
    echo_and_redirect "${logfile}" \
      "${MVN}" clean install \
        -DskipTests -DHBasePatchProcess \
        -Dhadoop-two.version="${HADOOP2_VERSION}"
    count=$(${GREP} -c ERROR "${logfile}")
    if [[ ${count} -gt 0 ]]; then
      add_vote_table -1 hadoopcheck "Patch causes ${count} errors with Hadoop v${HADOOP2_VERSION}."
      ((result=result+1))
    fi
  done

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  add_vote_table +1 hadoopcheck "Patch does not cause any errors with Hadoop ${HADOOP2_VERSIONS}."
  return 0
}

######################################

add_plugin hbaseprotoc

function hbaseprotoc_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.proto$ ]]; then
    add_test hbaseprotoc
  fi
}

function hbaseprotoc_postapply
{
  local i=0
  local fn
  local module
  local logfile
  local count
  local result

  big_console_header "Patch HBase protoc plugin"

  start_clock

  verify_needed_test hbaseprotoc
  if [[ $? == 0 ]]; then
    echo "Patch does not need hbaseprotoc testing."
    return 0
  fi

  personality_modules patch hbaseprotoc
  modules_workers patch hbaseprotoc compile -DskipTests -Pcompile-protobuf -X -DHBasePatchProcess

  # shellcheck disable=SC2153
  until [[ $i -eq ${#MODULE[@]} ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    module=${MODULE[$i]}
    fn=$(module_file_fragment "${module}")
    logfile="${PATCH_DIR}/patch-hbaseprotoc-${fn}.txt"

    count=$(${GREP} -c ERROR "${logfile}")

    if [[ ${count} -gt 0 ]]; then
      module_status ${i} -1 "patch-hbaseprotoc-${fn}.txt" "Patch generated "\
        "${count} new protoc errors in ${module}."
      ((result=result+1))
    fi
    ((i=i+1))
  done

  modules_messages patch hbaseprotoc true
  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

######################################

add_plugin hbaseanti

function hbaseanti_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hbaseanti
  fi
}

function hbaseanti_preapply
{
  local warnings
  local result

  big_console_header "Checking for known anti-patterns"

  start_clock

  verify_needed_test hbaseanti
  if [[ $? == 0 ]]; then
    echo "Patch does not need hbaseanti testing."
    return 0
  fi

  warnings=$(${GREP} 'new TreeMap<byte.*()' "${PATCH_DIR}/patch")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears to have anti-pattern where BYTES_COMPARATOR was omitted: ${warnings}."
    ((result=result+1))
  fi

  warnings=$(${GREP} 'import org.apache.hadoop.classification' "${PATCH_DIR}/patch")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears use Hadoop classification instead of HBase: ${warnings}."
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  add_vote_table +1 hbaseanti "" "Patch does not have any anti-patterns."
  return 0
}
