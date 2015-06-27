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
ISSUE_RE='^FLINK-[0-9]+$'
#shellcheck disable=SC2034
HOW_TO_CONTRIBUTE=""

add_plugin flinklib

function fliblib_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$
    || ${filename} =~ \.scala$
    || ${filename} =~ pom.xml$ ]]; then
    add_test flinklib
  fi
}

function flinklib_count
{
  find "${BASEDIR}" \
    | ${GREP} "/lib/" \
    | ${GREP} -v "_qa_workdir" \
    | wc -l
}

function flinklib_preapply
{
  start_clock
  big_console_header "${PATCH_BRANCH} flink library dependencies"

  verify_needed_test flinklib
  if [[ $? == 0 ]]; then
    echo "Patch does not need flinklib testing."
    return 0
  fi

  pushd "${BASEDIR}" >/dev/null
  echo_and_redirect "${PATCH_DIR}/branch-flinklib-root.txt" \
     "${MVN}" "${MAVEN_ARGS[@]}" package -DskipTests -Dmaven.javadoc.skip=true -Ptest-patch
  if [[ $? != 0 ]]; then
     add_vote_table -1 flinklib "Unable to determine flink libs in ${PATCH_BRANCH}."
  fi
  FLINK_PRE_LIB_FILES=$(flinklib_count)
  popd >/dev/null
}

function flinklib_postapply
{
  start_clock
  big_console_header "Patch flink library dependencies"

  verify_needed_test flinklib
  if [[ $? == 0 ]]; then
    echo "Patch does not need flinklib testing."
    return 0
  fi

  pushd "${BASEDIR}" >/dev/null
  echo_and_redirect "${PATCH_DIR}/patch-flinklib-root.txt" \
     "${MVN}" "${MAVEN_ARGS[@]}" package -DskipTests -Dmaven.javadoc.skip=true -Ptest-patch
  FLINK_POST_LIB_FILES=$(flinklib_count)
  popd >/dev/null


  if [[ "${FLINK_POST_LIB_FILES}" -gt "${FLINK_PRE_LIB_FILES}" ]]; then
    add_vote_table -1 flinklib "Patch increases lib folder dependencies from " \
      "${FLINK_PRE_LIB_FILES} to ${FLINK_POST_LIB_FILES}"
    return 1
  elif [[ "${FLINK_POST_LIB_FILES}" -eq "${FLINK_PRE_LIB_FILES}" ]]; then
    add_vote_table 0 flinklib "Patch did not change lib dependencies" \
      " (still ${FLINK_PRE_LIB_FILES})"
  else
    add_vote_table +1 flinklib "Patch decreases lib folder dependencies by " \
      "$((FLINK_PRE_LIB_FILES-FLINK_POST_LIB_FILES))."
  fi
  return 0
}

function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  case ${testtype} in
    mvninstall)
      if [[ ${repostatus} == branch ]]; then
        personality_enqueue_module . -DskipTests
        return
      fi
      return
      ;;
    releaseaudit)
      # this is very fast and provides the full path if we do it from
      # the root of the source
      personality_enqueue_module .
      return
    ;;
    unit)
      if [[ ${TEST_PARALLEL} == "true" ]] ; then
        extra="-Pparallel-tests"
        if [[ -n ${TEST_THREADS:-} ]]; then
          extra="${extra} -DtestsThreadCount=${TEST_THREADS}"
        fi
      fi
    ;;
    *)
      extra="-DskipTests"
    ;;
  esac

  for module in ${CHANGED_MODULES}; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}

function personality_file_tests
{
  local filename=$1

  yetus_debug "Using personality_file_tests, but calling the built-in:"
  builtin_personality_file_tests "${1}"

  if [[ ${filename} =~ \.scala$ ]]; then
    add_test unit
  fi
}
