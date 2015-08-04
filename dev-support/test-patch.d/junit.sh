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

add_test_format junit

JUNIT_TEST_TIMEOUTS=""
JUNIT_FAILED_TESTS=""

function junit_process_tests
{
  # shellcheck disable=SC2034
  declare module=$1
  declare buildlogfile=$2
  declare result=0
  declare module_test_timeouts
  declare module_failed_tests

  # shellcheck disable=SC2016
  module_test_timeouts=$(${AWK} '/^Running / { array[$NF] = 1 } /^Tests run: .* in / { delete array[$NF] } END { for (x in array) { print x } }' "${buildlogfile}")
  if [[ -n "${module_test_timeouts}" ]] ; then
    JUNIT_TEST_TIMEOUTS="${JUNIT_TEST_TIMEOUTS} ${module_test_timeouts}"
    ((result=result+1))
  fi

  #shellcheck disable=SC2026,SC2038,SC2016
  module_failed_tests=$(find . -name 'TEST*.xml'\
    | xargs "${GREP}" -l -E "<failure|<error"\
    | ${AWK} -F/ '{sub("TEST-org.apache.",""); sub(".xml",""); print $NF}')

  if [[ -n "${module_failed_tests}" ]] ; then
    JUNIT_FAILED_TESTS="${JUNIT_FAILED_TESTS} ${module_failed_tests}"
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

function junit_finalize_results
{
  declare jdk=$1

  if [[ -n "${JUNIT_FAILED_TESTS}" ]] ; then
    # shellcheck disable=SC2086
    populate_test_table "${jdk}Failed junit tests" ${JUNIT_FAILED_TESTS}
    JUNIT_FAILED_TESTS=""
  fi
  if [[ -n "${JUNIT_TEST_TIMEOUTS}" ]] ; then
    # shellcheck disable=SC2086
    populate_test_table "${jdk}Timed out junit tests" ${JUNIT_TEST_TIMEOUTS}
    JUNIT_TEST_TIMEOUTS=""
  fi
}
