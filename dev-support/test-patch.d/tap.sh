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

add_test_format tap

TAP_FAILED_TESTS=""
TAP_LOG_DIR="target/tap"

function tap_process_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --tap-log-dir=*)
        TAP_LOG_DIR=${i#=*}
      ;;
    esac
  done
}

function tap_usage
{
  echo "TAP Options:"
  echo "--tap-log-dir=<dir>    Directory relative to the module for tap output (default: \"target/tap\")"
}

function tap_process_tests
{
  # shellcheck disable=SC2034
  declare module=$1
  # shellcheck disable=SC2034
  declare buildlogfile=$2
  declare filefrag=$3
  declare result=0
  declare module_failed_tests
  declare filenames

  filenames=$(find "${TAP_LOG_DIR}" -type f -exec "${GREP}" -l -E "^not ok" {} \;)

  if [[ -n "${filenames}" ]]; then
    module_failed_tests=$(echo "${filenames}" \
      | ${SED} -e "s,${TAP_LOG_DIR},,g" -e s,^/,,g )
    # shellcheck disable=SC2086
    cat ${filenames} >> "${PATCH_DIR}/patch-${filefrag}.tap"
    TAP_LOGS="${TAP_LOGS} @@BASE@@/patch-${filefrag}.tap"
    TAP_FAILED_TESTS="${TAP_FAILED_TESTS} ${module_failed_tests}"
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

function tap_finalize_results
{
  declare jdk=$1

  if [[ -n "${TAP_FAILED_TESTS}" ]] ; then
    # shellcheck disable=SC2086
    populate_test_table "${jdk}Failed TAP tests" ${TAP_FAILED_TESTS}
    TAP_FAILED_TESTS=""
    add_footer_table "TAP logs" "${TAP_LOGS}"
  fi
}
