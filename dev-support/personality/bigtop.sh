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

# shellcheck disable=SC2034
PATCH_BRANCH_DEFAULT=master
# shellcheck disable=SC2034
JIRA_ISSUE_RE='^BIGTOP-[0-9]+$'
# shellcheck disable=SC2034
HOW_TO_CONTRIBUTE=""
# shellcheck disable=SC2034
BUILDTOOL=gradle
# shellcheck disable=SC2034
GITHUB_REPO="apache/bigtop"
# shellcheck disable=SC2034
BIGTOP_PUPPETSETUP=false

add_plugin bigtop

function bigtop_usage
{
  echo "Bigtop specific:"
  echo "--bigtop-puppet=[false|true]   execute the bigtop puppet setup (needs sudo to root)"
}

function bigtop_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
      --bigtop-puppet=*)
        BIGTOP_PUPPETSETUP=${i#*=}
      ;;
    esac
  done
}

function bigtop_precheck_postinstall
{
  if [[ ${BIGTOP_PUPPETSETUP} = "true" ]]; then
    pushd "${BASEDIR}" >/dev/null
    echo_and_redirect "${PATCH_DIR}/bigtop-branch-toolchain.txt" "${GRADLEW}" toolchain
    popd >/dev/null
  fi
}

function bigtop_postapply_postinstall
{
  if [[ ${BIGTOP_PUPPETSETUP} = "true" ]]; then
    pushd "${BASEDIR}" >/dev/null
    echo_and_redirect "${PATCH_DIR}/bigtop-patch-toolchain.txt" "${GRADLEW}" toolchain
    popd >/dev/null
  fi
}