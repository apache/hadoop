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
PATCH_BRANCH_DEFAULT=trunk
#shellcheck disable=SC2034
JIRA_ISSUE_RE='^KAFKA-[0-9]+$'
#shellcheck disable=SC2034
HOW_TO_CONTRIBUTE="http://kafka.apache.org/contributing.html"
# shellcheck disable=SC2034
BUILDTOOL=gradle
#shellcheck disable=SC2034
GITHUB_REPO="apache/kafka"

function personality_modules
{
  declare repostatus=$1
  declare testtype=$2
  declare module
  declare extra=""

  yetus_debug "Using kafka personality_modules"
  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  case ${testtype} in
    gradleboot)
      # kafka's bootstrap is broken
      if [[ ${testtype} == gradleboot ]]; then
        pushd "${BASEDIR}" >/dev/null
        echo_and_redirect "${PATCH_DIR}/kafka-configure-gradle.txt" gradle
        popd >/dev/null
      fi
    ;;
    compile)
      extra="clean jar"
    ;;
  esac

  for module in ${CHANGED_MODULES}; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}
