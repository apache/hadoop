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
ISSUE_RE='^TAJO-[0-9]+$'
#shellcheck disable=SC2034
HOW_TO_CONTRIBUTE="https://cwiki.apache.org/confluence/display/TAJO/How+to+Contribute+to+Tajo"

function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  case ${testtype} in
    mvninstall)
      extra="-DskipTests"
      if [[ ${repostatus} == branch ]]; then
        personality_enqueue_module . "${extra}"
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
