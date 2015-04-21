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

add_plugin whitespace

function whitespace_preapply
{
  local count

  big_console_header "Checking for whitespace at the end of lines"
  start_clock

  ${GREP} '^+' "${PATCH_DIR}/patch" | ${GREP} '[[:blank:]]$' > "${PATCH_DIR}/whitespace.txt"

  # shellcheck disable=SC2016
  count=$(wc -l "${PATCH_DIR}/whitespace.txt" | ${AWK} '{print $1}')

  if [[ ${count} -gt 0 ]]; then
    add_jira_table -1 whitespace "The patch has ${count}"\
      " line(s) that end in whitespace."
    add_jira_footer whitespace "@@BASE@@/whitespace.txt"
    return 1
  fi

  add_jira_table +1 whitespace "The patch has no lines that end in whitespace."
  return 0
}
