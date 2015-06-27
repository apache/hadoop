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

function whitespace_postapply
{
  local count
  local result=0

  big_console_header "Checking for whitespace at the end of lines"
  start_clock

  pushd "${BASEDIR}" >/dev/null
  # shellcheck disable=SC2016
  ${AWK} '/\t/ {print $0}' \
      "${GITDIFFCONTENT}" \
    | ${GREP} -v Makefile: >> "${PATCH_DIR}/whitespace-tabs.txt"

   ${GREP} -E '[[:blank:]]$' \
     "${GITDIFFCONTENT}" \
      >> "${PATCH_DIR}/whitespace-eol.txt"

  # shellcheck disable=SC2016
  count=$(wc -l "${PATCH_DIR}/whitespace-eol.txt" | ${AWK} '{print $1}')

  if [[ ${count} -gt 0 ]]; then
    add_vote_table -1 whitespace "The patch has ${count}"\
      " line(s) that end in whitespace. Use git apply --whitespace=fix."
    add_footer_table whitespace "@@BASE@@/whitespace-eol.txt"
    ((result=result+1))
  fi

  # shellcheck disable=SC2016
  count=$(wc -l "${PATCH_DIR}/whitespace-tabs.txt" | ${AWK} '{print $1}')

  if [[ ${count} -gt 0 ]]; then
    add_vote_table -1 whitespace "The patch has ${count}"\
      " line(s) with tabs."
    add_footer_table whitespace "@@BASE@@/whitespace-tabs.txt"
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    popd >/dev/null
    return 1
  fi

  popd >/dev/null
  add_vote_table +1 whitespace "Patch has no whitespace issues."
  return 0
}
