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

add_plugin rubocop

RUBOCOP_TIMER=0

RUBOCOP=${RUBOCOP:-$(which rubocop 2>/dev/null)}

function rubocop_usage
{
  echo "Rubocop specific:"
  echo "--rubocop=<path> path to rubocop executable"
}

function rubocop_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
    --rubocop=*)
      RUBOCOP=${i#*=}
    ;;
    esac
  done
}

function rubocop_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.rb$ ]]; then
    add_test rubocop
  fi
}

function rubocop_preapply
{
  local i

  verify_needed_test rubocop
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "rubocop plugin: prepatch"

  if [[ ! -x ${RUBOCOP} ]]; then
    yetus_error "${RUBOCOP} does not exist."
    return 0
  fi

  start_clock

  echo "Running rubocop against modified ruby scripts."
  pushd "${BASEDIR}" >/dev/null
  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ \.rb$ && -f ${i} ]]; then
      ${RUBOCOP} -f c "${i}" | ${AWK} '!/[0-9]* files? inspected/' >> "${PATCH_DIR}/branch-rubocop-result.txt"
    fi
  done
  popd >/dev/null
  # keep track of how much as elapsed for us already
  RUBOCOP_TIMER=$(stop_clock)
  return 0
}

function rubocop_postapply
{
  local i
  local numPrepatch
  local numPostpatch
  local diffPostpatch

  verify_needed_test rubocop
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "rubocop plugin: postpatch"

  if [[ ! -x ${RUBOCOP} ]]; then
    yetus_error "${RUBOCOP} is not available."
    add_vote_table 0 rubocop "Rubocop was not available."
    return 0
  fi

  start_clock

  # add our previous elapsed to our new timer
  # by setting the clock back
  offset_clock "${RUBOCOP_TIMER}"

  echo "Running rubocop against modified ruby scripts."
  # we re-check this in case one has been added
  pushd "${BASEDIR}" >/dev/null
  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ \.rb$ && -f ${i} ]]; then
      ${RUBOCOP} -f c "${i}" | ${AWK} '!/[0-9]* files? inspected/' >> "${PATCH_DIR}/patch-rubocop-result.txt"
    fi
  done
  popd >/dev/null

  # shellcheck disable=SC2016
  RUBOCOP_VERSION=$(${RUBOCOP} -v | ${AWK} '{print $NF}')
  add_footer_table rubocop "v${RUBOCOP_VERSION}"

  calcdiffs "${PATCH_DIR}/branch-rubocop-result.txt" "${PATCH_DIR}/patch-rubocop-result.txt" > "${PATCH_DIR}/diff-patch-rubocop.txt"
  diffPostpatch=$(${AWK} -F: 'BEGIN {sum=0} 4<NF {sum+=1} END {print sum}' "${PATCH_DIR}/diff-patch-rubocop.txt")

  if [[ ${diffPostpatch} -gt 0 ]] ; then
    # shellcheck disable=SC2016
    numPrepatch=$(${AWK} -F: 'BEGIN {sum=0} 4<NF {sum+=1} END {print sum}' "${PATCH_DIR}/branch-rubocop-result.txt")

    # shellcheck disable=SC2016
    numPostpatch=$(${AWK} -F: 'BEGIN {sum=0} 4<NF {sum+=1} END {print sum}' "${PATCH_DIR}/patch-rubocop-result.txt")

    add_vote_table -1 rubocop "The applied patch generated "\
      "${diffPostpatch} new rubocop issues (total was ${numPrepatch}, now ${numPostpatch})."
    add_footer_table rubocop "@@BASE@@/diff-patch-rubocop.txt"
    return 1
  fi

  add_vote_table +1 rubocop "There were no new rubocop issues."
  return 0
}
