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

add_plugin pylint

PYLINT_TIMER=0

PYLINT=${PYLINT:-$(which pylint 2>/dev/null)}
PYLINT_OPTIONS=${PYLINT_OPTIONS:-}

function pylint_usage
{
  echo "Pylint specific:"
  echo "--pylint=<path>         path to pylint executable"
  echo "--pylint-options=<path> pylint options other than output-format and reports"
}

function pylint_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
    --pylint=*)
      PYLINT=${i#*=}
    ;;
    --pylint-options=*)
      PYLINT_OPTIONS=${i#*=}
    ;;
    esac
  done
}

function pylint_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.py$ ]]; then
    add_test pylint
  fi
}

function pylint_preapply
{
  local i
  local count
  local tmp=${PATCH_DIR}/pylint.$$.${RANDOM}

  verify_needed_test pylint
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "pylint plugin: prepatch"

  if [[ ! -x ${PYLINT} ]]; then
    yetus_error "${PYLINT} does not exist."
    return 0
  fi

  start_clock

  echo "Running pylint against modified python scripts."
  pushd "${BASEDIR}" >/dev/null
  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ \.py$ && -f ${i} ]]; then
      # shellcheck disable=SC2086
      eval "${PYLINT} ${PYLINT_OPTIONS} --output-format=parseable --reports=n ${i}" 2>${tmp} |
      ${AWK} '1<NR' >> "${PATCH_DIR}/branch-pylint-result.txt"
    fi
    # shellcheck disable=SC2016
    count=$(${GREP} -v "^No config file found" "${tmp}" | wc -l | ${AWK} '{print $1}')
    if [[ ${count} -gt 0 ]]; then
      add_footer_table pylint "prepatch stderr: ${tmp}"
      return 1
    fi
  done
  rm "${tmp}" 2>/dev/null
  popd >/dev/null
  # keep track of how much as elapsed for us already
  PYLINT_TIMER=$(stop_clock)
  return 0
}

function pylint_postapply
{
  local i
  local count
  local numPrepatch
  local numPostpatch
  local diffPostpatch
  local tmp=${PATCH_DIR}/pylint.$$.${RANDOM}

  verify_needed_test pylint
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "pylint plugin: postpatch"

  if [[ ! -x ${PYLINT} ]]; then
    yetus_error "${PYLINT} is not available."
    add_vote_table 0 pylint "Pylint was not available."
    return 0
  fi

  start_clock

  # add our previous elapsed to our new timer
  # by setting the clock back
  offset_clock "${PYLINT_TIMER}"

  echo "Running pylint against modified python scripts."
  # we re-check this in case one has been added
  pushd "${BASEDIR}" >/dev/null
  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ \.py$ && -f ${i} ]]; then
      # shellcheck disable=SC2086
      eval "${PYLINT} ${PYLINT_OPTIONS} --output-format=parseable --reports=n ${i}" 2>${tmp} |
      ${AWK} '1<NR' >> "${PATCH_DIR}/patch-pylint-result.txt"
    fi
    # shellcheck disable=SC2016
    count=$(${GREP} -v "^No config file found" "${tmp}" | wc -l | ${AWK} '{print $1}')
    if [[ ${count} -gt 0 ]]; then
      add_vote_table -1 pylint "Something bad seems to have happened in running pylint. Please check pylint stderr files."
      add_footer_table pylint "postpatch stderr: ${tmp}"
      return 1
    fi
  done
  rm "${tmp}" 2>/dev/null
  popd >/dev/null

  # shellcheck disable=SC2016
  PYLINT_VERSION=$(${PYLINT} --version 2>/dev/null | ${GREP} pylint | ${AWK} '{print $NF}')
  add_footer_table pylint "v${PYLINT_VERSION%,}"

  calcdiffs "${PATCH_DIR}/branch-pylint-result.txt" "${PATCH_DIR}/patch-pylint-result.txt" > "${PATCH_DIR}/diff-patch-pylint.txt"
  diffPostpatch=$(${GREP} -c "^.*:.*: \[.*\] " "${PATCH_DIR}/diff-patch-pylint.txt")

  if [[ ${diffPostpatch} -gt 0 ]] ; then
    # shellcheck disable=SC2016
    numPrepatch=$(${GREP} -c "^.*:.*: \[.*\] " "${PATCH_DIR}/branch-pylint-result.txt")

    # shellcheck disable=SC2016
    numPostpatch=$(${GREP} -c "^.*:.*: \[.*\] " "${PATCH_DIR}/patch-pylint-result.txt")

    add_vote_table -1 pylint "The applied patch generated "\
      "${diffPostpatch} new pylint issues (total was ${numPrepatch}, now ${numPostpatch})."
    add_footer_table pylint "@@BASE@@/diff-patch-pylint.txt"
    return 1
  fi

  add_vote_table +1 pylint "There were no new pylint issues."
  return 0
}

function pylint_postcompile
{
  declare repostatus=$1

  if [[ "${repostatus}" = branch ]]; then
    pylint_preapply
  else
    pylint_postapply
  fi
}
