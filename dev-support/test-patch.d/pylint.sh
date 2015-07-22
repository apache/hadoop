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

function pylint_usage
{
  echo "Pylint specific:"
  echo "--pylint=<path> path to pylint executable"
}

function pylint_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
    --pylint=*)
      PYLINT=${i#*=}
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
      ${PYLINT} --indent-string="  " --output-format=parseable --reports=n "${i}" 2>/dev/null |
      ${AWK} '1<NR' >> "${PATCH_DIR}/branchpylint-result.txt"
    fi
  done
  popd >/dev/null
  # keep track of how much as elapsed for us already
  PYLINT_TIMER=$(stop_clock)
  return 0
}

function pylint_calcdiffs
{
  local orig=$1
  local new=$2
  local diffout=$3
  local tmp=${PATCH_DIR}/pl.$$.${RANDOM}
  local count=0
  local j

  # first, pull out just the errors
  # shellcheck disable=SC2016
  ${AWK} -F: '{print $NF}' "${orig}" >> "${tmp}.branch"

  # shellcheck disable=SC2016
  ${AWK} -F: '{print $NF}' "${new}" >> "${tmp}.patch"

  # compare the errors, generating a string of line
  # numbers. Sorry portability: GNU diff makes this too easy
  ${DIFF} --unchanged-line-format="" \
     --old-line-format="" \
     --new-line-format="%dn " \
     "${tmp}.branch" \
     "${tmp}.patch" > "${tmp}.lined"

  # now, pull out those lines of the raw output
  # shellcheck disable=SC2013
  for j in $(cat "${tmp}.lined"); do
    # shellcheck disable=SC2086
    head -${j} "${new}" | tail -1 >> "${diffout}"
  done

  if [[ -f "${diffout}" ]]; then
    # shellcheck disable=SC2016
    count=$(${AWK} -F: 'BEGIN {sum=0} 2<NF {sum+=1} END {print sum}' "${diffout}")
  fi
  rm "${tmp}.branch" "${tmp}.patch" "${tmp}.lined" 2>/dev/null
  echo "${count}"
}

function pylint_postapply
{
  local i
  local msg
  local numPrepatch
  local numPostpatch
  local diffPostpatch

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
      ${PYLINT} --indent-string="  " --output-format=parseable --reports=n "${i}" 2>/dev/null |
      ${AWK} '1<NR' >> "${PATCH_DIR}/patchpylint-result.txt"
    fi
  done
  popd >/dev/null

  # shellcheck disable=SC2016
  PYLINT_VERSION=$(${PYLINT} --version 2>/dev/null | ${GREP} pylint | ${AWK} '{print $NF}')
  PYLINT_VERSION=${PYLINT_VERSION%,}
  msg="v${PYLINT_VERSION}"
  add_footer_table pylint "${msg}"

  diffPostpatch=$(pylint_calcdiffs \
    "${PATCH_DIR}/branchpylint-result.txt" \
    "${PATCH_DIR}/patchpylint-result.txt" \
    "${PATCH_DIR}/diffpatchpylint.txt")

  if [[ ${diffPostpatch} -gt 0 ]] ; then
    # shellcheck disable=SC2016
    numPrepatch=$(${AWK} -F: 'BEGIN {sum=0} 2<NF {sum+=1} END {print sum}' "${PATCH_DIR}/branchpylint-result.txt")

    # shellcheck disable=SC2016
    numPostpatch=$(${AWK} -F: 'BEGIN {sum=0} 2<NF {sum+=1} END {print sum}' "${PATCH_DIR}/patchpylint-result.txt")

    add_vote_table -1 pylint "The applied patch generated "\
      "${diffPostpatch} new pylint (v${PYLINT_VERSION}) issues (total was ${numPrepatch}, now ${numPostpatch})."
    add_footer_table pylint "@@BASE@@/diffpatchpylint.txt"
    return 1
  fi

  add_vote_table +1 pylint "There were no new pylint issues."
  return 0
}
