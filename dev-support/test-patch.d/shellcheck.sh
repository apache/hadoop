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

add_plugin shellcheck

SHELLCHECK_TIMER=0

SHELLCHECK=${SHELLCHECK:-$(which shellcheck 2>/dev/null)}

SHELLCHECK_SPECIFICFILES=""

# if it ends in an explicit .sh, then this is shell code.
# if it doesn't have an extension, we assume it is shell code too
function shellcheck_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.sh$ ]]; then
    add_test shellcheck
    SHELLCHECK_SPECIFICFILES="${SHELLCHECK_SPECIFICFILES} ./${filename}"
  fi

  if [[ ! ${filename} =~ \. ]]; then
    add_test shellcheck
  fi
}

function shellcheck_private_findbash
{
  local i
  local value
  local list

  while read line; do
    value=$(find "${line}" ! -name '*.cmd' -type f \
      | ${GREP} -E -v '(.orig$|.rej$)')
    list="${list} ${value}"
  done < <(find . -type d -name bin -o -type d -name sbin -o -type d -name libexec -o -type d -name shellprofile.d)
  # shellcheck disable=SC2086
  echo ${list} ${SHELLCHECK_SPECIFICFILES} | tr ' ' '\n' | sort -u
}

function shellcheck_preapply
{
  local i

  verify_needed_test shellcheck
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "shellcheck plugin: prepatch"

  if [[ ! -x "${SHELLCHECK}" ]]; then
    hadoop_error "shellcheck is not available."
    return 0
  fi

  start_clock

  # shellcheck disable=SC2016
  SHELLCHECK_VERSION=$(shellcheck --version | ${GREP} version: | ${AWK} '{print $NF}')

  echo "Running shellcheck against all identifiable shell scripts"
  pushd "${BASEDIR}" >/dev/null
  for i in $(shellcheck_private_findbash); do
    if [[ -f ${i} ]]; then
      ${SHELLCHECK} -f gcc "${i}" >> "${PATCH_DIR}/${PATCH_BRANCH}shellcheck-result.txt"
    fi
  done
  popd > /dev/null
  # keep track of how much as elapsed for us already
  SHELLCHECK_TIMER=$(stop_clock)
  return 0
}

function shellcheck_calcdiffs
{
  local orig=$1
  local new=$2
  local diffout=$3
  local tmp=${PATCH_DIR}/sc.$$.${RANDOM}
  local count=0
  local j

  # first, pull out just the errors
  # shellcheck disable=SC2016
  ${AWK} -F: '{print $NF}' "${orig}" >> "${tmp}.branch"

  # shellcheck disable=SC2016
  ${AWK} -F: '{print $NF}' "${new}" >> "${tmp}.patch"

  # compare the errors, generating a string of line
  # numbers.  Sorry portability: GNU diff makes this too easy
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
    count=$(wc -l "${diffout}" | ${AWK} '{print $1}' )
  fi
  rm "${tmp}.branch" "${tmp}.patch" "${tmp}.lined" 2>/dev/null
  echo "${count}"
}

function shellcheck_postapply
{
  local i

  verify_needed_test shellcheck
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "shellcheck plugin: postpatch"

  if [[ ! -x "${SHELLCHECK}" ]]; then
    hadoop_error "shellcheck is not available."
    add_jira_table 0 shellcheck "Shellcheck was not available."
    return 0
  fi

  start_clock

  # add our previous elapsed to our new timer
  # by setting the clock back
  offset_clock "${SHELLCHECK_TIMER}"

  echo "Running shellcheck against all identifiable shell scripts"
  # we re-check this in case one has been added
  for i in $(shellcheck_private_findbash); do
    ${SHELLCHECK} -f gcc "${i}" >> "${PATCH_DIR}/patchshellcheck-result.txt"
  done

  # shellcheck disable=SC2016
  numPrepatch=$(wc -l "${PATCH_DIR}/${PATCH_BRANCH}shellcheck-result.txt" | ${AWK} '{print $1}')
  # shellcheck disable=SC2016
  numPostpatch=$(wc -l "${PATCH_DIR}/patchshellcheck-result.txt" | ${AWK} '{print $1}')

  diffPostpatch=$(shellcheck_calcdiffs \
    "${PATCH_DIR}/${PATCH_BRANCH}shellcheck-result.txt" \
    "${PATCH_DIR}/patchshellcheck-result.txt" \
      "${PATCH_DIR}/diffpatchshellcheck.txt"
    )

  if [[ ${diffPostpatch} -gt 0 ]] ; then
    add_jira_table -1 shellcheck "The applied patch generated "\
      "${diffPostpatch} new shellcheck (v${SHELLCHECK_VERSION}) issues (total was ${numPrepatch}, now ${numPostpatch})."
    add_jira_footer shellcheck "@@BASE@@/diffpatchshellcheck.txt"
    return 1
  fi

  add_jira_table +1 shellcheck "There were no new shellcheck (v${SHELLCHECK_VERSION}) issues."
  return 0
}
