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

add_plugin checkstyle

CHECKSTYLE_TIMER=0

# if it ends in an explicit .sh, then this is shell code.
# if it doesn't have an extension, we assume it is shell code too
function checkstyle_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test checkstyle
  fi
}

function checkstyle_mvnrunner
{
  local logfile=$1
  local output=$2
  local tmp=${PATCH_DIR}/$$.${RANDOM}
  local j

  "${MVN}" clean test checkstyle:checkstyle -DskipTests \
    -Dcheckstyle.consoleOutput=true \
    "-D${PROJECT_NAME}PatchProcess" 2>&1 \
      | tee "${logfile}" \
      | ${GREP} ^/ \
      | ${SED} -e "s,${BASEDIR},.,g" \
          > "${tmp}"

  # the checkstyle output files are massive, so
  # let's reduce the work by filtering out files
  # that weren't changed.  Some modules are
  # MASSIVE and this can cut the output down to
  # by orders of magnitude!!
  for j in ${CHANGED_FILES}; do
    ${GREP} "${j}" "${tmp}" >> "${output}"
  done

  rm "${tmp}" 2>/dev/null
}

function checkstyle_preapply
{
  local module_suffix
  local modules=${CHANGED_MODULES}
  local module

  verify_needed_test checkstyle

  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "checkstyle plugin: prepatch"

  start_clock

  for module in ${modules}
  do
    pushd "${module}" >/dev/null
    echo "  Running checkstyle in ${module}"
    module_suffix=$(basename "${module}")

    checkstyle_mvnrunner \
      "${PATCH_DIR}/maven-${PATCH_BRANCH}checkstyle-${module_suffix}.txt" \
      "${PATCH_DIR}/${PATCH_BRANCH}checkstyle${module_suffix}.txt"

    if [[ $? != 0 ]] ; then
      echo "Pre-patch ${PATCH_BRANCH} checkstyle compilation is broken?"
      add_jira_table -1 checkstyle "Pre-patch ${PATCH_BRANCH} ${module} checkstyle compilation may be broken."
    fi
    popd >/dev/null
  done

  # keep track of how much as elapsed for us already
  CHECKSTYLE_TIMER=$(stop_clock)
  return 0
}

function checkstyle_calcdiffs
{
  local orig=$1
  local new=$2
  local diffout=$3
  local tmp=${PATCH_DIR}/cs.$$.${RANDOM}
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

function checkstyle_postapply
{
  local rc=0
  local module
  local modules=${CHANGED_MODULES}
  local module_suffix
  local numprepatch=0
  local numpostpatch=0
  local diffpostpatch=0

  verify_needed_test checkstyle

  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "checkstyle plugin: postpatch"

  start_clock

  # add our previous elapsed to our new timer
  # by setting the clock back
  offset_clock "${CHECKSTYLE_TIMER}"

  for module in ${modules}
  do
    pushd "${module}" >/dev/null
    echo "  Running checkstyle in ${module}"
    module_suffix=$(basename "${module}")

    checkstyle_mvnrunner \
      "${PATCH_DIR}/maven-patchcheckstyle-${module_suffix}.txt" \
      "${PATCH_DIR}/patchcheckstyle${module_suffix}.txt"

    if [[ $? != 0 ]] ; then
      ((rc = rc +1))
      echo "Post-patch checkstyle compilation is broken."
      add_jira_table -1 checkstyle "Post-patch checkstyle ${module} compilation is broken."
      continue
    fi

    #shellcheck disable=SC2016
    diffpostpatch=$(checkstyle_calcdiffs \
      "${PATCH_DIR}/${PATCH_BRANCH}checkstyle${module_suffix}.txt" \
      "${PATCH_DIR}/patchcheckstyle${module_suffix}.txt" \
      "${PATCH_DIR}/diffcheckstyle${module_suffix}.txt" )

    if [[ ${diffpostpatch} -gt 0 ]] ; then
      ((rc = rc + 1))

      # shellcheck disable=SC2016
      numprepatch=$(wc -l "${PATCH_DIR}/${PATCH_BRANCH}checkstyle${module_suffix}.txt" | ${AWK} '{print $1}')
      # shellcheck disable=SC2016
      numpostpatch=$(wc -l "${PATCH_DIR}/patchcheckstyle${module_suffix}.txt" | ${AWK} '{print $1}')

      add_jira_table -1 checkstyle "The applied patch generated "\
        "${diffpostpatch} new checkstyle issues (total was ${numprepatch}, now ${numpostpatch})."
      footer="${footer} @@BASE@@/diffcheckstyle${module_suffix}.txt"
    fi

    popd >/dev/null
  done

  if [[ ${rc} -gt 0 ]] ; then
    add_jira_footer checkstyle "${footer}"
    return 1
  fi
  add_jira_table +1 checkstyle "There were no new checkstyle issues."
  return 0
}