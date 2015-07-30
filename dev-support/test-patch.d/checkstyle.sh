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

function checkstyle_filefilter
{
  local filename=$1

  if [[ ${BUILDTOOL} == maven
    || ${BUILDTOOL} == ant ]]; then
    if [[ ${filename} =~ \.java$ ]]; then
      add_test checkstyle
    fi
  fi
}

function checkstyle_runner
{
  local repostatus=$1
  local tmp=${PATCH_DIR}/$$.${RANDOM}
  local j
  local i=0
  local fn
  local savestart=${TIMER}
  local savestop
  local output
  local logfile
  local repo
  local modulesuffix
  local cmd

  modules_reset

  if [[ ${repostatus} == branch ]]; then
    repo=${PATCH_BRANCH}
  else
    repo="the patch"
  fi

  #shellcheck disable=SC2153
  until [[ $i -eq ${#MODULE[@]} ]]; do
    start_clock
    fn=$(module_file_fragment "${MODULE[${i}]}")
    modulesuffix=$(basename "${MODULE[${i}]}")
    output="${PATCH_DIR}/${repostatus}-checkstyle-${fn}.txt"
    logfile="${PATCH_DIR}/maven-${repostatus}-checkstyle-${fn}.txt"
    pushd "${BASEDIR}/${MODULE[${i}]}" >/dev/null

    case ${BUILDTOOL} in
      maven)
        cmd="${MVN} ${MAVEN_ARGS[*]} \
           checkstyle:checkstyle \
          -Dcheckstyle.consoleOutput=true \
          ${MODULEEXTRAPARAM[${i}]//@@@MODULEFN@@@/${fn}} -Ptest-patch"
      ;;
      ant)
        cmd="${ANT}  \
          -Dcheckstyle.consoleOutput=true \
          ${MODULEEXTRAPARAM[${i}]//@@@MODULEFN@@@/${fn}} \
          ${ANT_ARGS[*]} checkstyle"
      ;;
    esac

    #shellcheck disable=SC2086
    echo ${cmd} "> ${logfile}"
    #shellcheck disable=SC2086
    ${cmd}  2>&1 \
            | tee "${logfile}" \
            | ${GREP} ^/ \
            | ${SED} -e "s,${BASEDIR},.,g" \
                > "${tmp}"

    if [[ $? == 0 ]] ; then
      module_status ${i} +1 "${logfile}" "${modulesuffix} in ${repo} passed checkstyle"
    else
      module_status ${i} -1 "${logfile}" "${modulesuffix} in ${repo} failed checkstyle"
      ((result = result + 1))
    fi
    savestop=$(stop_clock)
    #shellcheck disable=SC2034
    MODULE_STATUS_TIMER[${i}]=${savestop}

    for j in ${CHANGED_FILES}; do
      ${GREP} "${j}" "${tmp}" >> "${output}"
    done

    rm "${tmp}" 2>/dev/null
    # shellcheck disable=SC2086
    popd >/dev/null
    ((i=i+1))
  done

  TIMER=${savestart}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

function checkstyle_preapply
{
  local result

  big_console_header "${PATCH_BRANCH} checkstyle"

  start_clock

  verify_needed_test checkstyle
  if [[ $? == 0 ]]; then
    echo "Patch does not need checkstyle testing."
    return 0
  fi

  personality_modules branch checkstyle
  checkstyle_runner branch
  result=$?
  modules_messages branch checkstyle true

  # keep track of how much as elapsed for us already
  CHECKSTYLE_TIMER=$(stop_clock)
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

function checkstyle_postapply
{
  local result
  local module
  local fn
  local i=0
  local numprepatch=0
  local numpostpatch=0
  local diffpostpatch=0

  big_console_header "Patch checkstyle plugin"

  start_clock

  verify_needed_test checkstyle
  if [[ $? == 0 ]]; then
    echo "Patch does not need checkstyle testing."
    return 0
  fi

  personality_modules patch checkstyle
  checkstyle_runner patch
  result=$?


  # add our previous elapsed to our new timer
  # by setting the clock back
  offset_clock "${CHECKSTYLE_TIMER}"

  until [[ $i -eq ${#MODULE[@]} ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    module=${MODULE[$i]}
    fn=$(module_file_fragment "${module}")

    if [[ ! -f "${PATCH_DIR}/branch-checkstyle-${fn}.txt" ]]; then
      touch "${PATCH_DIR}/branch-checkstyle-${fn}.txt"
    fi

    calcdiffs "${PATCH_DIR}/branch-checkstyle-${fn}.txt" "${PATCH_DIR}/patch-checkstyle-${fn}.txt" > "${PATCH_DIR}/diff-checkstyle-${fn}.txt"
    #shellcheck disable=SC2016
    diffpostpatch=$(wc -l "${PATCH_DIR}/diff-checkstyle-${fn}.txt" | ${AWK} '{print $1}')

    if [[ ${diffpostpatch} -gt 0 ]] ; then
      ((result = result + 1))

      # shellcheck disable=SC2016
      numprepatch=$(wc -l "${PATCH_DIR}/branch-checkstyle-${fn}.txt" | ${AWK} '{print $1}')
      # shellcheck disable=SC2016
      numpostpatch=$(wc -l "${PATCH_DIR}/patch-checkstyle-${fn}.txt" | ${AWK} '{print $1}')

      module_status ${i} -1 "diff-checkstyle-${fn}.txt" "Patch generated "\
        "${diffpostpatch} new checkstyle issues in "\
        "${module} (total was ${numprepatch}, now ${numpostpatch})."
    fi
    ((i=i+1))
  done

  modules_messages patch checkstyle true

  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}
