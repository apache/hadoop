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


FINDBUGS_HOME=${FINDBUGS_HOME:-}
FINDBUGS_WARNINGS_FAIL_PRECHECK=false

add_plugin findbugs

function findbugs_file_filter
{
  local filename=$1

  if [[ ${BUILDTOOL} == maven
    || ${BUILDTOOL} == ant ]]; then
    if [[ ${filename} =~ \.java$ ]]; then
      add_test findbugs
    fi
  fi
}

function findbugs_usage
{
  echo "FindBugs specific:"
  echo "--findbugs-home=<path> Findbugs home directory (default FINDBUGS_HOME environment variable)"
  echo "--findbugs-strict-precheck If there are Findbugs warnings during precheck, fail"
}

function findbugs_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
    --findbugs-home=*)
      FINDBUGS_HOME=${i#*=}
    ;;
    --findbugs-strict-precheck)
      FINDBUGS_WARNINGS_FAIL_PRECHECK=true
    ;;
    esac
  done
}

## @description  are the needed bits for findbugs present?
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 findbugs will work for our use
## @return       1 findbugs is missing some component
function findbugs_is_installed
{
  if [[ ! -e "${FINDBUGS_HOME}/bin/findbugs" ]]; then
    printf "\n\n%s is not executable.\n\n" "${FINDBUGS_HOME}/bin/findbugs"
    add_vote_table -1 findbugs "Findbugs is not installed."
    return 1
  fi
  return 0
}

## @description  Run the maven findbugs plugin and record found issues in a bug database
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function findbugs_runner
{
  local name=$1
  local module
  local result=0
  local fn
  local warnings_file
  local i=0
  local savestop

  personality_modules "${name}" findbugs
  case ${BUILDTOOL} in
    maven)
      modules_workers "${name}" findbugs clean test findbugs:findbugs
    ;;
    ant)
      modules_workers "${name}" findbugs findbugs
    ;;
  esac

  #shellcheck disable=SC2153
  until [[ ${i} -eq ${#MODULE[@]} ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    start_clock
    offset_clock "${MODULE_STATUS_TIMER[${i}]}"
    module="${MODULE[${i}]}"
    fn=$(module_file_fragment "${module}")

    case ${BUILDTOOL} in
      maven)
        file="${module}/target/findbugsXml.xml"
      ;;
      ant)
        file="${ANT_FINDBUGSXML}"
      ;;
    esac

    if [[ ! -f ${file} ]]; then
      module_status ${i} -1 "" "${name}/${module} no findbugs output file (${file})"
      ((i=i+1))
      continue
    fi

    warnings_file="${PATCH_DIR}/${name}-findbugs-${fn}-warnings"

    cp -p "${file}" "${warnings_file}.xml"

    if [[ ${name} == branch ]]; then
      "${FINDBUGS_HOME}/bin/setBugDatabaseInfo" -name "${PATCH_BRANCH}" \
          "${warnings_file}.xml" "${warnings_file}.xml"
    else
      "${FINDBUGS_HOME}/bin/setBugDatabaseInfo" -name patch \
          "${warnings_file}.xml" "${warnings_file}.xml"
    fi
    if [[ $? != 0 ]]; then
      savestop=$(stop_clock)
      MODULE_STATUS_TIMER[${i}]=${savestop}
      module_status ${i} -1 "" "${name}/${module} cannot run setBugDatabaseInfo from findbugs"
      ((retval = retval + 1))
      ((i=i+1))
      continue
    fi

    "${FINDBUGS_HOME}/bin/convertXmlToText" -html \
      "${warnings_file}.xml" \
      "${warnings_file}.html"
    if [[ $? != 0 ]]; then
      savestop=$(stop_clock)
      MODULE_STATUS_TIMER[${i}]=${savestop}
      module_status ${i} -1 "" "${name}/${module} cannot run convertXmlToText from findbugs"
      ((result = result + 1))
    fi

    if [[ -z ${FINDBUGS_VERSION}
        && ${name} == branch ]]; then
      FINDBUGS_VERSION=$(${GREP} -i "BugCollection version=" "${warnings_file}.xml" \
        | cut -f2 -d\" \
        | cut -f1 -d\" )
      if [[ -n ${FINDBUGS_VERSION} ]]; then
        add_footer_table findbugs "v${FINDBUGS_VERSION}"
      fi
    fi

    ((i=i+1))
  done
  return ${result}
}

## @description  Track pre-existing findbugs warnings
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function findbugs_preapply
{
  local fn
  local module
  local i=0
  local warnings_file
  local module_findbugs_warnings
  local results=0

  big_console_header "Pre-patch findbugs detection"

  verify_needed_test findbugs

  if [[ $? == 0 ]]; then
    echo "Patch does not appear to need findbugs tests."
    return 0
  fi

  findbugs_is_installed
  if [[ $? != 0 ]]; then
    return 1
  fi

  findbugs_runner branch
  results=$?

  if [[ "${FINDBUGS_WARNINGS_FAIL_PRECHECK}" == "true" ]]; then
    until [[ $i -eq ${#MODULE[@]} ]]; do
      if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
        ((result=result+1))
        ((i=i+1))
        continue
      fi
      module=${MODULE[${i}]}
      start_clock
      offset_clock "${MODULE_STATUS_TIMER[${i}]}"
      fn=$(module_file_fragment "${module}")
      warnings_file="${PATCH_DIR}/branch-findbugs-${fn}-warnings"
      # shellcheck disable=SC2016
      module_findbugs_warnings=$("${FINDBUGS_HOME}/bin/filterBugs" -first \
          "${PATCH_BRANCH}" \
          "${warnings_file}.xml" \
          "${warnings_file}.xml" \
          | ${AWK} '{print $1}')

      if [[ ${module_findbugs_warnings} -gt 0 ]] ; then
        module_status ${i} -1 "branch-findbugs-${fn}.html" "${module} in ${PATCH_BRANCH} cannot run convertXmlToText from findbugs"
        ((results=results+1))
      fi
      savestop=$(stop_clock)
      MODULE_STATUS_TIMER[${i}]=${savestop}
      ((i=i+1))
    done
    modules_messages branch findbugs true
  fi

  if [[ ${results} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Verify patch does not trigger any findbugs warnings
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function findbugs_postinstall
{
  local module
  local fn
  local combined_xml
  local branchxml
  local patchxml
  local newbugsbase
  local new_findbugs_warnings
  local line
  local firstpart
  local secondpart
  local i=0
  local results=0
  local savestop

  big_console_header "Patch findbugs detection"

  verify_needed_test findbugs

  if [[ $? == 0 ]]; then
    echo "Patch does not appear to need findbugs tests."
    return 0
  fi

  findbugs_is_installed
  if [[ $? != 0 ]]; then
    return 1
  fi

  findbugs_runner patch

  until [[ $i -eq ${#MODULE[@]} ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    start_clock
    offset_clock "${MODULE_STATUS_TIMER[${i}]}"
    module="${MODULE[${i}]}"
    pushd "${module}" >/dev/null
    fn=$(module_file_fragment "${module}")

    combined_xml="${PATCH_DIR}/combined-findbugs-${fn}.xml"
    branchxml="${PATCH_DIR}/branch-findbugs-${fn}-warnings.xml"
    patchxml="${PATCH_DIR}/patch-findbugs-${fn}-warnings.xml"

    if [[ ! -f "${branchxml}" ]]; then
      branchxml=${patchxml}
    fi

    newbugsbase="${PATCH_DIR}/new-findbugs-${fn}"

    "${FINDBUGS_HOME}/bin/computeBugHistory" -useAnalysisTimes -withMessages \
            -output "${combined_xml}" \
            "${branchxml}" \
            "${patchxml}"
    if [[ $? != 0 ]]; then
      popd >/dev/null
      module_status ${i} -1 "" "${module} cannot run computeBugHistory from findbugs"
      ((result=result+1))
      savestop=$(stop_clock)
      MODULE_STATUS_TIMER[${i}]=${savestop}
      ((i=i+1))
      continue
    fi

    #shellcheck disable=SC2016
    new_findbugs_warnings=$("${FINDBUGS_HOME}/bin/filterBugs" -first patch \
        "${combined_xml}" "${newbugsbase}.xml" | ${AWK} '{print $1}')
    if [[ $? != 0 ]]; then
      popd >/dev/null
      module_status ${i} -1 "" "${module} cannot run filterBugs (#1) from findbugs"
      ((result=result+1))
      savestop=$(stop_clock)
      MODULE_STATUS_TIMER[${i}]=${savestop}
      ((i=i+1))
      continue
    fi

    #shellcheck disable=SC2016
    new_findbugs_fixed_warnings=$("${FINDBUGS_HOME}/bin/filterBugs" -fixed patch \
        "${combined_xml}" "${newbugsbase}.xml" | ${AWK} '{print $1}')
    if [[ $? != 0 ]]; then
      popd >/dev/null
      module_status ${i} -1 "" "${module} cannot run filterBugs (#2) from findbugs"
      ((result=result+1))
      savestop=$(stop_clock)
      MODULE_STATUS_TIMER[${i}]=${savestop}
      ((i=i+1))
      continue
    fi

    echo "Found ${new_findbugs_warnings} new Findbugs warnings and ${new_findbugs_fixed_warnings} newly fixed warnings."
    findbugs_warnings=$((findbugs_warnings+new_findbugs_warnings))
    findbugs_fixed_warnings=$((findbugs_fixed_warnings+new_findbugs_fixed_warnings))

    "${FINDBUGS_HOME}/bin/convertXmlToText" -html "${newbugsbase}.xml" \
        "${newbugsbase}.html"
    if [[ $? != 0 ]]; then
      popd >/dev/null
      module_status ${i} -1 "" "${module} cannot run convertXmlToText from findbugs"
      ((result=result+1))
      savestop=$(stop_clock)
      MODULE_STATUS_TIMER[${i}]=${savestop}
      ((i=i+1))
      continue
    fi

    if [[ ${new_findbugs_warnings} -gt 0 ]] ; then
      populate_test_table FindBugs "module:${module}"
      while read line; do
        firstpart=$(echo "${line}" | cut -f2 -d:)
        secondpart=$(echo "${line}" | cut -f9- -d' ')
        add_test_table "" "${firstpart}:${secondpart}"
      done < <("${FINDBUGS_HOME}/bin/convertXmlToText" "${newbugsbase}.xml")

      module_status ${i} -1 "new-findbugs-${fn}.html" "${module} introduced "\
        "${new_findbugs_warnings} new FindBugs issues."
      ((result=result+1))
    fi
    savestop=$(stop_clock)
    MODULE_STATUS_TIMER[${i}]=${savestop}
    popd >/dev/null
    ((i=i+1))
  done

  modules_messages patch findbugs true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}
