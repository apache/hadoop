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

declare -a MAVEN_ARGS=("--batch-mode")

if [[ -z "${MAVEN_HOME:-}" ]]; then
  MAVEN=mvn
else
  MAVEN=${MAVEN_HOME}/bin/mvn
fi

add_plugin mvnsite
add_plugin mvneclipse
add_build_tool maven

function maven_usage
{
  echo "maven specific:"
  echo "--mvn-cmd=<cmd>        The 'mvn' command to use (default \${MAVEN_HOME}/bin/mvn, or 'mvn')"
}

function maven_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
      --mvn-cmd=*)
        MAVEN=${i#*=}
      ;;
    esac
  done

  if [[ ${OFFLINE} == "true" ]]; then
    MAVEN_ARGS=(${MAVEN_ARGS[@]} --offline)
  fi
}

function maven_buildfile
{
  echo "pom.xml"
}

function maven_executor
{
  echo "${MAVEN}" "${MAVEN_ARGS[@]}"
}

function mvnsite_filefilter
{
  local filename=$1

  if [[ ${BUILDTOOL} = maven ]]; then
    if [[ ${filename} =~ src/site ]]; then
       yetus_debug "tests/mvnsite: ${filename}"
       add_test mvnsite
     fi
   fi
}

function maven_modules_worker
{
  declare branch=$1
  declare tst=$2

  case ${tst} in
    javac|scalac)
      modules_workers ${branch} ${tst} clean test-compile
    ;;
    javadoc)
      modules_workers ${branch} javadoc clean javadoc:javadoc
    ;;
    unit)
      modules_workers ${branch} unit clean test -fae
    ;;
    *)
      yetus_error "WARNING: ${tst} is unsupported by ${BUILDTOOL}"
      return 1
    ;;
  esac
}

function maven_count_javac_probs
{
  local warningfile=$1

  #shellcheck disable=SC2016,SC2046
  ${GREP} '\[WARNING\]' "${warningfile}" | ${AWK} '{sum+=1} END {print sum}'
}

function maven_builtin_personality_file_tests
{
  local filename=$1

  yetus_debug "Using builtin mvn personality_file_tests"

  if [[ ${filename} =~ src/main/webapp ]]; then
    yetus_debug "tests/webapp: ${filename}"
  elif [[ ${filename} =~ \.sh
       || ${filename} =~ \.cmd
       || ${filename} =~ src/main/scripts
       || ${filename} =~ src/test/scripts
       ]]; then
    yetus_debug "tests/shell: ${filename}"
  elif [[ ${filename} =~ \.c$
       || ${filename} =~ \.cc$
       || ${filename} =~ \.h$
       || ${filename} =~ \.hh$
       || ${filename} =~ \.proto$
       || ${filename} =~ \.cmake$
       || ${filename} =~ CMakeLists.txt
       ]]; then
    yetus_debug "tests/units: ${filename}"
    add_test cc
    add_test unit
  elif [[ ${filename} =~ \.scala$ ]]; then
    add_test javac
    add_test scaladoc
    add_test unit
  elif [[ ${filename} =~ build.xml$
       || ${filename} =~ pom.xml$
       || ${filename} =~ \.java$
       || ${filename} =~ src/main
       ]]; then
      yetus_debug "tests/javadoc+units: ${filename}"
      add_test javac
      add_test javadoc
      add_test unit
  fi

  if [[ ${filename} =~ src/test ]]; then
    yetus_debug "tests"
    add_test unit
  fi

  if [[ ${filename} =~ \.java$ ]]; then
    add_test findbugs
  fi
}

## @description  Helper for check_patch_javadoc
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function maven_count_javadoc_probs
{
  local warningfile=$1

  #shellcheck disable=SC2016,SC2046
  ${GREP} -E "^[0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$1} END {print sum}'
}

## @description  Confirm site pre-patch
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function mvnsite_preapply
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  verify_needed_test mvnsite
  if [[ $? == 0 ]];then
    return 0
  fi
  big_console_header "Pre-patch ${PATCH_BRANCH} site verification"


  personality_modules branch mvnsite
  modules_workers branch mvnsite clean site site:stage
  result=$?
  modules_messages branch mvnsite true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Make sure site still compiles
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function mvnsite_postapply
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  verify_needed_test mvnsite
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "Determining number of patched site errors"

  personality_modules patch mvnsite
  modules_workers patch mvnsite clean site site:stage -Dmaven.javadoc.skip=true
  result=$?
  modules_messages patch mvnsite true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}


## @description  Make sure Maven's eclipse generation works.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function mvneclipse_postapply
{
  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn eclipse:eclipse still works"

  verify_needed_test javac
  if [[ $? == 0 ]]; then
    echo "Patch does not touch any java files. Skipping mvn eclipse:eclipse"
    return 0
  fi

  personality_modules patch mvneclipse
  modules_workers patch mvneclipse eclipse:eclipse
  result=$?
  modules_messages patch mvneclipse true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Verify mvn install works
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function maven_precheck_install
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn install works"

  verify_needed_test javadoc
  retval=$?

  verify_needed_test javac
  ((retval = retval + $? ))
  if [[ ${retval} == 0 ]]; then
    echo "This patch does not appear to need mvn install checks."
    return 0
  fi

  personality_modules branch mvninstall
  modules_workers branch mvninstall -fae clean install -Dmaven.javadoc.skip=true
  result=$?
  modules_messages branch mvninstall true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Verify mvn install works
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function maven_postapply_install
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn install still works"

  verify_needed_test javadoc
  retval=$?

  verify_needed_test javac
  ((retval = retval + $? ))
  if [[ ${retval} == 0 ]]; then
    echo "This patch does not appear to need mvn install checks."
    return 0
  fi

  personality_modules patch mvninstall
  modules_workers patch mvninstall clean install -Dmaven.javadoc.skip=true
  result=$?
  modules_messages patch mvninstall true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}
