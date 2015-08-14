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

if [[ -z "${ANT_HOME:-}" ]]; then
  ANT=ant
else
  ANT=${ANT_HOME}/bin/ant
fi

add_build_tool ant

declare -a ANT_ARGS=("-noinput")

function ant_usage
{
  echo "ant specific:"
  echo "--ant-cmd=<cmd>        The 'ant' command to use (default \${ANT_HOME}/bin/ant, or 'ant')"
}

function ant_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
      --ant-cmd=*)
        ANT=${i#*=}
      ;;
    esac
  done

  # if we requested offline, pass that to mvn
  if [[ ${OFFLINE} == "true" ]]; then
    ANT_ARGS=(${ANT_ARGS[@]} -Doffline=)
  fi
}

function ant_buildfile
{
  echo "build.xml"
}

function ant_executor
{
  echo "${ANT}" "${ANT_ARGS[@]}"
}

function ant_modules_worker
{
  declare branch=$1
  declare tst=$2
  shift 2

  case ${tst} in
    javac)
      modules_workers ${branch} javac
    ;;
    javadoc)
      modules_workers ${branch} javadoc clean javadoc
    ;;
    unit)
      modules_workers ${branch} unit
    ;;
    *)
      yetus_error "WARNING: ${tst} is unsupported by ${BUILDTOOL}"
      return 1
    ;;
  esac
}

function ant_count_javac_probs
{
  declare warningfile=$1
  declare val1
  declare val2

  #shellcheck disable=SC2016
  val1=$(${GREP} -E "\[javac\] [0-9]+ errors?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  #shellcheck disable=SC2016
  val2=$(${GREP} -E "\[javac\] [0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  echo $((val1+val2))
}

## @description  Helper for check_patch_javadoc
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function ant_count_javadoc_probs
{
  local warningfile=$1
  local val1
  local val2

  #shellcheck disable=SC2016
  val1=$(${GREP} -E "\[javadoc\] [0-9]+ errors?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  #shellcheck disable=SC2016
  val2=$(${GREP} -E "\[javadoc\] [0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  echo $((val1+val2))
}

function ant_builtin_personality_file_tests
{
  local filename=$1

  yetus_debug "Using builtin ant personality_file_tests"

  if [[ ${filename} =~ \.sh
       || ${filename} =~ \.cmd
       ]]; then
    yetus_debug "tests/shell: ${filename}"
  elif [[ ${filename} =~ \.c$
       || ${filename} =~ \.cc$
       || ${filename} =~ \.h$
       || ${filename} =~ \.hh$
       || ${filename} =~ \.proto$
       || ${filename} =~ src/test
       || ${filename} =~ \.cmake$
       || ${filename} =~ CMakeLists.txt
       ]]; then
    yetus_debug "tests/units: ${filename}"
    add_test javac
    add_test unit
  elif [[ ${filename} =~ build.xml
       || ${filename} =~ ivy.xml
       || ${filename} =~ \.java$
       ]]; then
      yetus_debug "tests/javadoc+units: ${filename}"
      add_test javac
      add_test javadoc
      add_test unit
  fi

  if [[ ${filename} =~ \.java$ ]]; then
    add_test findbugs
  fi
}