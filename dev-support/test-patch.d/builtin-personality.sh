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

function builtin_personality_modules
{
  local repostatus=$1
  local testtype=$2

  local module

  yetus_debug "Using builtin personality_modules"
  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  # this always makes sure the local repo has a fresh
  # copy of everything per pom rules.
  if [[ ${repostatus} == branch
     && ${testtype} == mvninstall ]];then
     personality_enqueue_module .
     return
   fi

  for module in ${CHANGED_MODULES}; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module}
  done
}

function personality_modules
{
  builtin_personality_modules "$@"
}

function builtin_mvn_personality_file_tests
{
  local filename=$1

  yetus_debug "Using builtin mvn personality_file_tests"

  if [[ ${filename} =~ src/main/webapp ]]; then
    yetus_debug "tests/webapp: ${filename}"
  elif [[ ${filename} =~ \.sh
       || ${filename} =~ \.cmd
       ]]; then
    yetus_debug "tests/shell: ${filename}"
  elif [[ ${filename} =~ \.md$
       || ${filename} =~ \.md\.vm$
       || ${filename} =~ src/site
       || ${filename} =~ src/main/docs
       ]]; then
    yetus_debug "tests/site: ${filename}"
    add_test site
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
    add_test mvninstall
    add_test unit
  elif [[ ${filename} =~ pom.xml$
       || ${filename} =~ \.java$
       || ${filename} =~ \.scala$
       || ${filename} =~ src/main
       ]]; then
    if [[ ${filename} =~ src/main/bin
       || ${filename} =~ src/main/sbin ]]; then
      yetus_debug "tests/shell: ${filename}"
    else
      yetus_debug "tests/javadoc+units: ${filename}"
      add_test javac
      add_test javadoc
      add_test mvninstall
      add_test unit
    fi
  fi

  if [[ ${filename} =~ \.java$ ]]; then
    add_test findbugs
  fi
}

function builtin_ant_personality_file_tests
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

function builtin_personality_file_tests
{
  case ${BUILDTOOL} in
    maven)
      builtin_mvn_personality_file_tests "$@"
    ;;
    ant)
      builtin_ant_personality_file_tests "$@"
    ;;
    *)
      return 1
    ;;
  esac
}

function personality_file_tests
{
  builtin_personality_file_tests "$@"
}
