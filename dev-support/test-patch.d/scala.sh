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

add_plugin scalac
add_plugin scaladoc

function scalac_filefilter
{
  declare filename=$1

  if [[ ${filename} =~ \.scala$ ]]; then
   yetus_debug "tests/scalac: ${filename}"
   add_test scalac
   add_test compile
  fi
}

function scaladoc_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.scala$ ]]; then
    yetus_debug "tests/scaladoc: ${filename}"
    add_test scaladoc
  fi
}

## @description
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function scalac_compile
{
  declare codebase=$1
  declare multijdkmode=$2

  verify_needed_test scalac
  if [[ $? = 0 ]]; then
    return 0
  fi

  if [[ ${codebase} = patch ]]; then
    generic_postlog_compare compile scalac "${multijdkmode}"
  fi
}

## @description  Count and compare the number of JavaDoc warnings pre- and post- patch
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function scaladoc_rebuild
{
  declare codebase=$1

  if [[ "${codebase}" = branch ]]; then
    generic_pre_handler scaladoc false
  else
    generic_post_handler scaladoc scaladoc false true
  fi
}