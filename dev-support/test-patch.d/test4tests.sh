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

add_plugin test4tests

## @description  Check the patch file for changed/new tests
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function test4tests_patchfile
{
  declare testReferences=0
  declare i

  big_console_header "Checking there are new or changed tests in the patch."

  verify_needed_test unit

  if [[ $? == 0 ]]; then
    echo "Patch does not appear to need new or modified tests."
    return 0
  fi

  start_clock

  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ (^|/)test/ ]]; then
      ((testReferences=testReferences + 1))
    fi
  done

  echo "There appear to be ${testReferences} test file(s) referenced in the patch."
  if [[ ${testReferences} == 0 ]] ; then
    add_vote_table -1 "test4tests" \
      "The patch doesn't appear to include any new or modified tests. " \
      "Please justify why no new tests are needed for this patch." \
      "Also please list what manual steps were performed to verify this patch."
    return 1
  fi
  add_vote_table +1 "test4tests" \
    "The patch appears to include ${testReferences} new or modified test files."
  return 0
}