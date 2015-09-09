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

add_plugin author

## @description  Check the current directory for @author tags
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function author_patchfile
{
  declare patchfile=$1
  declare authorTags
  # shellcheck disable=SC2155
  declare -r appname=$(basename "${BASH_SOURCE-$0}")

  big_console_header "Checking there are no @author tags in the patch."

  start_clock

  if [[ ${CHANGED_FILES} =~ ${appname} ]]; then
    echo "Skipping @author checks as ${appname} has been patched."
    add_vote_table 0 @author "Skipping @author checks as ${appname} has been patched."
    return 0
  fi

  authorTags=$("${GREP}" -c -i '^[^-].*@author' "${patchfile}")
  echo "There appear to be ${authorTags} @author tags in the patch."
  if [[ ${authorTags} != 0 ]] ; then
    add_vote_table -1 @author \
      "The patch appears to contain ${authorTags} @author tags which the" \
      " community has agreed to not allow in code contributions."
    return 1
  fi
  add_vote_table +1 @author "The patch does not contain any @author tags."
  return 0
}