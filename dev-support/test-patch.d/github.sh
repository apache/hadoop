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

add_bugsystem github

## @description Write the contents of a file to github
## @params filename
## @stability stable
## @audience public
function github_write_comment
{
  local -r commentfile=${1}
  shift

  local retval=1

  return ${retval}
}


## @description  Print out the finished details to the Github PR
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
function github_finalreport
{
  local result=$1
  local i
  local commentfile=${PATCH_DIR}/commentfile
  local comment

  rm "${commentfile}" 2>/dev/null

  if [[ ${JENKINS} != "true" ]] ; then
    return 0
  fi

  big_console_header "Adding comment to Github"

  add_footer_table "Console output" "${BUILD_URL}console"

  if [[ ${result} == 0 ]]; then
    add_header_line ":confetti_ball: **+1 overall**"
  else
    add_header_line ":broken_heart: **-1 overall**"
  fi

  printf "\n\n\n\n" >>  "${commentfile}"

  i=0
  until [[ $i -eq ${#TP_HEADER[@]} ]]; do
    printf "%s\n\n" "${TP_HEADER[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done

  {
    printf "\n\n"
    echo "| Vote | Subsystem | Runtime | Comment |"
    echo "|:----:|----------:|--------:|:--------|"
  } >> "${commentfile}"

  i=0
  until [[ $i -eq ${#TP_VOTE_TABLE[@]} ]]; do
    echo "${TP_VOTE_TABLE[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done

  if [[ ${#TP_TEST_TABLE[@]} -gt 0 ]]; then
    {
      printf "\n\n"
      echo "| Reason | Tests |"
      echo "|-------:|:------|"
    } >> "${commentfile}"
    i=0
    until [[ $i -eq ${#TP_TEST_TABLE[@]} ]]; do
      echo "${TP_TEST_TABLE[${i}]}" >> "${commentfile}"
      ((i=i+1))
    done
  fi

  {
    printf "\n\n"
    echo "| Subsystem | Report/Notes |"
    echo "|----------:|:-------------|"
  } >> "${commentfile}"

  i=0
  until [[ $i -eq ${#TP_FOOTER_TABLE[@]} ]]; do
    comment=$(echo "${TP_FOOTER_TABLE[${i}]}" |
              ${SED} -e "s,@@BASE@@,${BUILD_URL}artifact/patchprocess,g")
    printf "%s\n" "${comment}" >> "${commentfile}"
    ((i=i+1))
  done

  printf "\n\nThis message was automatically generated.\n\n" >> "${commentfile}"

  write_to_github "${commentfile}"
}
