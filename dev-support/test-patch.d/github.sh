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

GITHUBURL="https://github.com"
GITHUBREPO="apache/hadoop"

function github_usage
{
  echo "GITHUB Options:"
  echo "--github-base-url=<url>  The URL of the JIRA server (default:'${GITHUBURL}')"
  echo "--github-password=<pw>   Github password"
  echo "--github-repo=<repo>     github repo to use (default:'${GITHUBREPO}')"
  echo "--github-token=<token>   The token to use to write to github"
  echo "--github-user=<user>     Github user"

}

function github_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --github-base-url=*)
        GITHUBURL=${i#*=}
      ;;
      --github-repo=*)
        GITHUBREPO=${i#*=}
      ;;
      --github-token=*)
        GITHUB_TOKEN=${i#*=}
      ;;
      --github-password=*)
        GITHUB_PASSWD=${i#*=}
      ;;
      --github-user=*)
        GITHUB_USER=${i#*=}
      ;;
    esac
  done
}

function github_locate_patch
{
  declare input=$1
  declare output=$2

  if [[ "${OFFLINE}" == true ]]; then
    yetus_debug "github_locate_patch: offline, skipping"
    return 1
  fi

  ${WGET} -q -O "${output}" "${GITHUBURL}/${GITHUBREPO}/pull/${input}.patch"
  if [[ $? != 0 ]]; then
    yetus_debug "github_locate_patch: not a github pull request."
    return 1
  fi

  # https://api.github.com/repos/apache/hadoop/pulls/25
  # base->sha?

  GITHUBISSUE=${input}
  GITHUBCOMMITID=""
  return 0
}

## @description Write the contents of a file to github
## @params filename
## @stability stable
## @audience public
function github_write_comment
{
  declare -r commentfile=${1}
  shift

  declare retval=1

  return ${retval}
}


## @description  Print out the finished details to the Github PR
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
function github_finalreport
{
  declare result=$1
  declare i
  declare commentfile=${PATCH_DIR}/commentfile
  declare comment

  # TODO: There really should be a reference to the JIRA issue, as needed

  rm "${commentfile}" 2>/dev/null

  if [[ ${JENKINS} != "true"
    || -z ${GITHUBISSUE} ]] ; then
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
