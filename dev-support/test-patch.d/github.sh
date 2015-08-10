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

GITHUB_URL="https://github.com"
GITHUB_REPO="apache/hadoop"

GITHUB_PASSWD=""
GITHUB_TOKEN=""
GITHUB_USER=""
GITHUB_ISSUE=""

function github_usage
{
  echo "GITHUB Options:"
  echo "--github-base-url=<url>  The URL of the JIRA server (default:'${GITHUB_URL}')"
  echo "--github-password=<pw>   Github password"
  echo "--github-repo=<repo>     github repo to use (default:'${GITHUB_REPO}')"
  echo "--github-token=<token>   The token to use to write to github"
  echo "--github-user=<user>     Github user"

}

function github_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --github-base-url=*)
        GITHUB_URL=${i#*=}
      ;;
      --github-repo=*)
        GITHUB_REPO=${i#*=}
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

## @description this gets called when JIRA thinks this
## @description issue is just a pointer to github
function github_jira_bridge
{
  declare fileloc=$1
  declare urlfromjira
  declare count
  declare pos1
  declare pos2

  # the JIRA issue has already been downloaded.  So let's
  # find the URL.  This is currently hard-coded to github.com
  # Sorry Github Enterprise users. :(

  # shellcheck disable=SC2016
  urlfromjira=$(${AWK} 'match($0,"https://github.com/.*patch"){print $1}' "${PATCH_DIR}/jira" | tail -1)
  count=${urlfromjira//[^\/]}
  count=${#count}
  ((pos2=count-3))
  ((pos1=pos2))

  GITHUB_URL=$(echo "${urlfromjira}" | cut -f1-${pos2} -d/)

  ((pos1=pos1+1))
  ((pos2=pos1+1))

  GITHUB_REPO=$(echo "${urlfromjira}" | cut -f${pos1}-${pos2} -d/)

  ((pos1=pos2+2))
  unset pos2

  GITHUB_ISSUE=$(echo "${urlfromjira}" | cut -f${pos1}-${pos2} -d/ | cut -f1 -d.)

  github_locate_patch "${GITHUB_ISSUE}" "${fileloc}"
}

function github_determine_issue
{
  declare input=$1

  if [[ ${input} =~ ^[0-9]+$
     && -n ${GITHUB_REPO} ]]; then
    ISSUE=${input}
    return 0
  fi

  return 1
}

function github_locate_patch
{
  declare input=$1
  declare output=$2

  if [[ "${OFFLINE}" == true ]]; then
    yetus_debug "github_locate_patch: offline, skipping"
    return 1
  fi

  if [[ ! ${input} =~ ^[0-9]+$ ]]; then
    yetus_debug "github: ${input} is not a pull request #"
    return 1
  fi

  PATCHURL="${GITHUB_URL}/${GITHUB_REPO}/pull/${input}.patch"
  echo "GITHUB PR #${input} is being downloaded at $(date) from"
  echo "${PATCHURL}"

  ${CURL} --silent --fail \
          --output "${output}" \
          --location \
         "${PATCHURL}"

  if [[ $? != 0 ]]; then
    yetus_debug "github_locate_patch: not a github pull request."
    return 1
  fi

  # https://api.github.com/repos/apache/hadoop/pulls/25
  # base->sha?

  GITHUB_ISSUE=${input}

  add_footer_table "GITHUB PR" "${GITHUB_URL}/${GITHUB_REPO}/pull/${input}"

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

  declare retval=0

  if [[ "${OFFLINE}" == true ]]; then
    return 0
  fi

  echo "{\"body\":\"" > "${PATCH_DIR}/ghcomment.$$"
  sed -e 's,\\,\\\\,g' \
      -e 's,\",\\\",g' "${PATCH_DIR}/ghcomment.$$" \
    >> "${PATCH_DIR}/ghcomment.$$"
  echo "\"}" >> "${PATCH_DIR}/ghcomment.$$"

  if [[ -n ${GITHUB_USER}
     && -n ${GITHUB_PASSWD} ]]; then
    githubauth="-u \"${GITHUB_USER}:${GITHUB_PASSWD}\""
  elif [[ -n ${GITHUB_TOKEN} ]]; then
    githubauth="-H \"Authorization: token ${GITHUB_TOKEN}\""
  else
    return 0
  fi

  ${CURL} -X POST \
       -H "Accept: application/json" \
       -H "Content-Type: application/json" \
       ${githubauth} \
       -d @"${PATCH_DIR}/jiracomment.$$" \
       --silent --location \
         "${JIRA_URL}/rest/api/2/issue/${ISSUE}/comment" \
        >/dev/null

  retval=$?
  rm "${PATCH_DIR}/jiracomment.$$"
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
  declare commentfile=${PATCH_DIR}/gitcommentfile.$$
  declare comment

  # TODO: There really should be a reference to the JIRA issue, as needed

  rm "${commentfile}" 2>/dev/null

  if [[ ${JENKINS} != "true"
    || -z ${GITHUB_ISSUE} ]] ; then
    return 0
  fi

  big_console_header "Adding comment to Github"

  add_footer_table "Console output" "${BUILD_URL}console"

  if [[ ${result} == 0 ]]; then
    echo ":confetti_ball: **+1 overall**" >> "${commentfile}"
  else
    echo ":broken_heart: **-1 overall**" >> "${commentfile}"
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

  github_write_comment "${commentfile}"
}
