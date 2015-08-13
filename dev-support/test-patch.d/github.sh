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

# This bug system provides github integration

add_bugsystem github

# personalities can override the following settings:

# Web interface URL.
GITHUB_BASE_URL="https://github.com"

# API interface URL.
GITHUB_API_URL="https://api.github.com"

# user/repo
GITHUB_REPO=""

# user settings
GITHUB_PASSWD=""
GITHUB_TOKEN=""
GITHUB_USER=""
GITHUB_ISSUE=""

# private globals...
GITHUB_BRIDGED=false
GITHUB_COMMITSHA=""

function github_usage
{
  echo "GITHUB Options:"
  echo "--github-api-url=<url>   The URL of the API for github (default: '${GITHUB_API_URL}')"
  echo "--github-base-url=<url>  The URL of the github server (default:'${GITHUB_BASE_URL}')"
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
      --github-api-url=*)
        GITHUB_API_URL=${i#*=}
      ;;
      --github-base-url=*)
        GITHUB_BASE_URL=${i#*=}
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
## @description WARNING: Called from JIRA plugin!
function github_jira_bridge
{
  declare fileloc=$1
  declare urlfromjira

  # we use this to prevent loops later on
  GITHUB_BRIDGED=true

  # the JIRA issue has already been downloaded.  So let's
  # find the URL.  This is currently hard-coded to github.com
  # Sorry Github Enterprise users. :(

  # shellcheck disable=SC2016
  urlfromjira=$(${AWK} 'match($0,"https://github.com/.*patch"){print $1}' "${PATCH_DIR}/jira" | tail -1)
  github_breakup_url "${urlfromjira}"
  github_locate_patch "${GITHUB_ISSUE}" "${fileloc}"
}

## @description given a URL, break it up into github plugin globals
## @description this will *override* any personality or yetus defaults
## @params url
function github_breakup_url
{
  declare url=$1
  declare count
  declare pos1
  declare pos2

  count=${url//[^\/]}
  count=${#count}
  ((pos2=count-3))
  ((pos1=pos2))

  GITHUB_BASE_URL=$(echo "${url}" | cut -f1-${pos2} -d/)

  ((pos1=pos1+1))
  ((pos2=pos1+1))

  GITHUB_REPO=$(echo "${url}" | cut -f${pos1}-${pos2} -d/)

  ((pos1=pos2+2))
  unset pos2

  GITHUB_ISSUE=$(echo "${url}" | cut -f${pos1}-${pos2} -d/ | cut -f1 -d.)
}


## @description based upon a github PR, attempt to link back to JIRA
function github_find_jira_title
{
  declare title
  declare maybe
  declare retval

  if [[ ! -f "${PATCH_DIR}/github-pull.json" ]]; then
    return 1
  fi

  title=$(GREP title "${PATCH_DIR}/github-pull.json" \
    | cut -f4 -d\")

  # people typically do two types:  JIRA-ISSUE: and [JIRA-ISSUE]
  # JIRA_ISSUE_RE is pretty strict so we need to chop that stuff
  # out first

  maybe=$(echo "${title}" | cut -f2 -d\[ | cut -f1 -d\])
  jira_determine_issue "${maybe}"
  retval=$?

  if [[ ${retval} == 0 ]]; then
    return 0
  fi

  maybe=$(echo "${title}" | cut -f1 -d:)
  jira_determine_issue "${maybe}"
  retval=$?

  if [[ ${retval} == 0 ]]; then
    return 0
  fi
}

function github_determine_issue
{
  declare input=$1

  if [[ ${input} =~ ^[0-9]+$
     && -n ${GITHUB_REPO} ]]; then
    # shellcheck disable=SC2034
    ISSUE=${input}
    if [[ -z ${GITHUB_ISSUE} ]]; then
      GITHUB_ISSUE=${input}
    fi
  fi

  # if JIRA didn't call us, should we call it?
  if [[ ${GITHUB_BRIDGED} == false ]]; then
    github_find_jira_title
    if [[ $? == 0 ]]; then
      return 0
    fi
  fi

  if [[ -n ${GITHUB_ISSUE} ]]; then
    return 0
  fi

  return 1
}

## @description  Try to guess the branch being tested using a variety of heuristics
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success, with PATCH_BRANCH updated appropriately
## @return       1 on failure
function github_determine_branch
{
  if [[ ! -f "${PATCH_DIR}/github-pull.json" ]]; then
    return 1
  fi

  # shellcheck disable=SC2016
  PATCH_BRANCH=$(${AWK} 'match($0,"\"ref\": \""){print $2}' "${PATCH_DIR}/github-pull.json"\
     | cut -f2 -d\"\
     | tail -1  )

  yetus_debug "Github determine branch: starting with ${PATCH_BRANCH}"

  verify_valid_branch  "${PATCH_BRANCH}"
  if [[ $? == 0 ]]; then
    return 0
  fi
  return 1
}

function github_locate_patch
{
  declare input=$1
  declare output=$2
  declare githubauth

  if [[ "${OFFLINE}" == true ]]; then
    yetus_debug "github_locate_patch: offline, skipping"
    return 1
  fi


  # https://github.com/your/repo/pull/##
  if [[ ${input} =~ ^${GITHUB_BASE_URL}.*/pull/[0-9]+$ ]]; then
    github_breakup_url "${input}.patch"
    input=${GITHUB_ISSUE}
  fi

  # https://github.com/your/repo/pulls/##.patch
  if [[ ${input} =~ ^${GITHUB_BASE_URL}.*patch$ ]]; then
    github_breakup_url "${input}"
    input=${GITHUB_ISSUE}
  fi

  # https://github.com/your/repo/pulls/##.diff
  if [[ ${input} =~ ^${GITHUB_BASE_URL}.*diff$ ]]; then
    github_breakup_url "${input}"
    input=${GITHUB_ISSUE}
  fi

  # if it isn't a number at this point, no idea
  # how to process
  if [[ ! ${input} =~ ^[0-9]+$ ]]; then
    yetus_debug "github: ${input} is not a pull request #"
    return 1
  fi

  # we always pull the .patch version (even if .diff was given)
  # with the assumption that this way binary files work.
  # The downside of this is that the patch files are
  # significantly larger and therefore take longer to process
  PATCHURL="${GITHUB_BASE_URL}/${GITHUB_REPO}/pull/${input}.patch"
  echo "GITHUB PR #${input} is being downloaded at $(date) from"
  echo "${GITHUB_BASE_URL}/${GITHUB_REPO}/pull/${input}"

  if [[ -n "${GITHUB_USER}"
     && -n "${GITHUB_PASSWD}" ]]; then
    githubauth="${GITHUB_USER}:${GITHUB_PASSWD}"
  elif [[ -n "${GITHUB_TOKEN}" ]]; then
    githubauth="Authorization: token ${GITHUB_TOKEN}"
  else
    githubauth="X-ignore-me: fake"
  fi

  # Let's pull the PR JSON for later use
  ${CURL} --silent --fail \
          -H "Accept: application/vnd.github.v3.full+json" \
          -H "${githubauth}" \
          --output "${PATCH_DIR}/github-pull.json" \
          --location \
         "${GITHUB_API_URL}/repos/${GITHUB_REPO}/pulls/${input}"

  echo "Patch from GITHUB PR #${input} is being downloaded at $(date) from"
  echo "${PATCHURL}"

  # the actual patch file
  ${CURL} --silent --fail \
          --output "${output}" \
          --location \
          -H "${githubauth}" \
         "${PATCHURL}"

  if [[ $? != 0 ]]; then
    yetus_debug "github_locate_patch: not a github pull request."
    return 1
  fi

  GITHUB_ISSUE=${input}

  # github will translate this to be #(xx) !
  add_footer_table "GITHUB PR" "${GITHUB_BASE_URL}/${GITHUB_REPO}/pull/${input}"

  return 0
}

function github_linecomments
{
  declare plugin=$1
  declare file=$2
  # shellcheck disable=SC2034
  declare realline=$3
  declare uniline=$4
  declare text=$5
  declare tempfile="${PATCH_DIR}/ghcomment.$$.${RANDOM}"
  declare githubauth

  if [[ "${file}" =~ ^./ ]]; then
    file=${file##./}
  fi

  if [[ -z "${GITHUB_COMMITSHA}" ]]; then
    GITHUB_COMMITSHA=$(${GREP} \"sha\" "${PATCH_DIR}/github-pull.json" 2>/dev/null \
      | head -1 \
      | cut -f4 -d\")
  fi

  if [[ -z "${uniline}" ]]; then
    return
  fi

  # build our REST post
  {
    printf "{\"body\":\""
    echo "${plugin}: ${text}" \
      | ${SED} -e 's,\\,\\\\,g' \
        -e 's,\",\\\",g' \
        -e 's,$,\\r\\n,g' \
      | tr -d '\n'
    echo "\","
    echo "\"commit_id\":\"${GITHUB_COMMITSHA}\","
    echo "\"path\":\"${file}\","
    echo "\"position\":${uniline}"
    echo "}"
  } > "${tempfile}"

  if [[ -n "${GITHUB_USER}"
     && -n "${GITHUB_PASSWD}" ]]; then
    githubauth="${GITHUB_USER}:${GITHUB_PASSWD}"
  elif [[ -n "${GITHUB_TOKEN}" ]]; then
    githubauth="Authorization: token ${GITHUB_TOKEN}"
  else
    return 0
  fi

  ${CURL} -X POST \
    -H "Accept: application/vnd.github.v3.full+json" \
    -H "Content-Type: application/json" \
    -H "${githubauth}" \
    -d @"${tempfile}" \
    --silent --location \
    "${GITHUB_API_URL}/repos/${GITHUB_REPO}/pulls/${GITHUB_ISSUE}/comments" \
    >/dev/null
  rm "${tempfile}"
}

## @description Write the contents of a file to github
## @params filename
## @stability stable
## @audience public
function github_write_comment
{
  declare -r commentfile=${1}
  declare retval=0
  declare restfile="${PATCH_DIR}/ghcomment.$$"
  declare githubauth

  if [[ "${OFFLINE}" == true ]]; then
    echo "Github Plugin: Running in offline, comment skipped."
    return 0
  fi

  {
    printf "{\"body\":\""
    ${SED} -e 's,\\,\\\\,g' \
        -e 's,\",\\\",g' \
        -e 's,$,\\r\\n,g' "${commentfile}" \
    | tr -d '\n'
    echo "\"}"
  } > "${restfile}"

  if [[ -n "${GITHUB_USER}"
     && -n "${GITHUB_PASSWD}" ]]; then
    githubauth="${GITHUB_USER}:${GITHUB_PASSWD}"
  elif [[ -n "${GITHUB_TOKEN}" ]]; then
    githubauth="Authorization: token ${GITHUB_TOKEN}"
  else
    echo "Github Plugin: no credentials provided to write a comment."
    return 0
  fi

  ${CURL} -X POST \
       -H "Accept: application/vnd.github.v3.full+json" \
       -H "Content-Type: application/json" \
       -H "${githubauth}" \
       -d @"${restfile}" \
       --silent --location \
         "${GITHUB_API_URL}/repos/${GITHUB_REPO}/issues/${GITHUB_ISSUE}/comments" \
        >/dev/null

  retval=$?
  rm "${restfile}"
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
  until [[ ${i} -eq ${#TP_HEADER[@]} ]]; do
    printf "%s\n\n" "${TP_HEADER[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done

  {
    printf "\n\n"
    echo "| Vote | Subsystem | Runtime | Comment |"
    echo "|:----:|----------:|--------:|:--------|"
  } >> "${commentfile}"

  i=0
  until [[ ${i} -eq ${#TP_VOTE_TABLE[@]} ]]; do
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
    until [[ ${i} -eq ${#TP_TEST_TABLE[@]} ]]; do
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
