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

JIRACLI=${JIRA:-jira}
JIRA_URL=${JIRA_URL:-"https://issues.apache.org/jira"}
JIRA_ISSUE_RE='^(YETUS)-[0-9]+$'

add_bugsystem jira

function jira_usage
{
  echo "JIRA Options:"
  echo "--jira-issue-re=<expr> Bash regular expression to use when trying to find a jira ref in the patch name (default: \'${JIRA_ISSUE_RE}\')"
  echo "--jira-cmd=<cmd>       The 'jira' command to use (default '${JIRACLI}')"
  echo "--jira-password=<pw>   The password for the 'jira' command"
  echo "--jira-base-url=<url>  The URL of the JIRA server (default:'${JIRA_URL}')"
  echo "--jira-user=<user>     The user for the 'jira' command"
}

function jira_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --jira-cmd=*)
        JIRACLI=${i#*=}
      ;;
      --jira-base-url=*)
        JIRA_URL=${i#*=}
      ;;
      --jira-issue-re=*)
        JIRA_ISSUE_RE=${i#*=}
      ;;
      --jira-password=*)
        JIRA_PASSWD=${i#*=}
      ;;
      --jira-user=*)
        JIRA_USER=${i#*=}
      ;;
    esac
  done
}

function jira_determine_issue
{
  declare input=$1
  declare patchnamechunk
  declare maybeissue

  # shellcheck disable=SC2016
  patchnamechunk=$(echo "${input}" | ${AWK} -F/ '{print $NF}')

  maybeissue=$(echo "${patchnamechunk}" | cut -f1,2 -d-)

  if [[ ${maybeissue} =~ ${JIRA_ISSUE_RE} ]]; then
    ISSUE=${maybeissue}
    add_footer_table "JIRA Issue" "${ISSUE}"
    return 0
  fi

  return 1
}

function jira_http_fetch
{
  declare input=$1
  declare output=$2

  if [[ -n "${JIRA_USER}"
     && -n "${JIRA_PASSWD}" ]]; then
     ${CURL} --silent --fail \
             --user "${JIRA_USER}:${JIRA_PASSWD}" \
             --output "${output}" \
             --location \
            "${JIRA_URL}/${input}"
  else
    ${CURL} --silent --fail \
            --output "${output}" \
            --location \
           "${JIRA_URL}/${input}"
  fi
}

function jira_locate_patch
{
  declare input=$1
  declare fileloc=$2

  yetus_debug "jira_locate_patch: trying ${JIRA_URL}/browse/${input}"

  if [[ "${OFFLINE}" == true ]]; then
    yetus_debug "jira_locate_patch: offline, skipping"
    return 1
  fi

  jira_http_fetch "browse/${input}" "${PATCH_DIR}/jira"

  if [[ $? != 0 ]]; then
    yetus_debug "jira_locate_patch: not a JIRA."
    return 1
  fi

  # TODO: we should check for a gitbub-based pull request here
  # if we find one, call the github plug-in directly with the
  # appropriate bits so that it gets setup to write a comment
  # to the PR

  if [[ $(${GREP} -c 'Patch Available' "${PATCH_DIR}/jira") == 0 ]] ; then
    if [[ ${JENKINS} == true ]]; then
      yetus_error "ERROR: ${input} is not \"Patch Available\"."
      cleanup_and_exit 1
    else
      yetus_error "WARNING: ${input} is not \"Patch Available\"."
    fi
  fi

  #shellcheck disable=SC2016
  relativePatchURL=$(${AWK} 'match($0,"/secure/attachment/[0-9]*/[^\"]*"){print substr($0,RSTART,RLENGTH)}' "${PATCH_DIR}/jira" |
    ${GREP} -v -e 'htm[l]*$' | sort | tail -1 | ${SED} -e 's,[ ]*$,,g')
  PATCHURL="${JIRA_URL}${relativePatchURL}"
  if [[ ! ${PATCHURL} =~ \.patch$ ]]; then
    guess_patch_file "${PATCH_DIR}/patch"
    if [[ $? == 0 ]]; then
      yetus_debug "The patch ${PATCHURL} was not named properly, but it looks like a patch file. proceeding, but issue/branch matching might go awry."
      add_vote_table 0 patch "The patch file was not named according to ${PROJECT_NAME}'s naming conventions. Please see ${HOW_TO_CONTRIBUTE} for instructions."
    fi
  fi
  echo "${ISSUE} patch is being downloaded at $(date) from"
  echo "${PATCHURL}"
  add_footer_table "JIRA Patch URL" "${PATCHURL}"
  jira_http_fetch "${relativePatchURL}" "${fileloc}"
  if [[ $? != 0 ]];then
    yetus_error "ERROR: ${PATCH_OR_ISSUE} could not be downloaded."
    cleanup_and_exit 1
  fi
  return 0
}

## @description Write the contents of a file to JIRA
## @params filename
## @stability stable
## @audience public
## @returns ${JIRACLI} exit code
function jira_write_comment
{
  declare -r commentfile=${1}
  shift

  declare retval=0

  if [[ "${OFFLINE}" == true ]]; then
    return 0
  fi

  if [[ -n ${JIRA_PASSWD}
     && -n ${JIRA_USER} ]]; then

    echo "{\"body\":\"" > "${PATCH_DIR}/jiracomment.$$"
    sed -e 's,\\,\\\\,g' \
        -e 's,\",\\\",g' \
        -e 's,$,\\r\\n,g' "${commentfile}" \
    | tr -d '\n'>> "${PATCH_DIR}/jiracomment.$$"
    echo "\"}" >> "${PATCH_DIR}/jiracomment.$$"

    ${CURL} -X POST \
         -H "Accept: application/json" \
         -H "Content-Type: application/json" \
         -u "${JIRA_USER}:${JIRA_PASSWD}" \
         -d @"${PATCH_DIR}/jiracomment.$$" \
         --silent --location \
           "${JIRA_URL}/rest/api/2/issue/${ISSUE}/comment" \
          >/dev/null
    retval=$?
    #rm "${PATCH_DIR}/jiracomment.$$"
  fi
  return ${retval}
}

## @description  Print out the finished details to the JIRA issue
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
function jira_finalreport
{
  declare result=$1
  declare i
  declare commentfile=${PATCH_DIR}/jiracommentfile
  declare comment
  declare vote
  declare ourstring
  declare ela
  declare subs
  declare color
  declare comment

  rm "${commentfile}" 2>/dev/null

  if [[ ${JENKINS} != "true"
      || ${OFFLINE} == true ]] ; then
    return 0
  fi

  big_console_header "Adding comment to JIRA"

  add_footer_table "Console output" "${BUILD_URL}console"

  if [[ ${result} == 0 ]]; then
    echo "| (/) *{color:green}+1 overall{color}* |" >> ${commentfile}
  else
    echo "| (x) *{color:red}-1 overall{color}* |" >> ${commentfile}
  fi

  echo "\\\\" >>  "${commentfile}"

  i=0
  until [[ $i -eq ${#TP_HEADER[@]} ]]; do
    printf "%s\n" "${TP_HEADER[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done

  echo "\\\\" >>  "${commentfile}"

  echo "|| Vote || Subsystem || Runtime || Comment ||" >> "${commentfile}"

  i=0
  until [[ $i -eq ${#TP_VOTE_TABLE[@]} ]]; do
    ourstring=$(echo "${TP_VOTE_TABLE[${i}]}" | tr -s ' ')
    vote=$(echo "${ourstring}" | cut -f2 -d\| | tr -d ' ')
    subs=$(echo "${ourstring}"  | cut -f3 -d\|)
    ela=$(echo "${ourstring}" | cut -f4 -d\|)
    comment=$(echo "${ourstring}"  | cut -f5 -d\|)

    # summary line
    if [[ -z ${vote}
      && -n ${ela} ]]; then
      color="black"
    elif [[ -z ${vote} ]]; then
      # keep same color
      true
    else
      # new vote line
      case ${vote} in
        1|"+1")
          color="green"
        ;;
        -1)
          color="red"
        ;;
        0)
          color="blue"
        ;;
        *)
          color="black"
        ;;
      esac
    fi

    printf "| {color:%s}%s{color} | {color:%s}%s{color} | {color:%s}%s{color} | {color:%s}%s{color} |\n" \
      "${color}" "${vote}" \
      "${color}" "${subs}" \
      "${color}" "${ela}" \
      "${color}" "${comment}" \
      >> "${commentfile}"
    ((i=i+1))
  done

  if [[ ${#TP_TEST_TABLE[@]} -gt 0 ]]; then
    { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

    echo "|| Reason || Tests ||" >>  "${commentfile}"
    i=0
    until [[ $i -eq ${#TP_TEST_TABLE[@]} ]]; do
      printf "%s\n" "${TP_TEST_TABLE[${i}]}" >> "${commentfile}"
      ((i=i+1))
    done
  fi

  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

  echo "|| Subsystem || Report/Notes ||" >> "${commentfile}"
  i=0
  until [[ $i -eq ${#TP_FOOTER_TABLE[@]} ]]; do
    comment=$(echo "${TP_FOOTER_TABLE[${i}]}" |
              ${SED} -e "s,@@BASE@@,${BUILD_URL}artifact/patchprocess,g")
    printf "%s\n" "${comment}" >> "${commentfile}"
    ((i=i+1))
  done

  printf "\n\nThis message was automatically generated.\n\n" >> "${commentfile}"

  jira_write_comment "${commentfile}"
}
