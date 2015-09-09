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

# this bug system handles JIRA.  Personalities
# can override the following variables:

# base JIRA URL
JIRA_URL=${JIRA_URL:-"https://issues.apache.org/jira"}

# Issue regex to help identify the project
JIRA_ISSUE_RE=''

add_bugsystem jira

function jira_usage
{
  echo "JIRA Options:"
  echo "--jira-issue-re=<expr>  Bash regular expression to use when trying to find a jira ref in the patch name (default: \'${JIRA_ISSUE_RE}\')"
  echo "--jira-password=<pw>    The password for the 'jira' command"
  echo "--jira-base-url=<url>   The URL of the JIRA server (default:'${JIRA_URL}')"
  echo "--jira-user=<user>      The user for the 'jira' command"
}

function jira_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
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

## @description provides issue determination based upon the URL and more.
## @description WARNING: called from the github plugin!
function jira_determine_issue
{
  declare input=$1
  declare patchnamechunk
  declare maybeissue

  if [[ -n ${JIRA_ISSUE} ]]; then
    return 0
  fi

  # shellcheck disable=SC2016
  patchnamechunk=$(echo "${input}" | ${AWK} -F/ '{print $NF}')

  maybeissue=$(echo "${patchnamechunk}" | cut -f1,2 -d-)

  if [[ ${maybeissue} =~ ${JIRA_ISSUE_RE} ]]; then
    # shellcheck disable=SC2034
    ISSUE=${maybeissue}
    JIRA_ISSUE=${maybeissue}
    add_footer_table "JIRA Issue" "${JIRA_ISSUE}"
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
  declare relativeurl

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

  # if github is configured and we see what looks like a URL,
  # send this to the github plugin to process.
  if [[ -n "${GITHUB_BASE_URL}"
      && $(${GREP} -c  "${GITHUB_BASE_URL}"'[^ ]*patch' "${PATCH_DIR}/jira") != 0 ]]; then
    jira_determine_issue "${input}"
    echo "${input} appears to be a Github PR. Switching Modes."
    github_jira_bridge "${fileloc}"
    return $?
  elif [[ $(${GREP} -c 'Patch Available' "${PATCH_DIR}/jira") == 0 ]]; then
    if [[ ${JENKINS} == true ]]; then
      yetus_error "ERROR: ${input} is not \"Patch Available\"."
      cleanup_and_exit 1
    else
      yetus_error "WARNING: ${input} is not \"Patch Available\"."
    fi
  fi

  #shellcheck disable=SC2016
  relativeurl=$(${AWK} 'match($0,"/secure/attachment/[0-9]*/[^\"]*"){print substr($0,RSTART,RLENGTH)}' "${PATCH_DIR}/jira" |
    ${GREP} -v -e 'htm[l]*$' | sort | tail -1 | ${SED} -e 's,[ ]*$,,g')
  PATCHURL="${JIRA_URL}${relativeurl}"
  if [[ ! ${PATCHURL} =~ \.patch$ ]]; then
    guess_patch_file "${PATCH_DIR}/patch"
    if [[ $? == 0 ]]; then
      yetus_debug "The patch ${PATCHURL} was not named properly, but it looks like a patch file. Proceeding, but issue/branch matching might go awry."
      add_vote_table 0 patch "The patch file was not named according to ${PROJECT_NAME}'s naming conventions. Please see ${HOW_TO_CONTRIBUTE} for instructions."
    fi
  fi
  echo "${input} patch is being downloaded at $(date) from"
  echo "${PATCHURL}"
  add_footer_table "JIRA Patch URL" "${PATCHURL}"
  jira_http_fetch "${relativeurl}" "${fileloc}"
  if [[ $? != 0 ]];then
    yetus_error "ERROR: ${input}/${PATCHURL} could not be downloaded."
    cleanup_and_exit 1
  fi
  return 0
}

## @description  Try to guess the branch being tested using a variety of heuristics
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success, with PATCH_BRANCH updated appropriately
function jira_determine_branch
{
  declare patchnamechunk
  declare total
  declare count
  declare hinttype

  for hinttype in "${PATCHURL}" "${PATCH_OR_ISSUE}"; do
    if [[ -z "${hinttype}" ]]; then
      continue
    fi

    # If one of these matches the JIRA issue regex
    # then we don't want it to trigger the branch
    # detection since that's almost certainly not
    # intended.  In other words, if ISSUE-99 is the
    # name of a branch, you want to test ISSUE-99
    # against master, not ISSUE-99's branch
    if [[ ${hinttype} =~ ${JIRA_ISSUE_RE} ]]; then
      continue
    fi

    yetus_debug "Determine branch: starting with ${hinttype}"
    patchnamechunk=$(echo "${hinttype}" \
            | ${SED} -e 's,.*/\(.*\)$,\1,' \
                     -e 's,\.txt,.,' \
                     -e 's,.patch,.,g' \
                     -e 's,.diff,.,g' \
                     -e 's,\.\.,.,g' \
                     -e 's,\.$,,g' )

    # ISSUE-branch-##
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1,2 -d-)
    yetus_debug "Determine branch: ISSUE-branch-## = ${PATCH_BRANCH}"
    if [[ -n "${PATCH_BRANCH}" ]]; then
      verify_valid_branch  "${PATCH_BRANCH}"
      if [[ $? == 0 ]]; then
        return 0
      fi
    fi

    # ISSUE-##[.##].branch
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d. )
    count="${PATCH_BRANCH//[^.]}"
    total=${#count}
    ((total = total + 3 ))
    until [[ ${total} -lt 2 ]]; do
      PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3-${total} -d.)
      yetus_debug "Determine branch: ISSUE[.##].branch = ${PATCH_BRANCH}"
      ((total=total-1))
      if [[ -n "${PATCH_BRANCH}" ]]; then
        verify_valid_branch  "${PATCH_BRANCH}"
        if [[ $? == 0 ]]; then
          return 0
        fi
      fi
    done

    # ISSUE.branch.##
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f2- -d. )
    count="${PATCH_BRANCH//[^.]}"
    total=${#count}
    ((total = total + 3 ))
    until [[ ${total} -lt 2 ]]; do
      PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f2-${total} -d.)
      yetus_debug "Determine branch: ISSUE.branch[.##] = ${PATCH_BRANCH}"
      ((total=total-1))
      if [[ -n "${PATCH_BRANCH}" ]]; then
        verify_valid_branch  "${PATCH_BRANCH}"
        if [[ $? == 0 ]]; then
          return 0
        fi
      fi
    done

    # ISSUE-branch.##
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1- -d. )
    count="${PATCH_BRANCH//[^.]}"
    total=${#count}
    ((total = total + 1 ))
    until [[ ${total} -lt 1 ]]; do
      PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1-${total} -d. )
      yetus_debug "Determine branch: ISSUE-branch[.##] = ${PATCH_BRANCH}"
      ((total=total-1))
      if [[ -n "${PATCH_BRANCH}" ]]; then
        verify_valid_branch  "${PATCH_BRANCH}"
        if [[ $? == 0 ]]; then
          return 0
        fi
      fi
    done
  done

  return 1
}

## @description Write the contents of a file to JIRA
## @params filename
## @stability stable
## @audience public
## @returns exit code from posting to jira
function jira_write_comment
{
  declare -r commentfile=${1}
  declare retval=0

  if [[ "${OFFLINE}" == true ]]; then
    echo "JIRA Plugin: Running in offline, comment skipped."
    return 0
  fi

  if [[ -n ${JIRA_PASSWD}
     && -n ${JIRA_USER} ]]; then

    # RESTify the comment
    {
      echo "{\"body\":\""
      ${SED} -e 's,\\,\\\\,g' \
          -e 's,\",\\\",g' \
          -e 's,$,\\r\\n,g' "${commentfile}" \
      | tr -d '\n'
      echo "\"}"
    } > "${PATCH_DIR}/jiracomment.$$"

    ${CURL} -X POST \
         -H "Accept: application/json" \
         -H "Content-Type: application/json" \
         -u "${JIRA_USER}:${JIRA_PASSWD}" \
         -d @"${PATCH_DIR}/jiracomment.$$" \
         --silent --location \
           "${JIRA_URL}/rest/api/2/issue/${JIRA_ISSUE}/comment" \
          >/dev/null
    retval=$?
    rm "${PATCH_DIR}/jiracomment.$$"
  else
    echo "JIRA Plugin: no credentials provided to write a comment."
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

  if [[ ${JENKINS} == "false"
      || ${OFFLINE} == true ]] ; then
    return 0
  fi

  if [[ -z "${JIRA_ISSUE}" ]]; then
    return 0
  fi

  big_console_header "Adding comment to JIRA"

  add_footer_table "Console output" "${BUILD_URL}console"

  if [[ ${result} == 0 ]]; then
    echo "| (/) *{color:green}+1 overall{color}* |" >> "${commentfile}"
  else
    echo "| (x) *{color:red}-1 overall{color}* |" >> "${commentfile}"
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
