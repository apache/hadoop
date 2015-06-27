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

add_bugsystem jira

function jira_usage
{
  echo "JIRA Options:"
  echo "--jira-cmd=<cmd>       The 'jira' command to use (default 'jira')"
  echo "--jira-password=<pw>   The password for the 'jira' command"
  echo "--jira-user=<user>     The user for the 'jira' command"
}

function jira_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
      --jira-cmd=*)
        JIRACLI=${i#*=}
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

## @description Write the contents of a file to JIRA
## @params filename
## @stability stable
## @audience public
## @returns ${JIRACLI} exit code
function jira_write_comment
{
  local -r commentfile=${1}
  shift

  local retval=0


  if [[ -n ${JIRA_PASSWD}
     && -n ${JIRA_USER} ]]; then
    # shellcheck disable=SC2086
    ${JIRACLI} --comment "$(cat ${commentfile})" \
               -s https://issues.apache.org/jira \
               -a addcomment -u ${JIRA_USER} \
               -p "${JIRA_PASSWD}" \
               --issue "${ISSUE}"
    retval=$?
    ${JIRACLI} -s https://issues.apache.org/jira \
               -a logout -u "${JIRA_USER}" \
               -p "${JIRA_PASSWD}"
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
  local result=$1
  local i
  local commentfile=${PATCH_DIR}/commentfile
  local comment
  local vote
  local ourstring
  local ela
  local subs
  local color
  local comment

  rm "${commentfile}" 2>/dev/null

  if [[ ${JENKINS} != "true" ]] ; then
    return 0
  fi

  big_console_header "Adding comment to JIRA"

  add_footer_table "Console output" "${BUILD_URL}console"

  if [[ ${result} == 0 ]]; then
    add_header_line "| (/) *{color:green}+1 overall{color}* |"
  else
    add_header_line "| (x) *{color:red}-1 overall{color}* |"
  fi

  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

  i=0
  until [[ $i -eq ${#TP_HEADER[@]} ]]; do
    printf "%s\n" "${TP_HEADER[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done

  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

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
