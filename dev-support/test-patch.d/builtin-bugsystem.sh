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


# This bug system handles the output on the screen.

add_bugsystem console

# we always call this one last

function generic_locate_patch
{
  declare input=$1
  declare output=$2

  if [[ "${OFFLINE}" == true ]]; then
    yetus_debug "generic_locate_patch: offline, skipping"
    return 1
  fi

  ${CURL} --silent \
          --output "${output}" \
         "${input}"
  if [[ $? != 0 ]]; then
    yetus_debug "jira_locate_patch: not a JIRA."
    return 1
  fi
  return 0
}

## @description  Print out the finished details on the console
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
## @return       0 on success
## @return       1 on failure
function console_finalreport
{
  declare result=$1
  shift
  declare i=0
  declare ourstring
  declare vote
  declare subs
  declare ela
  declare comment
  declare commentfile1="${PATCH_DIR}/comment.1"
  declare commentfile2="${PATCH_DIR}/comment.2"
  declare normaltop
  declare line
  declare seccoladj=0
  declare spcfx=${PATCH_DIR}/spcl.txt

  if [[ ${result} == 0 ]]; then
    if [[ ${JENKINS} == false ]]; then
      {
        printf "IF9fX19fX19fX18gCjwgU3VjY2VzcyEgPgogLS0tLS0tLS0tLSAKIFwgICAg";
        printf "IC9cICBfX18gIC9cCiAgXCAgIC8vIFwvICAgXC8gXFwKICAgICAoKCAgICBP";
        printf "IE8gICAgKSkKICAgICAgXFwgLyAgICAgXCAvLwogICAgICAgXC8gIHwgfCAg";
        printf "XC8gCiAgICAgICAgfCAgfCB8ICB8ICAKICAgICAgICB8ICB8IHwgIHwgIAog";
        printf "ICAgICAgIHwgICBvICAgfCAgCiAgICAgICAgfCB8ICAgfCB8ICAKICAgICAg";
        printf "ICB8bXwgICB8bXwgIAo"
      } > "${spcfx}"
    fi
    printf "\n\n+1 overall\n\n"
  else
    if [[ ${JENKINS} == false ]]; then
      {
        printf "IF9fX19fICAgICBfIF8gICAgICAgICAgICAgICAgXyAKfCAgX19ffF8gXyhf";
        printf "KSB8XyAgIF8gXyBfXyBfX198IHwKfCB8XyAvIF9gIHwgfCB8IHwgfCB8ICdf";
        printf "Xy8gXyBcIHwKfCAgX3wgKF98IHwgfCB8IHxffCB8IHwgfCAgX18vX3wKfF98";
        printf "ICBcX18sX3xffF98XF9fLF98X3wgIFxfX18oXykKICAgICAgICAgICAgICAg";
        printf "ICAgICAgICAgICAgICAgICAK"
      } > "${spcfx}"
    fi
    printf "\n\n-1 overall\n\n"
  fi

  if [[ -f ${spcfx} ]]; then
    if which base64 >/dev/null 2>&1; then
      base64 --decode "${spcfx}" 2>/dev/null
    elif which openssl >/dev/null 2>&1; then
      openssl enc -A -d -base64 -in "${spcfx}" 2>/dev/null
    fi
    echo
    echo
    rm "${spcfx}"
  fi

  seccoladj=$(findlargest 2 "${TP_VOTE_TABLE[@]}")
  if [[ ${seccoladj} -lt 10 ]]; then
    seccoladj=10
  fi

  seccoladj=$((seccoladj + 2 ))
  i=0
  until [[ $i -eq ${#TP_HEADER[@]} ]]; do
    printf "%s\n" "${TP_HEADER[${i}]}"
    ((i=i+1))
  done

  printf "| %s | %*s |  %s   | %s\n" "Vote" ${seccoladj} Subsystem Runtime "Comment"
  echo "============================================================================"
  i=0
  until [[ $i -eq ${#TP_VOTE_TABLE[@]} ]]; do
    ourstring=$(echo "${TP_VOTE_TABLE[${i}]}" | tr -s ' ')
    vote=$(echo "${ourstring}" | cut -f2 -d\|)
    subs=$(echo "${ourstring}"  | cut -f3 -d\|)
    ela=$(echo "${ourstring}" | cut -f4 -d\|)
    comment=$(echo "${ourstring}"  | cut -f5 -d\|)

    echo "${comment}" | fold -s -w $((78-seccoladj-22)) > "${commentfile1}"
    normaltop=$(head -1 "${commentfile1}")
    ${SED} -e '1d' "${commentfile1}"  > "${commentfile2}"

    printf "| %4s | %*s | %-10s |%-s\n" "${vote}" ${seccoladj} \
      "${subs}" "${ela}" "${normaltop}"
    while read line; do
      printf "|      | %*s |            | %-s\n" ${seccoladj} " " "${line}"
    done < "${commentfile2}"

    ((i=i+1))
    rm "${commentfile2}" "${commentfile1}" 2>/dev/null
  done

  if [[ ${#TP_TEST_TABLE[@]} -gt 0 ]]; then
    seccoladj=$(findlargest 1 "${TP_TEST_TABLE[@]}")
    printf "\n\n%*s | Tests\n" "${seccoladj}" "Reason"
    i=0
    until [[ $i -eq ${#TP_TEST_TABLE[@]} ]]; do
      ourstring=$(echo "${TP_TEST_TABLE[${i}]}" | tr -s ' ')
      vote=$(echo "${ourstring}" | cut -f2 -d\|)
      subs=$(echo "${ourstring}"  | cut -f3 -d\|)
      printf "%*s | %s\n" "${seccoladj}" "${vote}" "${subs}"
      ((i=i+1))
    done
  fi

  printf "\n\n|| Subsystem || Report/Notes ||\n"
  echo "============================================================================"
  i=0

  until [[ $i -eq ${#TP_FOOTER_TABLE[@]} ]]; do
    comment=$(echo "${TP_FOOTER_TABLE[${i}]}" |
              ${SED} -e "s,@@BASE@@,${PATCH_DIR},g")
    printf "%s\n" "${comment}"
    ((i=i+1))
  done
}
