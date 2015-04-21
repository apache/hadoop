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

add_plugin checkstyle

CHECKSTYLE_TIMER=0

# if it ends in an explicit .sh, then this is shell code.
# if it doesn't have an extension, we assume it is shell code too
function checkstyle_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test checkstyle
  fi
}

function checkstyle_preapply
{
  verify_needed_test checkstyle

  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "checkstyle plugin: prepatch"

  start_clock
  echo_and_redirect "${PATCH_DIR}/${PATCH_BRANCH}checkstyle.txt" "${MVN}" test checkstyle:checkstyle-aggregate -DskipTests "-D${PROJECT_NAME}PatchProcess"
  if [[ $? != 0 ]] ; then
    echo "Pre-patch ${PATCH_BRANCH} checkstyle compilation is broken?"
    add_jira_table -1 checkstyle "Pre-patch ${PATCH_BRANCH} checkstyle compilation may be broken."
    return 1
  fi

  cp -p "${BASEDIR}/target/checkstyle-result.xml" \
    "${PATCH_DIR}/checkstyle-result-${PATCH_BRANCH}.xml"

  # keep track of how much as elapsed for us already
  CHECKSTYLE_TIMER=$(stop_clock)
  return 0
}

function checkstyle_postapply
{
  verify_needed_test checkstyle

  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "checkstyle plugin: postpatch"

  start_clock

  # add our previous elapsed to our new timer
  # by setting the clock back
  offset_clock "${CHECKSTYLE_TIMER}"

  echo_and_redirect "${PATCH_DIR}/patchcheckstyle.txt" "${MVN}" test checkstyle:checkstyle-aggregate -DskipTests "-D${PROJECT_NAME}PatchProcess"
  if [[ $? != 0 ]] ; then
    echo "Post-patch checkstyle compilation is broken."
    add_jira_table -1 checkstyle "Post-patch checkstyle compilation is broken."
    return 1
  fi

  cp -p "${BASEDIR}/target/checkstyle-result.xml" \
    "${PATCH_DIR}/checkstyle-result-patch.xml"

  checkstyle_runcomparison

  # shellcheck disable=SC2016
  CHECKSTYLE_POSTPATCH=$(wc -l "${PATCH_DIR}/checkstyle-result-diff.txt" | ${AWK} '{print $1}')

  if [[ ${CHECKSTYLE_POSTPATCH} -gt 0 ]] ; then

    add_jira_table -1 checkstyle "The applied patch generated "\
      "${CHECKSTYLE_POSTPATCH}" \
      " additional checkstyle issues."
    add_jira_footer checkstyle "@@BASE@@/checkstyle-result-diff.txt"

    return 1
  fi
  add_jira_table +1 checkstyle "There were no new checkstyle issues."
  return 0
}


function checkstyle_runcomparison
{

  python <(cat <<EOF
import os
import sys
import xml.etree.ElementTree as etree
from collections import defaultdict

if len(sys.argv) != 3 :
  print "usage: %s checkstyle-result-master.xml checkstyle-result-patch.xml" % sys.argv[0]
  exit(1)

def path_key(x):
  path = x.attrib['name']
  return path[path.find('${PROJECT_NAME}-'):]

def print_row(path, master_errors, patch_errors):
    print '%s\t%s\t%s' % (k,master_dict[k],child_errors)

master = etree.parse(sys.argv[1])
patch = etree.parse(sys.argv[2])

master_dict = defaultdict(int)

for child in master.getroot().getchildren():
    if child.tag != 'file':
        continue
    child_errors = len(child.getchildren())
    if child_errors == 0:
        continue
    master_dict[path_key(child)] = child_errors

for child in patch.getroot().getchildren():
    if child.tag != 'file':
        continue
    child_errors = len(child.getchildren())
    if child_errors == 0:
        continue
    k = path_key(child)
    if child_errors > master_dict[k]:
        print_row(k, master_dict[k], child_errors)

EOF
) "${PATCH_DIR}/checkstyle-result-${PATCH_BRANCH}.xml" "${PATCH_DIR}/checkstyle-result-patch.xml" > "${PATCH_DIR}/checkstyle-result-diff.txt"

}
