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

add_plugin xml

function xml_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.xml$ ]]; then
    add_test xml
  fi
}

function xml_postapply
{
  local js
  local i
  local count

  verify_needed_test xml
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "Checking if XML files are well-formed"

  js="${JAVA_HOME}/bin/jrunscript"
  if [[ ! -x ${js} ]]; then
    yetus_error "${js} does not exist"
    return 0
  fi

  start_clock

  pushd "${BASEDIR}" >/dev/null
  for i in ${CHANGED_FILES}; do
    if [[ ! ${i} =~ \.xml$ ]]; then
      continue
    fi
    ${js} -e "XMLDocument(arguments[0])" "${i}" >> "${PATCH_DIR}/xml.txt" 2>&1
    if [[ $? != 0 ]]; then
      ((count=count+1))
    fi
  done

  if [[ ${count} -gt 0 ]]; then
    add_vote_table -1 xml "The patch has ${count} ill-formed XML file(s)."
    add_footer_table xml "@@BASE@@/xml.txt"
    popd >/dev/null
    return 1
  fi

  popd >/dev/null
  add_vote_table +1 xml "The patch has no ill-formed XML file."
  return 0
}
