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

targetdir=../../../target
mkdir -p ${targetdir}/surefire-reports ${targetdir}/tap

batsexe=$(which bats) 2>/dev/null

if [[ -z ${batsexe} ]]; then
  echo "not ok - no bats executable found" >  "${targetdir}/tap/shelltest.tap"
  echo ""
  echo ""
  echo "ERROR: bats not installed. Skipping bash tests."
  echo "ERROR: Please install bats as soon as possible."
  echo ""
  echo ""
  exit 0
fi

for j in *.bats; do
  echo Running bats -t "${j}"
  bats -t "${j}" 2>&1 | tee "${targetdir}/tap/${j}.tap"
  result=${PIPESTATUS[0]}
  ((exitcode=exitcode+result))
done

if [[ ${exitcode} -gt 0 ]]; then
  exit 1
fi
exit 0
