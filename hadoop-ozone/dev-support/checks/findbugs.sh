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

FINDBUGS_ALL_FILE=./target/findbugs-all.txt

mkdir -p ./target
rm "$FINDBUGS_ALL_FILE" || true
touch "$FINDBUGS_ALL_FILE"

mvn -fn findbugs:check -Dfindbugs.failOnError=false  -am -pl :hadoop-ozone-dist -Phdds

find hadoop-ozone -name findbugsXml.xml | xargs -n1 convertXmlToText | tee -a "${FINDBUGS_ALL_FILE}"
find hadoop-hdds -name findbugsXml.xml | xargs -n1 convertXmlToText | tee -a "${FINDBUGS_ALL_FILE}"

bugs=$(cat "$FINDBUGS_ALL_FILE" | wc -l)

if [[ ${bugs} -gt 0 ]]; then
   exit -1
else
   exit 0
fi
