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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target"}
mkdir -p "$REPORT_DIR"

REPORT_FILE="$REPORT_DIR/summary.txt"

cd hadoop-hdds || exit 1
mvn -B -fn org.apache.rat:apache-rat-plugin:0.13:check
cd ../hadoop-ozone || exit 1
mvn -B -fn org.apache.rat:apache-rat-plugin:0.13:check

cd "$DIR/../../.." || exit 1

grep -r --include=rat.txt "!????" hadoop-hdds hadoop-ozone | tee "$REPORT_FILE"

wc -l "$REPORT_FILE" | awk '{print $1}'> "$REPORT_DIR/failures"

if [[ -s "${REPORT_FILE}" ]]; then
   exit 1
fi

