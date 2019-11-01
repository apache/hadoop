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

BASE_DIR="$(pwd -P)"
REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/checkstyle"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/summary.txt"

mvn -B -fn checkstyle:check -f pom.ozone.xml

#Print out the exact violations with parsing XML results with sed
find "." -name checkstyle-errors.xml -print0 \
  | xargs -0 sed '$!N; /<file.*\n<\/file/d;P;D' \
  | sed \
      -e '/<\?xml.*>/d' \
      -e '/<checkstyle.*/d' \
      -e '/<\/.*/d' \
      -e 's/<file name="\([^"]*\)".*/\1/' \
      -e 's/<error.*line="\([[:digit:]]*\)".*message="\([^"]*\)".*/ \1: \2/' \
      -e "s!^${BASE_DIR}/!!" \
  | tee "$REPORT_FILE"

## generate counter
grep -c ':' "$REPORT_FILE" > "$REPORT_DIR/failures"

if [[ -s "${REPORT_FILE}" ]]; then
   exit 1
fi
