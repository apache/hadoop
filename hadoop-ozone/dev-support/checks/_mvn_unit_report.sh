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

REPORT_DIR=${REPORT_DIR:-$PWD}

_realpath() {
  if realpath "$@" > /dev/null; then
    realpath "$@"
  else
    local relative_to
    relative_to=$(realpath "${1/--relative-to=/}") || return 1
    realpath "$2" | sed -e "s@${relative_to}/@@"
  fi
}

## generate summary txt file
find "." -name 'TEST*.xml' -print0 \
    | xargs -n1 -0 "grep" -l -E "<failure|<error" \
    | awk -F/ '{sub("'"TEST-"'",""); sub(".xml",""); print $NF}' \
    | tee "$REPORT_DIR/summary.txt"

#Copy heap dump and dump leftovers
find "." -name "*.hprof" \
    -or -name "*.dump" \
    -or -name "*.dumpstream" \
    -or -name "hs_err_*.log" \
  -exec cp {} "$REPORT_DIR/" \;

## Add the tests where the JVM is crashed
grep -A1 'Crashed tests' "${REPORT_DIR}/output.log" \
  | grep -v -e 'Crashed tests' -e '--' \
  | cut -f2- -d' ' \
  | sort -u >> "${REPORT_DIR}/summary.txt"

## Check if Maven was killed
if grep -q 'Killed.* mvn .* test ' "${REPORT_DIR}/output.log"; then
  echo 'Maven test run was killed' >> "${REPORT_DIR}/summary.txt"
fi

#Collect of all of the report failes of FAILED tests
while IFS= read -r -d '' dir; do
   while IFS=$'\n' read -r file; do
      DIR_OF_TESTFILE=$(dirname "$file")
      NAME_OF_TESTFILE=$(basename "$file")
      NAME_OF_TEST="${NAME_OF_TESTFILE%.*}"
      DESTDIRNAME=$(_realpath --relative-to="$PWD" "$DIR_OF_TESTFILE/../..") || continue
      mkdir -p "$REPORT_DIR/$DESTDIRNAME"
      #shellcheck disable=SC2086
      cp -r "$DIR_OF_TESTFILE"/*$NAME_OF_TEST* "$REPORT_DIR/$DESTDIRNAME/"
   done < <(grep -l -r FAILURE --include="*.txt" "$dir" | grep -v output.txt)
done < <(find "." -name surefire-reports -print0)

## generate summary markdown file
export SUMMARY_FILE="$REPORT_DIR/summary.md"
for TEST_RESULT_FILE in $(find "$REPORT_DIR" -name "*.txt" | grep -v output); do

    FAILURES=$(grep FAILURE "$TEST_RESULT_FILE" | grep "Tests run" | awk '{print $18}' | sort | uniq)

    for FAILURE in $FAILURES; do
        TEST_RESULT_LOCATION="$(_realpath --relative-to="$REPORT_DIR" "$TEST_RESULT_FILE")"
        TEST_OUTPUT_LOCATION="${TEST_RESULT_LOCATION//.txt/-output.txt}"
        printf " * [%s](%s) ([output](%s))\n" "$FAILURE" "$TEST_RESULT_LOCATION" "$TEST_OUTPUT_LOCATION" >> "$SUMMARY_FILE"
    done
done

if [ -s "$SUMMARY_FILE" ]; then
   printf "# Failing tests: \n\n" | cat - "$SUMMARY_FILE" > temp && mv temp "$SUMMARY_FILE"
fi

## generate counter
wc -l "$REPORT_DIR/summary.txt" | awk '{print $1}'> "$REPORT_DIR/failures"
