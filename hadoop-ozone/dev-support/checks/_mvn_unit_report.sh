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

## generate summary txt file
find "." -name 'TEST*.xml' -print0 \
    | xargs -n1 -0 "grep" -l -E "<failure|<error" \
    | awk -F/ '{sub("'"TEST-JUNIT_TEST_OUTPUT_DIR"'",""); sub(".xml",""); print $NF}' \
    | tee "$REPORT_DIR/summary.txt"


#Collect of all of the FAILED results
for dir in $(find "." -name surefire-reports); do
   for file in $(grep -l -r FAILURE --include="*.txt" $dir | grep -v output.txt ); do
      DIR_OF_TESTFILE=$(dirname $file)
      NAME_OF_TESTFILE=$(basename $file)
      NAME_OF_TEST="${NAME_OF_TESTFILE%.*}"
      DESTDIRNAME=$(realpath --relative-to="$PWD" $DIR_OF_TESTFILE/../..)
      mkdir -p "$REPORT_DIR/$DESTDIRNAME"
      cp -r "$DIR_OF_TESTFILE/"*$NAME_OF_TEST* "$REPORT_DIR/$DESTDIRNAME/"
   done
done

## generate summary markdown file
export SUMMARY_FILE="$REPORT_DIR/summary.md"
printf "   Failing tests: \n\n" > "$SUMMARY_FILE"
for TEST_RESULT_FILE in $(find "$REPORT_DIR" -name "*.txt" | grep -v output); do

    FAILURES=$(cat $TEST_RESULT_FILE | grep FAILURE | grep "Tests run" | awk '{print $18}' | sort | uniq)

    for FAILURE in $FAILURES; do
        printf "      $FAILURE\n" >> "$SUMMARY_FILE"
        TEST_RESULT_LOCATION=$(realpath --relative-to=$REPORT_DIR $TEST_RESULT_FILE)
        printf "            (result)[./$TEST_RESULT_LOCATION]\n\n" >> "$SUMMARY_FILE"
    done
done
printf "\n\n" >> "$SUMMARY_FILE"

## generate counter
wc -l "$REPORT_DIR/summary.txt" | awk '{print $1}'> "$REPORT_DIR/failures"
