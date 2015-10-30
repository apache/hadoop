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

load hadoop-functions_test_helper

@test "hadoop_rotate_log (defaults)" {
  touch "${TMP}/log"
  hadoop_rotate_log "${TMP}/log"
  [ -f "${TMP}/log.1" ]
  [ ! -f "${TMP}/log" ]
}

@test "hadoop_rotate_log (one archive log)" {
  touch "${TMP}/log"
  hadoop_rotate_log "${TMP}/log" 1
  [ -f "${TMP}/log.1" ]
  [ ! -f "${TMP}/log" ]
}

@test "hadoop_rotate_log (default five archive logs)" {
  local i
  for i in {1..5}; do
    echo "Testing ${i}"
    touch "${TMP}/log"
    hadoop_rotate_log "${TMP}/log"
    ls "${TMP}"
    [ -f "${TMP}/log.${i}" ]
  done
}

@test "hadoop_rotate_log (ten archive logs)" {
  local i
  for i in {1..10}; do
    echo "Testing ${i}"
    touch "${TMP}/log"
    hadoop_rotate_log "${TMP}/log" 10
    ls "${TMP}"
    [ -f "${TMP}/log.${i}" ]
  done
}