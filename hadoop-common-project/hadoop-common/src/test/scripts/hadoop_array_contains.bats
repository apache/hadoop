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

@test "hadoop_array_contains (empty)" {
  run hadoop_array_contains value "${ARRAY[@]}"
  [ "${status}" = 1 ]
}

@test "hadoop_array_contains (exist)" {
  ARRAY=("value")
  run hadoop_array_contains value "${ARRAY[@]}"
  [ "${status}" = 0 ]
}

@test "hadoop_array_contains (notexist)" {
  ARRAY=("different")
  run hadoop_array_contains value "${ARRAY[@]}"
  [ "${status}" = 1 ]
}

@test "hadoop_array_contains (exist, multi)" {
  ARRAY=("val1" "val2" "val3")
  for j in val1 val2 val3; do
    run hadoop_array_contains "${j}" "${ARRAY[@]}"
    [ "${status}" = 0 ]
  done
}

@test "hadoop_array_contains (multi, not exist)" {
  ARRAY=("val1" "val2" "val3")
  run hadoop_array_contains value "${ARRAY[@]}"
  [ "${status}" = 1 ]
}
