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

@test "hadoop_sort_array (empty)" {
  hadoop_sort_array ARRAY
}

@test "hadoop_sort_array (single value)" {
  ARRAY=("value")
  hadoop_sort_array ARRAY
}

@test "hadoop_sort_array (multiple value)" {
  ARRAY=("b" "c" "a")
  preifsod=$(echo "${IFS}" | od -c)
  hadoop_sort_array ARRAY
  postifsod=$(echo "${IFS}" | od -c)

  [ "${ARRAY[0]}" = "a" ]
  [ "${ARRAY[1]}" = "b" ]
  [ "${ARRAY[2]}" = "c" ]
  [ "${preifsod}" = "${postifsod}" ]
}
