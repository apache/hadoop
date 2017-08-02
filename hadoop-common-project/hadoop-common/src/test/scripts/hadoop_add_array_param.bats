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

@test "hadoop_add_array_param (empty)" {
  hadoop_add_array_param ARRAY value
  [ "${ARRAY[0]}" = value ]
}

@test "hadoop_add_array_param (exist)" {
  ARRAY=("val2")
  hadoop_add_array_param ARRAY val1
  [ "${ARRAY[0]}" = val2 ]
  [ "${ARRAY[1]}" = val1 ]
}

@test "hadoop_add_array_param (double exist)" {
  ARRAY=("val2" "val1")
  hadoop_add_array_param ARRAY val3
  [ "${ARRAY[0]}" = val2 ]
  [ "${ARRAY[1]}" = val1 ]
  [ "${ARRAY[2]}" = val3 ]
}

