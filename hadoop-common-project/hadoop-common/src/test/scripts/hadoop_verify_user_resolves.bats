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

@test "hadoop_verify_user_resolves (bad: null)" {
  run hadoop_verify_user_resolves
  [ "${status}" = "1" ]
}

@test "hadoop_verify_user_resolves (bad: var string)" {
  run hadoop_verify_user_resolves PrObAbLyWiLlNoTeXiSt
  [ "${status}" = "1" ]
}

@test "hadoop_verify_user_resolves (bad: number as var)" {
  run hadoop_verify_user_resolves 501
  [ "${status}" = "1" ]
}

@test "hadoop_verify_user_resolves (good: name)" {
  myvar=$(id -u -n)
  run hadoop_verify_user_resolves myvar
  [ "${status}" = "0" ]
}

@test "hadoop_verify_user_resolves (skip: number)" {
  skip "id on uids is not platform consistent"
  myvar=1
  run hadoop_verify_user_resolves myvar
  [ "${status}" = "0" ]
}
