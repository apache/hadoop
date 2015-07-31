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

@test "hadoop_java_setup (negative not set)" {
  unset JAVA_HOME
  run hadoop_java_setup
  [ "${status}" -eq 1 ]
}

@test "hadoop_java_setup (negative not a dir)" {
  touch ${TMP}/foo
  JAVA_HOME="${TMP}/foo"
  run hadoop_java_setup
  [ "${status}" -eq 1 ]
}

@test "hadoop_java_setup (negative not exec)" {
  mkdir -p "${TMP}/bin"
  touch "${TMP}/bin/java"
  JAVA_HOME="${TMP}"
  chmod a-x "${TMP}/bin/java"
  run hadoop_java_setup
  [ "${status}" -eq 1 ]
}

@test "hadoop_java_setup (positive)" {
  mkdir -p "${TMP}/bin"
  touch "${TMP}/bin/java"
  JAVA_HOME="${TMP}"
  chmod a+x "${TMP}/bin/java"
  run hadoop_java_setup
  [ "${status}" -eq 0 ]
}