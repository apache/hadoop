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

freetheclasses () {
  local j

  for j in HADOOP_CLASSPATH  \
      HADOOP_USE_CLIENT_CLASSLOADER \
      HADOOP_USER_CLASSPATH_FIRST \
      CLASSPATH; do
      unset ${!j}
  done
}

createdirs () {
  local j

  for j in new old foo bar baz; do
    mkdir -p "${TMP}/${j}"
  done
}

@test "hadoop_add_to_classpath_userpath (nothing)" {
   freetheclasses
   hadoop_add_to_classpath_userpath
   [ -z "${CLASSPATH}" ]
}

@test "hadoop_add_to_classpath_userpath (none)" {
   freetheclasses
   CLASSPATH=test
   hadoop_add_to_classpath_userpath
   [ "${CLASSPATH}" = "test" ]
}

@test "hadoop_add_to_classpath_userpath (only)" {
   freetheclasses
   createdirs
   HADOOP_CLASSPATH="${TMP}/new"
   hadoop_add_to_classpath_userpath
   [ "${CLASSPATH}" = "${TMP}/new" ]
}

@test "hadoop_add_to_classpath_userpath (classloader)" {
   freetheclasses
   createdirs
   HADOOP_CLASSPATH="${TMP}/new"
   HADOOP_USE_CLIENT_CLASSLOADER="true"
   hadoop_add_to_classpath_userpath
   [ -z "${CLASSPATH}" ]
}

@test "hadoop_add_to_classpath_userpath (1+1 dupe)" {
   freetheclasses
   createdirs
   CLASSPATH=${TMP}/foo
   HADOOP_CLASSPATH=${TMP}/foo
   HADOOP_USER_CLASSPATH_FIRST=""
   hadoop_add_to_classpath_userpath
   echo ">${CLASSPATH}<"
   [ "${CLASSPATH}" = "${TMP}/foo" ]
}

@test "hadoop_add_to_classpath_userpath (3+2 after)" {
   freetheclasses
   createdirs
   CLASSPATH=${TMP}/foo:${TMP}/bar:${TMP}/baz
   HADOOP_CLASSPATH=${TMP}/new:${TMP}/old
   HADOOP_USER_CLASSPATH_FIRST=""
   hadoop_add_to_classpath_userpath
   echo ">${CLASSPATH}<"
   [ "${CLASSPATH}" = "${TMP}/foo:${TMP}/bar:${TMP}/baz:${TMP}/new:${TMP}/old" ]
}

@test "hadoop_add_to_classpath_userpath (3+2 before)" {
   freetheclasses
   createdirs
   CLASSPATH=${TMP}/foo:${TMP}/bar:${TMP}/baz
   HADOOP_CLASSPATH=${TMP}/new:${TMP}/old
   HADOOP_USER_CLASSPATH_FIRST="true"
   hadoop_add_to_classpath_userpath
   echo ">${CLASSPATH}<"
   [ "${CLASSPATH}" = "${TMP}/new:${TMP}/old:${TMP}/foo:${TMP}/bar:${TMP}/baz" ]
}
