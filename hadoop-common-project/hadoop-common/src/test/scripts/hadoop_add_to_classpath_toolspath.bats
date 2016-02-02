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

  for j in HADOOP_TOOLS_PATH  \
      CLASSPATH; do
      unset ${j}
  done
}

createdirs () {
  local j

  for j in new old foo bar baz; do
    mkdir -p "${TMP}/${j}"
  done
}

@test "hadoop_add_to_classpath_toolspath (nothing)" {
   freetheclasses
   hadoop_add_to_classpath_toolspath
   [ -z "${CLASSPATH}" ]
}

@test "hadoop_add_to_classpath_toolspath (none)" {
   freetheclasses
   CLASSPATH=test
   hadoop_add_to_classpath_toolspath
   [ "${CLASSPATH}" = "test" ]
}

@test "hadoop_add_to_classpath_toolspath (only)" {
   freetheclasses
   createdirs
   HADOOP_TOOLS_PATH="${TMP}/new"
   hadoop_add_to_classpath_toolspath
   [ "${CLASSPATH}" = "${TMP}/new" ]
}

@test "hadoop_add_to_classpath_toolspath (1+1)" {
   freetheclasses
   createdirs
   CLASSPATH=${TMP}/foo
   HADOOP_TOOLS_PATH=${TMP}/foo
   hadoop_add_to_classpath_toolspath
   echo ">${CLASSPATH}<"
   [ ${CLASSPATH} = "${TMP}/foo" ]
}

@test "hadoop_add_to_classpath_toolspath (3+2)" {
   freetheclasses
   createdirs
   CLASSPATH=${TMP}/foo:${TMP}/bar:${TMP}/baz
   HADOOP_TOOLS_PATH=${TMP}/new:${TMP}/old
   hadoop_add_to_classpath_toolspath
   echo ">${CLASSPATH}<"
   [ ${CLASSPATH} = "${TMP}/foo:${TMP}/bar:${TMP}/baz:${TMP}/new:${TMP}/old" ]
}
