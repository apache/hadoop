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
        HADOOP_ENABLE_BUILD_PATHS \
        CLASSPATH HADOOP_COMMON_DIR \
        HADOOP_COMMON_HOME \
        HADOOP_COMMON_LIB_JARS_DIR \
        HADOOP_ENABLE_BUILD_PATHS ; do
      unset ${j}
  done
}

createdirs () {
  local j

  for j in hadoop-common/target/classes \
           commondir/webapps commonlibjars ; do
    mkdir -p "${TMP}/${j}"
    touch "${TMP}/${j}/fake.jar"
  done
  HADOOP_COMMON_HOME=${TMP}
  HADOOP_COMMON_DIR=commondir
  HADOOP_COMMON_LIB_JARS_DIR=commonlibjars
}

@test "hadoop_add_common_to_classpath (negative)" {
   freetheclasses
   createdirs
   unset HADOOP_COMMON_HOME
   run hadoop_add_common_to_classpath
   [ "${status}" -eq 1 ]
}

@test "hadoop_add_common_to_classpath (positive)" {
   freetheclasses
   createdirs
   set +e
   hadoop_add_common_to_classpath
   set -e
   echo ">${CLASSPATH}<"
   [ "${CLASSPATH}" = "${TMP}/commonlibjars/*:${TMP}/commondir/*" ]
}

@test "hadoop_add_common_to_classpath (build paths)" {
   freetheclasses
   createdirs
   HADOOP_ENABLE_BUILD_PATHS=true
   set +e
   hadoop_add_common_to_classpath
   set -e
   echo ">${CLASSPATH}<"
   [ "${CLASSPATH}" = "${TMP}/hadoop-common/target/classes:${TMP}/commonlibjars/*:${TMP}/commondir/*" ]
 }
