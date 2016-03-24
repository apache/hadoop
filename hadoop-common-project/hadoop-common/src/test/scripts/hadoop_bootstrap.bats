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

@test "hadoop_bootstrap (no libexec)" {
  unset HADOOP_LIBEXEC_DIR
  run hadoop_bootstrap
  [ "${status}" -eq 1 ]
}

@test "hadoop_bootstrap (libexec)" {
  unset   HADOOP_HOME
  unset   HADOOP_COMMON_DIR
  unset   HADOOP_COMMON_LIB_JARS_DIR
  unset   HDFS_DIR
  unset   HDFS_LIB_JARS_DIR
  unset   YARN_DIR
  unset   YARN_LIB_JARS_DIR
  unset   MAPRED_DIR
  unset   MAPRED_LIB_JARS_DIR
  unset   HADOOP_TOOLS_HOME
  unset   HADOOP_TOOLS_DIR
  unset   HADOOP_TOOLS_LIB_JARS_DIR
  unset   HADOOP_OS_TYPE

  hadoop_bootstrap

  # all of these should be set
  [ -n ${HADOOP_HOME} ]
  [ -n ${HADOOP_COMMON_DIR} ]
  [ -n ${HADOOP_COMMON_LIB_JARS_DIR} ]
  [ -n ${HDFS_DIR} ]
  [ -n ${HDFS_LIB_JARS_DIR} ]
  [ -n ${YARN_DIR} ]
  [ -n ${YARN_LIB_JARS_DIR} ]
  [ -n ${MAPRED_DIR} ]
  [ -n ${MAPRED_LIB_JARS_DIR} ]
  [ -n ${HADOOP_OS_TYPE} ]
  [ -n ${HADOOP_TOOLS_PATH} ]
  [ -n ${HADOOP_TOOLS_HOME} ]
  [ -n ${HADOOP_TOOLS_DIR} ]
  [ -n ${HADOOP_TOOLS_LIB_JARS_DIR} ]
}
