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

@test "hadoop_subcommand_opts (missing param)" {
  HADOOP_OPTS="x"
  run hadoop_subcommand_opts testvar
  [ "${status}" = "1" ]
}

@test "hadoop_subcommand_opts (simple not exist)" {
  HADOOP_OPTS="x"
  hadoop_subcommand_opts hadoop subcommand
  [ "${HADOOP_OPTS}" = "x" ]
}

@test "hadoop_subcommand_opts (hadoop simple exist)" {
  HADOOP_OPTS="x"
  HADOOP_TEST_OPTS="y"
  hadoop_subcommand_opts hadoop test
  echo "${HADOOP_OPTS}"
  [ "${HADOOP_OPTS}" = "x y" ]
}

@test "hadoop_subcommand_opts (hadoop complex exist)" {
  HADOOP_OPTS="x"
  HADOOP_TEST_OPTS="y z"
  hadoop_subcommand_opts hadoop test
  echo "${HADOOP_OPTS}"
  [ "${HADOOP_OPTS}" = "x y z" ]
}

@test "hadoop_subcommand_opts (hdfs simple exist)" {
  HADOOP_OPTS="x"
  HDFS_TEST_OPTS="y"
  hadoop_subcommand_opts hdfs test
  echo "${HADOOP_OPTS}"
  [ "${HADOOP_OPTS}" = "x y" ]
}

@test "hadoop_subcommand_opts (yarn simple exist)" {
  HADOOP_OPTS="x"
  YARN_TEST_OPTS="y"
  hadoop_subcommand_opts yarn test
  echo "${HADOOP_OPTS}"
  [ "${HADOOP_OPTS}" = "x y" ]
}

@test "hadoop_subcommand_opts (deprecation case)" {
  HADOOP_OPTS="x"
  HADOOP_NAMENODE_OPTS="y"
  hadoop_subcommand_opts hdfs namenode
  echo "${HADOOP_OPTS}"
  [ "${HADOOP_OPTS}" = "x y" ]
}
