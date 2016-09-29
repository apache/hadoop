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

@test "hadoop_subcommand_opts (daemonization false)" {
  HADOOP_OPTS="1"
  HADOOP_CLIENT_OPTS="2"
  HADOOP_SUBCMD_SUPPORTDAEMONIZATION="false"
  hadoop_add_client_opts
  [ "${HADOOP_OPTS}" = "1 2" ]
}

@test "hadoop_subcommand_opts (daemonization true)" {
  HADOOP_OPTS="1"
  HADOOP_CLIENT_OPTS="2"
  HADOOP_SUBCMD_SUPPORTDAEMONIZATION="true"
  hadoop_add_client_opts
  [ "${HADOOP_OPTS}" = "1" ]
}

@test "hadoop_subcommand_opts (daemonization empty)" {
  HADOOP_OPTS="1"
  HADOOP_CLIENT_OPTS="2"
  unset HADOOP_SUBCMD_SUPPORTDAEMONIZATION
  hadoop_add_client_opts
  [ "${HADOOP_OPTS}" = "1 2" ]
}
