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

load hdfs-functions_test_helper

# the loading of shell profiles are tested elseswhere
# this only tests the specific subcommand parts

subcommandsetup () {
  export HADOOP_LIBEXEC_DIR="${TMP}/libexec"
  export HADOOP_CONF_DIR="${TMP}/conf"
  mkdir -p "${HADOOP_LIBEXEC_DIR}"
  echo   ". \"${BATS_TEST_DIRNAME}/../../../../../hadoop-common-project/hadoop-common/src/main/bin/hadoop-functions.sh\"" > "${HADOOP_LIBEXEC_DIR}/hdfs-config.sh"
  cat <<-'TOKEN'   >> "${HADOOP_LIBEXEC_DIR}/hdfs-config.sh"

hdfs_subcommand_sub () {
  echo "unittest"
  exit 0
}

hdfs_subcommand_cacheadmin ()
{
  echo cacheadmin
  exit 0
}

hdfs_subcommand_envcheck ()
{
  echo ${HADOOP_SHELL_EXECNAME}
  exit 0
}

hdfs_subcommand_multi ()
{
  echo $2
  exit 0
}
TOKEN
  chmod a+rx "${HADOOP_LIBEXEC_DIR}/hdfs-config.sh"
}

@test "hdfs_subcommand (addition)" {
  subcommandsetup
  run "${BATS_TEST_DIRNAME}/../../main/bin/hdfs" sub
  echo ">${output}<"
  [ "${output}" = unittest ]
}

@test "hdfs_subcommand (substitute)" {
  subcommandsetup
  run "${BATS_TEST_DIRNAME}/../../main/bin/hdfs" cacheadmin
  echo ">${output}<"
  [ "${output}" = cacheadmin ]
}

@test "hdfs_subcommand (envcheck)" {
  subcommandsetup
  run "${BATS_TEST_DIRNAME}/../../main/bin/hdfs" envcheck
  [ "${output}" = hdfs ]
}

@test "hdfs_subcommand (multiparams)" {
  subcommandsetup
  run "${BATS_TEST_DIRNAME}/../../main/bin/hdfs" multi 1 2
  [ "${output}" = 2 ]
}
