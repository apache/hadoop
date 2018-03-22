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

# Setup minimal environment to invoke the 'hadoop' command.
hadoopcommandsetup () {
  export HADOOP_LIBEXEC_DIR="${TMP}/libexec"
  export HADOOP_CONF_DIR="${TMP}/conf"
  mkdir -p "${HADOOP_LIBEXEC_DIR}"
  echo   ". \"${BATS_TEST_DIRNAME}/../../main/bin/hadoop-functions.sh\"" > "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
  chmod a+rx "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
}

# Verify that the 'hadoop' command correctly infers MYNAME and
# HADOOP_SHELL_EXECNAME
@test "hadoop_shell_execname" {
  hadoopcommandsetup
  export QATESTMODE=unittest
  run "${BATS_TEST_DIRNAME}/../../main/bin/hadoop" envvars
  echo ">${output}<"
  [[ ${output} =~ MYNAME=.*/hadoop ]]
  [[ ${output} =~ HADOOP_SHELL_EXECNAME=hadoop ]]
}
