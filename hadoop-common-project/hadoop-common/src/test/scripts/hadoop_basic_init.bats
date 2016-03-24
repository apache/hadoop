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



basicinitsetup () {
  local j

  testvars="HADOOP_IDENT_STRING \
        HADOOP_LOG_DIR \
        HADOOP_LOGFILE \
        HADOOP_LOGLEVEL \
        HADOOP_NICENESS \
        HADOOP_STOP_TIMEOUT \
        HADOOP_PID_DIR \
        HADOOP_ROOT_LOGGER \
        HADOOP_DAEMON_ROOT_LOGGER \
        HADOOP_SECURITY_LOGGER \
        HADOOP_SSH_OPTS \
        HADOOP_SECURE_LOG_DIR \
        HADOOP_SECURE_PID_DIR \
        HADOOP_SSH_PARALLEL"

  dirvars="HADOOP_COMMON_HOME \
        HADOOP_MAPRED_HOME \
        HADOOP_HDFS_HOME \
        HADOOP_YARN_HOME \
        HADOOP_TOOLS_HOME"

  for j in ${testvars}; do
    unset ${j}
  done

  HADOOP_HOME=${TMP}
}

check_var_values () {
  for j in ${testvars}; do
    echo "Verifying ${j} has a value"
    [ -n "${!j}" ]
  done
}

@test "hadoop_basic_init (bad dir errors)" {
  local j
  local i
  # we need to do these in the same order for
  # the unit test, so that the tests are easier
  # to write/test
  basicinitsetup
  for j in ${dirvars}; do
    echo "testing ${j}"
    i=${TMP}/${j}
    mkdir -p "${i}"
    #shellcheck disable=SC2086
    eval ${j}=${i}
    hadoop_basic_init
    echo "Verifying $j has >${i}< >${!j}<"
    [ "${!j}" = "${i}" ]
  done
}


@test "hadoop_basic_init (no non-dir overrides)" {
  basicinitsetup
  hadoop_basic_init
  check_var_values
}

@test "hadoop_basic_init (test non-dir overrides)" {
  local j
  for j in ${testvars}; do
    basicinitsetup
    echo testing ${j}
    eval ${j}=foo
    hadoop_basic_init
    check_var_values
    echo "Verifying $j has foo >${!j}<"
    [ "${j}" = "foo" ]
  done
}
