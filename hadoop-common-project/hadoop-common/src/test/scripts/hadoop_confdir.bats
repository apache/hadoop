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

create_fake_dirs () {
  HADOOP_PREFIX=${TMP}
  for j in conf etc/hadoop; do
    mkdir -p "${HADOOP_PREFIX}/${j}"
    echo "unittest=${j}" > "${HADOOP_PREFIX}/${j}/hadoop-env.sh"
  done
}

@test "hadoop_find_confdir (default)" {
  create_fake_dirs
  hadoop_find_confdir
  [ -n "${HADOOP_CONF_DIR}" ]
}

@test "hadoop_find_confdir (bw compat: conf)" {
  create_fake_dirs
  hadoop_find_confdir
  echo ">${HADOOP_CONF_DIR}< >${HADOOP_PREFIX}/conf<"
  [ "${HADOOP_CONF_DIR}" = ${HADOOP_PREFIX}/conf ]
}

@test "hadoop_find_confdir (etc/hadoop)" {
  create_fake_dirs
  rm -rf "${HADOOP_PREFIX}/conf"
  hadoop_find_confdir
  [ "${HADOOP_CONF_DIR}" = ${HADOOP_PREFIX}/etc/hadoop ]
}

@test "hadoop_verify_confdir (negative) " {
  create_fake_dirs
  HADOOP_CONF_DIR=${HADOOP_PREFIX}/conf
  run hadoop_verify_confdir
  [ -n "${output}" ]
}

@test "hadoop_verify_confdir (positive) " {
  create_fake_dirs
  HADOOP_CONF_DIR=${HADOOP_PREFIX}/conf
  touch "${HADOOP_CONF_DIR}/log4j.properties"
  run hadoop_verify_confdir
  [ -z "${output}" ]
}

@test "hadoop_exec_hadoopenv (positive) " {
  create_fake_dirs
  HADOOP_CONF_DIR=${HADOOP_PREFIX}/conf
  hadoop_exec_hadoopenv
  [ -n "${HADOOP_ENV_PROCESSED}" ]
  [ "${unittest}" = conf ]
}

@test "hadoop_exec_hadoopenv (negative) " {
  create_fake_dirs
  HADOOP_CONF_DIR=${HADOOP_PREFIX}/conf
  HADOOP_ENV_PROCESSED=true
  hadoop_exec_hadoopenv
  [ -z "${unittest}" ]
}

@test "hadoop_exec_userfuncs" {
  create_fake_dirs
  HADOOP_CONF_DIR=${HADOOP_PREFIX}/conf
  echo "unittest=userfunc" > "${HADOOP_CONF_DIR}/hadoop-user-functions.sh"
  hadoop_exec_userfuncs
  [ "${unittest}" = "userfunc" ]
}

@test "hadoop_exec_hadooprc" {
  HOME=${TMP}
  echo "unittest=hadooprc" > "${TMP}/.hadooprc"
  hadoop_exec_hadooprc
  [ ${unittest} = "hadooprc" ]
}


