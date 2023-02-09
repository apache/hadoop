#!/usr/bin/env bash
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

setup() {

  TMP="${BATS_TEST_DIRNAME}/../../../target/test-dir/bats.$$.${RANDOM}"
  mkdir -p "${TMP}"
  TMP=$(cd -P -- "${TMP}" >/dev/null && pwd -P)
  export TMP
  TESTBINDIR="${BATS_TEST_DIRNAME}"
  HADOOP_LIBEXEC_DIR=${TESTBINDIR}/../../main/bin
  HADOOP_LIBEXEC_DIR=$(cd -P -- "${HADOOP_LIBEXEC_DIR}" >/dev/null && pwd -P)

  # shellcheck disable=SC2034
  HADOOP_SHELL_SCRIPT_DEBUG=true
  unset HADOOP_CONF_DIR
  # we unset both of these for bw compat
  unset HADOOP_HOME
  unset HADOOP_PREFIX

  echo "bindir: ${TESTBINDIR}" 2>&1

  mkdir -p "${TMP}"

  # shellcheck disable=SC2034
  QATESTMODE=true

  # shellcheck disable=SC1090
  . "${BATS_TEST_DIRNAME}/../../../../../hadoop-common-project/hadoop-common/src/main/bin/hadoop-functions.sh"
  pushd "${TMP}" >/dev/null
}

teardown() {
  popd >/dev/null
  rm -rf "${TMP}"
}


strstr() {
  if [ "${1#*$2}" != "${1}" ]; then
    echo true
  else
    echo false
  fi
}
