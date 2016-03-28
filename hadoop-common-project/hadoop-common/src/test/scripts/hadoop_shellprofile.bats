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

shellprofilesetup () {
  HADOOP_LIBEXEC_DIR="${TMP}/libexec"
  HADOOP_CONF_DIR="${TMP}/conf"
  mkdir -p "${HADOOP_LIBEXEC_DIR}/shellprofile.d" "${HADOOP_CONF_DIR}/shellprofile.d"
}

_test_hadoop_init () {
  unittest=init
}

_test_hadoop_classpath () {
  unittest=classpath
}

_test_hadoop_nativelib () {
  unittest=nativelib
}

_test_hadoop_finalize () {
  unittest=finalize
}

@test "hadoop_import_shellprofiles (negative)" {
  shellprofilesetup
  unset HADOOP_LIBEXEC_DIR
  run hadoop_import_shellprofiles
  [ -n "${output}" ]
}

@test "hadoop_import_shellprofiles (libexec sh import)" {
  shellprofilesetup
  echo "unittest=libexec" > "${HADOOP_LIBEXEC_DIR}/shellprofile.d/test.sh"
  hadoop_import_shellprofiles
  [ "${unittest}" = libexec ]
}

@test "hadoop_import_shellprofiles (libexec conf sh import+override)" {
  shellprofilesetup
  echo "unittest=libexec" > "${HADOOP_LIBEXEC_DIR}/shellprofile.d/test.sh"
  echo "unittest=conf" > "${HADOOP_CONF_DIR}/shellprofile.d/test.sh"
  hadoop_import_shellprofiles
  [ "${unittest}" = conf ]
}

@test "hadoop_import_shellprofiles (libexec no cmd import)" {
  shellprofilesetup
  echo "unittest=libexec" > "${HADOOP_LIBEXEC_DIR}/shellprofile.d/test.cmd"
  hadoop_import_shellprofiles
  [ -z "${unittest}" ]
}

@test "hadoop_import_shellprofiles (H_O_T)" {
  HADOOP_OPTIONAL_TOOLS=1,2
  shellprofilesetup
  hadoop_import_shellprofiles
  [ "${HADOOP_TOOLS_OPTIONS}" == " 1  2 " ]
}

@test "hadoop_add_profile+hadoop_shellprofiles_init" {
  hadoop_add_profile test
  hadoop_shellprofiles_init
  [ "${unittest}" = init ]
}

@test "hadoop_add_profile+hadoop_shellprofiles_classpath" {
  hadoop_add_profile test
  hadoop_shellprofiles_classpath
  [ "${unittest}" = classpath ]
}

@test "hadoop_add_profile+hadoop_shellprofiles_nativelib" {
  hadoop_add_profile test
  hadoop_shellprofiles_nativelib
  [ "${unittest}" = nativelib ]
}

@test "hadoop_add_profile+hadoop_shellprofiles_finalize" {
  hadoop_add_profile test
  hadoop_shellprofiles_finalize
  [ "${unittest}" = finalize ]
}