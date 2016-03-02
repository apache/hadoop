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

toolsetup () {
  HADOOP_LIBEXEC_DIR="${TMP}/libexec"
  mkdir -p "${HADOOP_LIBEXEC_DIR}/tools"
}

@test "hadoop_classpath_tools (load)" {
  toolsetup
  echo "unittest=libexec" > "${HADOOP_LIBEXEC_DIR}/tools/test.sh"
  hadoop_add_to_classpath_tools test
  [ -n "${unittest}" ]
}


@test "hadoop_classpath_tools (not exist)" {
  toolsetup
  hadoop_add_to_classpath_tools test
  [ -z "${unittest}" ]
}

@test "hadoop_classpath_tools (function)" {
  toolsetup
  {
    echo "function hadoop_classpath_tools_test {"
    echo " unittest=libexec"
    echo " }"
  } > "${HADOOP_LIBEXEC_DIR}/tools/test.sh"
  hadoop_add_to_classpath_tools test
  declare -f
  [ -n "${unittest}" ]
}
