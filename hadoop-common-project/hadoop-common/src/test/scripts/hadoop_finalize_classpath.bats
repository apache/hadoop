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

@test "hadoop_finalize_classpath (only conf dir)" {
  CLASSPATH=""
  HADOOP_CONF_DIR="${TMP}"

  hadoop_translate_cygwin_path () { true; }
  hadoop_add_to_classpath_userpath () { true; }

  hadoop_finalize_classpath

  [ "${CLASSPATH}" = "${TMP}" ]

}

@test "hadoop_finalize_classpath (before conf dir)" {
  CLASSPATH="1"
  HADOOP_CONF_DIR="${TMP}"

  hadoop_translate_cygwin_path () { true; }
  hadoop_add_to_classpath_userpath () { true; }

  hadoop_finalize_classpath

  [ "${CLASSPATH}" = "${TMP}:1" ]
}

@test "hadoop_finalize_classpath (adds user)" {
  CLASSPATH=""
  HADOOP_CONF_DIR="${TMP}"

  hadoop_translate_cygwin_path () { true; }
  hadoop_add_to_classpath_userpath () { testvar=true; }

  hadoop_finalize_classpath

  [ "${testvar}" = "true" ]
}

@test "hadoop_finalize_classpath (calls cygwin)" {
  CLASSPATH=""
  HADOOP_CONF_DIR="${TMP}"
  HADOOP_IS_CYGWIN=true

  hadoop_translate_cygwin_path () { [ $1 = CLASSPATH ]; }
  hadoop_add_to_classpath_userpath () { true; }

  hadoop_finalize_classpath
}