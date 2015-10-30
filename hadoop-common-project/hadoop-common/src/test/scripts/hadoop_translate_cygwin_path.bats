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

@test "hadoop_translate_cygwin_path (negative)" {
  HADOOP_IS_CYGWIN=false
  testvar="/this/path/is/cool"
  hadoop_translate_cygwin_path testvar
  [ "${testvar}" = "/this/path/is/cool" ]
}

@test "hadoop_translate_cygwin_path (positive)" {
  HADOOP_IS_CYGWIN=true
  testvar="/this/path/is/cool"

  cygpath () {
    echo "test"
  }

  hadoop_translate_cygwin_path testvar
  [ "${testvar}" = "test" ]
}


@test "hadoop_translate_cygwin_path (path positive)" {
  HADOOP_IS_CYGWIN=true
  testvar="/this/path/is/cool"

  cygpath () {
    echo "test"
  }

  hadoop_translate_cygwin_path testvar true
  [ "${testvar}" = "test" ]
}