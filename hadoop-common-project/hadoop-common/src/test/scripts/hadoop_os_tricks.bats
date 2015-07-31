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

@test "hadoop_os_tricks (cygwin sets cygwin)" {
  HADOOP_OS_TYPE=CYGWIN-IS-GNU-USER-LAND
  hadoop_os_tricks
  [ "${HADOOP_IS_CYGWIN}" = "true" ]
}

@test "hadoop_os_tricks (linux sets arena max)" {
  HADOOP_OS_TYPE=Linux
  hadoop_os_tricks
  [ -n "${MALLOC_ARENA_MAX}" ]
}

@test "hadoop_os_tricks (osx sets java_home)" {
  HADOOP_OS_TYPE=Darwin
  hadoop_os_tricks
  [ -n "${JAVA_HOME}" ]
}