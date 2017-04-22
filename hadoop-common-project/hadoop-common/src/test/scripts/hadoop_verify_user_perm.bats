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

@test "hadoop_verify_user_perm (hadoop: no setting)" {
  run hadoop_verify_user_perm hadoop test
  [ "${status}" = "0" ]
}

@test "hadoop_verify_user_perm (yarn: no setting)" {
  run hadoop_verify_user_perm yarn test
  [ "${status}" = "0" ]
}

@test "hadoop_verify_user_perm (hadoop: allow)" {
  HADOOP_TEST_USER=${USER}
  run hadoop_verify_user_perm hadoop test
  [ "${status}" = "0" ]
}

@test "hadoop_verify_user_perm (yarn: allow)" {
  YARN_TEST_USER=${USER}
  run hadoop_verify_user_perm yarn test
  [ "${status}" = "0" ]
}

# colon isn't a valid username, so let's use it
# this should fail regardless of who the user is
# that is running the test code
@test "hadoop_verify_user_perm (hadoop: disallow)" {
  HADOOP_TEST_USER=:
  run hadoop_verify_user_perm hadoop test
  [ "${status}" = "1" ]
}

@test "hadoop_verify_user_perm (yarn: disallow)" {
  YARN_TEST_USER=:
  run hadoop_verify_user_perm yarn test
  [ "${status}" = "1" ]
}
