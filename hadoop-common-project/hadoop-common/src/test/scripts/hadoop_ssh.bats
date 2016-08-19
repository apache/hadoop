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

@test "hadoop_actual_ssh" {
  skip "Not implemented"
  hadoop_actual_ssh
}

@test "hadoop_connect_to_hosts" {
  skip "Not implemented"
  hadoop_connect_to_hosts
}

@test "hadoop_connect_to_hosts_without_pdsh" {
  skip "Not implemented"
  hadoop_connect_to_hosts_without_pdsh
}

@test "hadoop_common_worker_mode_execute (--workers 1)" {
  run  hadoop_common_worker_mode_execute --workers command
  [ "${output}" = "command" ]
}

@test "hadoop_common_worker_mode_execute (--workers 2)" {
  run  hadoop_common_worker_mode_execute --workers command1 command2
  [ "${output}" = "command1 command2" ]
}

@test "hadoop_common_worker_mode_execute (--hosts)" {
  run  hadoop_common_worker_mode_execute --hosts filename command
  [ "${output}" = "command" ]
}

@test "hadoop_common_worker_mode_execute (--hostnames 2)" {
  run  hadoop_common_worker_mode_execute --hostnames "host1,host2" command1 command2
  [ "${output}" = "command1 command2" ]
}
