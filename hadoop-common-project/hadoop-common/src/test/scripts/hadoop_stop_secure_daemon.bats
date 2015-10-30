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

@test "hadoop_stop_secure_daemon" {
  old_daemon_pid=12345
  old_priv_pid=23456
  new_daemon_pid=54321
  new_priv_pid=65432
  HADOOP_STOP_TIMEOUT=3

  echo ${old_daemon_pid} > daemonpidfile
  echo ${old_priv_pid} > privpidfile
  run hadoop_stop_secure_daemon stop daemonpidfile privpidfile &
  sleep 1
  echo ${new_daemon_pid} > daemonpidfile
  echo ${new_priv_pid} > privpidfile
  sleep ${HADOOP_STOP_TIMEOUT}

  [ -f daemonpidfile ]
  [ "$(cat daemonpidfile)" = "${new_daemon_pid}" ]
  [ -f privpidfile ]
  [ "$(cat privpidfile)" = "${new_priv_pid}" ]
}
