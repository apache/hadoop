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

@test "hadoop_stop_daemon_changing_pid" {
  old_pid=12345
  new_pid=54321
  HADOOP_STOP_TIMEOUT=3

  echo ${old_pid} > pidfile
  run hadoop_stop_daemon stop pidfile &
  sleep 1
  echo ${new_pid} > pidfile
  sleep ${HADOOP_STOP_TIMEOUT}

  [ -f pidfile ]
  [ "$(cat pidfile)" = "${new_pid}" ]
}

@test "hadoop_stop_daemon_force_kill" {

  HADOOP_STOP_TIMEOUT=4

  # Run the following in a sub-shell so that its termination doesn't affect the test
  (sh ${TESTBINDIR}/process_with_sigterm_trap.sh ${TMP}/pidfile &)

  # Wait for the process to go into tight loop
  sleep 1

  [ -f ${TMP}/pidfile ]
  pid=$(cat "${TMP}/pidfile")

  run hadoop_stop_daemon my_command ${TMP}/pidfile 2>&1

  # The process should no longer be alive
  ! kill -0 ${pid} > /dev/null 2>&1

  # The PID file should be gone
  [ ! -f ${TMP}/pidfile ]
}
