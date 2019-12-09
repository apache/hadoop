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

# Mock docker command
docker () {
  if [ "$1" = "-v" ]; then
    shift
    echo Docker version ${DCKR_MOCK_VER:?}
  elif [ "$1" = run ]; then
    shift
    until [ $# -eq 0 ]; do
      if [ "$1" = -v ]; then
        shift
        echo "$1"|awk -F':' '{if (NF == 3 && $3 == "z")
                  printf "Mounted %s with z option.\n", $1
                              else if (NF == 2)
                  printf "Mounted %s without z option.\n", $1}'
      fi
      shift
    done
  fi
}
export -f docker
export DCKR_MOCK_VER

# Mock a SELinux enabled system
enable_selinux () {
  mkdir -p "${TMP}/bin"
  echo true >"${TMP}/bin"/selinuxenabled
  chmod a+x "${TMP}/bin"/selinuxenabled
  if [ "${PATH#${TMP}/bin}" = "${PATH}" ]; then
    PATH="${TMP}/bin":"$PATH"
  fi
}

setup_user () {
  if [ -z "$(printenv USER)" ]; then
    if [ -z "$USER" ]; then
      USER=${HOME##*/}
    fi
    export USER
  fi
}

# Mock stat command as used in start-build-env.sh
stat () {
  if [ "$1" = --printf='%C' -a $# -eq 2 ]; then
    printf 'mock_u:mock_r:mock_t:s0'
  else
    command stat "$@"
  fi
}
export -f stat

# Verify that host directories get mounted without z option
# and INFO messages get printed out
@test "start-build-env.sh (Docker without z mount option)" {
  if [ "$(uname -s)" != "Linux" ]; then
    skip "Not on Linux platform"
  fi
  enable_selinux
  setup_user
  DCKR_MOCK_VER=1.4
  run "${BATS_TEST_DIRNAME}/../../../../../start-build-env.sh"
  [ "$status" -eq 0 ]
  [[ ${lines[0]} == "INFO: SELinux is enabled." ]]
  [[ ${lines[1]} =~ \
     "Mounted ".*" may not be accessible to the container." ]]
  [[ ${lines[2]} == \
     "INFO: If so, on the host, run the following command:" ]]
  [[ ${lines[3]} =~ "# chcon -Rt svirt_sandbox_file_t " ]]
  [[ ${lines[-2]} =~ "Mounted ".*" without z option." ]]
  [[ ${lines[-1]} =~ "Mounted ".*" without z option." ]]
}

# Verify that host directories get mounted with z option
@test "start-build-env.sh (Docker with z mount option)" {
  if [ "$(uname -s)" != "Linux" ]; then
    skip "Not on Linux platform"
  fi
  enable_selinux
  setup_user
  DCKR_MOCK_VER=1.7
  run "${BATS_TEST_DIRNAME}/../../../../../start-build-env.sh"
  [ "$status" -eq 0 ]
  [[ ${lines[-2]} =~ "Mounted ".*" with z option." ]]
  [[ ${lines[-1]} =~ "Mounted ".*" with z option." ]]
}
