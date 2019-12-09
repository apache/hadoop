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

@test "hadoop_mkdir (create)" {
  DIR=${BATS_TMPDIR}/nodir
  rm -fr ${DIR}
  run hadoop_mkdir ${DIR}
  [ "${status}" = 0 ]
  [ "${output}" = "WARNING: ${DIR} does not exist. Creating." ]
}


@test "hadoop_mkdir (exists)" {
  DIR=${BATS_TMPDIR}/exists
  mkdir -p ${DIR}
  run hadoop_mkdir ${DIR}
  [ "${status}" = 0 ]
  [ -z "${output}" ]
}


@test "hadoop_mkdir (failed)" {
  DIR=${BATS_TMPDIR}/readonly_dir/dir
  mkdir -p ${BATS_TMPDIR}/readonly_dir
  chmod a-w ${BATS_TMPDIR}/readonly_dir
  run hadoop_mkdir ${DIR}
  [ "${status}" != 0 ]
}