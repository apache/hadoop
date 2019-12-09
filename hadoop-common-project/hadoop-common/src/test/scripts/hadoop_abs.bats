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

create_fake () {
  mkdir ${TMP}/j
  touch ${TMP}/j/k
  ln -s j ${TMP}/l
}


@test "hadoop_abs (simple not exist)" {
  run hadoop_abs fake
  [ "${status}" -eq 1 ]
}

@test "hadoop_abs (simple dir)" {
  create_fake
  run hadoop_abs "${TMP}/j"
  [ "${output}" = "${TMP}/j" ]
}

@test "hadoop_abs (simple file)" {
  create_fake
  run hadoop_abs "${TMP}/j/k"
  [ "${output}" = "${TMP}/j/k" ]
}

@test "hadoop_abs (relative file1)" {
  create_fake
  run hadoop_abs "${TMP}/j/../j/k"
  [ "${output}" = "${TMP}/j/k" ]
}

@test "hadoop_abs (relative file2)" {
  create_fake
  run hadoop_abs "${RELTMP}/j/../j/k"
  [ "${output}" = "${TMP}/j/k" ]
}

@test "hadoop_abs (relative dir)" {
  create_fake
  fred=$(cd -P -- ".." >/dev/null && pwd -P)
  run hadoop_abs ".."
  [ "${output}" = "${fred}" ]
}

@test "hadoop_abs (symlink)" {
  create_fake
  run hadoop_abs "${TMP}/l"
  [ "${output}" = "${TMP}/j" ]
}
