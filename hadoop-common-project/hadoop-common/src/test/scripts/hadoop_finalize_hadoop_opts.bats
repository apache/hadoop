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

@test "hadoop_finalize_hadoop_opts (raw)" {
  local j

  HADOOP_IS_CYGWIN=false
  HADOOP_OPTS=""
  hadoop_finalize_hadoop_opts
  for j in hadoop.log.dir \
        hadoop.log.file \
        hadoop.home.dir \
        hadoop.root.logger \
        hadoop.policy.file \
        hadoop.security.logger \
        hadoop.id.str; do

    [ "${HADOOP_OPTS#*${j}}" != "${HADOOP_OPTS}" ]
  done
}

@test "hadoop_finalize_hadoop_opts (cygwin)" {
  local j

  HADOOP_IS_CYGWIN=true
  HADOOP_OPTS=""

  hadoop_translate_cygwin_path () {
    eval ${1}="foobarbaz"
  }

  hadoop_finalize_hadoop_opts
  for j in hadoop.log.dir \
        hadoop.home.dir; do
    echo "${j} from >${HADOOP_OPTS}<"
    [ "${HADOOP_OPTS#*${j}=foobarbaz}" != "${HADOOP_OPTS}" ]
  done
}
