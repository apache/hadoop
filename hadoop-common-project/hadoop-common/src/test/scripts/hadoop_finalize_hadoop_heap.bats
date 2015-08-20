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

resetops () {
  unset HADOOP_HEAPSIZE_MAX
  unset HADOOP_HEAPSIZE
  unset HADOOP_HEAPSIZE_MIN
  unset HADOOP_OPTS
}

@test "hadoop_finalize_hadoop_heap (negative)" {
  resetops
  hadoop_finalize_hadoop_heap
  [ -z "${HADOOP_OPTS}" ]
}

@test "hadoop_finalize_hadoop_heap (no unit max)" {
  resetops
  HADOOP_HEAPSIZE_MAX=1000
  hadoop_finalize_hadoop_heap
  echo ">${HADOOP_OPTS}<"
  [ "${HADOOP_OPTS}" = "-Xmx1000m" ]
}

@test "hadoop_finalize_hadoop_heap (no unit old)" {
  resetops
  HADOOP_HEAPSIZE=1000
  hadoop_finalize_hadoop_heap
  echo ">${HADOOP_OPTS}<"
  [ "${HADOOP_OPTS}" = "-Xmx1000m" ]
}

@test "hadoop_finalize_hadoop_heap (unit max)" {
  resetops
  HADOOP_HEAPSIZE_MAX=10g
  hadoop_finalize_hadoop_heap
  echo ">${HADOOP_OPTS}<"
  [ "${HADOOP_OPTS}" = "-Xmx10g" ]
}

@test "hadoop_finalize_hadoop_heap (unit old)" {
  resetops
  HADOOP_HEAPSIZE=10g
  hadoop_finalize_hadoop_heap
  echo ">${HADOOP_OPTS}<"
  [ "${HADOOP_OPTS}" = "-Xmx10g" ]
}

@test "hadoop_finalize_hadoop_heap (no unit min)" {
  resetops
  HADOOP_HEAPSIZE_MIN=1000
  hadoop_finalize_hadoop_heap
  echo ">${HADOOP_OPTS}<"
  [ "${HADOOP_OPTS}" = "-Xms1000m" ]
}

@test "hadoop_finalize_hadoop_heap (unit min)" {
  resetops
  HADOOP_HEAPSIZE_MIN=10g
  hadoop_finalize_hadoop_heap
  echo ">${HADOOP_OPTS}<"
  [ "${HADOOP_OPTS}" = "-Xms10g" ]
}

@test "hadoop_finalize_hadoop_heap (dedupe)" {
  resetops
  HADOOP_HEAPSIZE_MAX=1000
  HADOOP_OPTS="-Xmx5g"
  hadoop_finalize_hadoop_heap
  hadoop_finalize_hadoop_heap
  echo ">${HADOOP_OPTS}<"
  [ "${HADOOP_OPTS}" = "-Xmx5g" ]
}