#!/bin/sh

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

. $(dirname "$0")/../misc.sh

echo "Hello World!" > "${localDir}/dat"
hadoop fs -put "${localDir}/dat" "${baseDir}/src1"
hadoop fs -put "${localDir}/dat" "${baseDir}/src2"

echo "1..3"

# 1. touchz
hadoop fs -touchz "${baseDir}/dat"
expect_out "touchz" "size:0" hadoop fs -stat "size:%b" "${baseDir}/dat"

# 2. concat
expect_ret "concat" 0 hadoop fs -concat "${baseDir}/dat" "${baseDir}/src1" "${baseDir}/src2"
# expect_out "size:26" hadoop fs -stat "size:%b" "${baseDir}/dat"

# 3. getmerge
hadoop fs -getmerge "${baseDir}" "${localDir}/merged"
expect_ret "getmerge" 0 test -s "${localDir}/merged"
