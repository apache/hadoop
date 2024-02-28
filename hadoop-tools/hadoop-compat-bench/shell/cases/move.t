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

echo "1..2"

# 1. moveFromLocal
expect_ret "moveFromLocal" 0 hadoop fs -moveFromLocal "${localDir}/dat" "${baseDir}/"

# 2. mv
hadoop fs -mv "${baseDir}/dat" "${baseDir}/dat2"
expect_ret "mv" 0 hadoop fs -test -f "${baseDir}/dat2"

# moveToLocal is not achieved on HDFS
# hadoop fs -moveToLocal "${baseDir}/dat2" "${localDir}/"
# expect_ret "moveToLocal" 0 test -f "${localDir}/dat2"
