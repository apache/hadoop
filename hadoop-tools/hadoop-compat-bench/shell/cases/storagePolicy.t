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
hadoop fs -put "${localDir}/dat" "${baseDir}/"

echo "1..5"

# 1. listPolicies
expect_ret "listPolicies" 0 hdfs storagepolicies -Dfs.defaultFS="${baseDir}" -listPolicies

# 2. setStoragePolicy
expect_out "setStoragePolicy" "Set storage policy ${storagePolicy} .*" hdfs storagepolicies -setStoragePolicy -path "${baseDir}" -policy "${storagePolicy}"

# 3. getStoragePolicy
expect_out "getStoragePolicy" ".*${storagePolicy}.*" hdfs storagepolicies -getStoragePolicy -path "${baseDir}"

# 4. satisfyStoragePolicy
expect_out "satisfyStoragePolicy" "Scheduled blocks to move .*" hdfs storagepolicies -satisfyStoragePolicy -path "${baseDir}"

# 5. unsetStoragePolicy
expect_out "unsetStoragePolicy" "Unset storage policy .*" hdfs storagepolicies -unsetStoragePolicy -path "${baseDir}"
