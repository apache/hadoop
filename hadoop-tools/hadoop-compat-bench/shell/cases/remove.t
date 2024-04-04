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
hadoop fs -mkdir -p "${baseDir}/dir/sub"
hadoop fs -put "${localDir}/dat" "${baseDir}/dir/"
hadoop fs -put "${localDir}/dat" "${baseDir}/dir/sub/"

echo "1..4"

# 1. rm
hadoop fs -rm -f -skipTrash "${baseDir}/dir/dat"
expect_ret "rm" 1 hadoop fs -test -e "${baseDir}/dir/dat"

# 2. rmr
hadoop fs -rmr "${baseDir}/dir/sub"
expect_ret "rmr" 1 hadoop fs -test -e "${baseDir}/dir/sub"

# 3. rmdir
hadoop fs -rmdir "${baseDir}/dir"
expect_ret "rmdir" 1 hadoop fs -test -e "${baseDir}/dir"

# 4. expunge
expect_ret "expunge" 0 hadoop fs -expunge -immediate -fs "${baseDir}"
