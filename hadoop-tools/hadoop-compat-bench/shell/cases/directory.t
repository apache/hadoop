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

echo "1..8"

# 1. mkdir
expect_ret "mkdir" 0 hadoop fs -mkdir -p "${baseDir}/dir/sub"

# 2. ls
expect_lines "ls" 2 ".*dat.*" ".*dir.*" hadoop fs -ls "${baseDir}"

# 3. lsr
expect_lines "lsr" 3 ".*dat.*" ".*dir.*" ".*sub.*" hadoop fs -lsr "${baseDir}"

# 4. count
expect_out "count" ".*13.*" hadoop fs -count "${baseDir}"

# 5. du
expect_out "du" ".*13.*" hadoop fs -du "${baseDir}"

# 6. dus
expect_out "dus" ".*13.*" hadoop fs -dus "${baseDir}"

# 7. df
expect_ret "df" 0 hadoop fs -df "${baseDir}"

# 8. find
expect_out "find" ".*dat.*" hadoop fs -find "${baseDir}" -name "dat" -print
