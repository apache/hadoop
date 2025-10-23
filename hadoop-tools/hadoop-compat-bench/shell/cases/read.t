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

# 1. get
hadoop fs -get "${baseDir}/dat" "${localDir}/"
expect_ret "get" 0 test -f "${localDir}/dat"

# 2. cat
expect_out "cat" "Hello World!" hadoop fs -cat "${baseDir}/dat"

# 3. text
expect_out "text" "Hello World!" hadoop fs -text "${baseDir}/dat"

# 4. head
expect_out "head" "Hello World!" hadoop fs -head "${baseDir}/dat"

# 5. tail
expect_out "tail" "Hello World!" hadoop fs -tail "${baseDir}/dat"
