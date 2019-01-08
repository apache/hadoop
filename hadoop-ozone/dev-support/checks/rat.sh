#!/usr/bin/env bash
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

mkdir -p target
rm target/rat-aggregated.txt
mvn -fn org.apache.rat:apache-rat-plugin:0.13:check -am -pl :hadoop-ozone-dist -Phdds
grep -r --include=rat.txt "!????" | tee ./target/rat-aggregated.txt
if [ "$(cat target/rat-aggregated.txt)" ]; then
   exit -1
fi

