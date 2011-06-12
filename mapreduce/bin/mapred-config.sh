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

# included in all the mapred scripts with source command
# should not be executed directly

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export HADOOP_MAPRED_HOME="${HADOOP_MAPRED_HOME:-$bin/..}"

if [ -d "${HADOOP_COMMON_HOME}" ]; then
  . "$HADOOP_COMMON_HOME"/bin/hadoop-config.sh
elif [ -d "${HADOOP_HOME}" ]; then
  . "$HADOOP_HOME"/bin/hadoop-config.sh
else
  echo "Hadoop common not found."
  exit
fi
