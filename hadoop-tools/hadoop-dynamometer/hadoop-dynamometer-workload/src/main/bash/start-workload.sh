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

# This script simply passes its arguments along to the workload driver
# driver after finding a hadoop command in PATH/HADOOP_COMMON_HOME/HADOOP_HOME
# (searching in that order).

if type hadoop &> /dev/null; then
  hadoop_cmd="hadoop"
elif type "$HADOOP_COMMON_HOME/bin/hadoop" &> /dev/null; then
  hadoop_cmd="$HADOOP_COMMON_HOME/bin/hadoop"
elif type "$HADOOP_HOME/bin/hadoop" &> /dev/null; then
  hadoop_cmd="$HADOOP_HOME/bin/hadoop"
else
  echo "Unable to find a valid hadoop command to execute; exiting."
  exit 1
fi

script_pwd="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../.."

for f in ${script_pwd}/lib/*.jar; do
  # Skip adding the workload JAR since it is added by the `hadoop jar` command
  if [[ "$f" != *"dynamometer-workload-"* ]]; then
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
  fi
done
"$hadoop_cmd" jar "${script_pwd}"/lib/hadoop-dynamometer-workload-*.jar \
  org.apache.hadoop.tools.dynamometer.workloadgenerator.WorkloadDriver "$@"
