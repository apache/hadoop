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

if ! declare -f mapred_subcommand_archive-logs >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = mapred ]]; then
    hadoop_add_subcommand "archive-logs" "combine aggregated logs into hadoop archives"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  archive-logs command for mapred
## @audience     public
## @stability    stable
## @replaceable  yes
function mapred_subcommand_archive-logs
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.HadoopArchiveLogs
  hadoop_add_to_classpath_tools hadoop-archive-logs
  hadoop_debug "Appending HADOOP_CLIENT_OPTS onto HADOOP_OPTS"
  HADOOP_OPTS="${HADOOP_OPTS} ${HADOOP_CLIENT_OPTS}"
}

fi
