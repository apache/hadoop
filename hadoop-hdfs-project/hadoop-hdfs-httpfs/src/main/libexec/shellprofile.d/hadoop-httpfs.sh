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

if [[ "${HADOOP_SHELL_EXECNAME}" = hdfs ]]; then
  hadoop_add_subcommand "httpfs" daemon "run HttpFS server, the HDFS HTTP Gateway"
fi

## @description  Command handler for httpfs subcommand
## @audience     private
## @stability    stable
## @replaceable  no
function hdfs_subcommand_httpfs
{
  if [[ -f "${HADOOP_CONF_DIR}/httpfs-env.sh" ]]; then
    # shellcheck disable=SC1090
    . "${HADOOP_CONF_DIR}/httpfs-env.sh"
  fi

  # shellcheck disable=SC2034
  HADOOP_SUBCMD_SUPPORTDAEMONIZATION=true
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.fs.http.server.HttpFSServerWebServer
  # shellcheck disable=SC2034

  hadoop_add_param HADOOP_OPTS "-Dhttpfs.home.dir" \
    "-Dhttpfs.home.dir=${HTTPFS_HOME:-${HADOOP_HDFS_HOME}}"
  hadoop_add_param HADOOP_OPTS "-Dhttpfs.config.dir" \
    "-Dhttpfs.config.dir=${HTTPFS_CONFIG:-${HADOOP_CONF_DIR}}"
  hadoop_add_param HADOOP_OPTS "-Dhttpfs.log.dir" \
    "-Dhttpfs.log.dir=${HTTPFS_LOG:-${HADOOP_LOG_DIR}}"

  local temp_dir=${HTTPFS_TEMP:-${HADOOP_HDFS_HOME}/temp}
  hadoop_add_param HADOOP_OPTS "-Dhttpfs.temp.dir" \
    "-Dhttpfs.temp.dir=${temp_dir}"
  case ${HADOOP_DAEMON_MODE} in
    start|default)
      hadoop_mkdir "${temp_dir}"
    ;;
  esac
}
