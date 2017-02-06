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
  hadoop_add_subcommand "httpfs" "run HttpFS server, the HDFS HTTP Gateway"
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

  hadoop_deprecate_envvar HTTPFS_CONFIG HADOOP_CONF_DIR
  hadoop_deprecate_envvar HTTPFS_LOG HADOOP_LOG_DIR

  hadoop_using_envvar HTTPFS_HTTP_HOSTNAME
  hadoop_using_envvar HTTPFS_HTTP_PORT
  hadoop_using_envvar HTTPFS_MAX_HTTP_HEADER_SIZE
  hadoop_using_envvar HTTPFS_MAX_THREADS
  hadoop_using_envvar HTTPFS_SSL_ENABLED
  hadoop_using_envvar HTTPFS_SSL_KEYSTORE_FILE
  hadoop_using_envvar HTTPFS_TEMP

  # shellcheck disable=SC2034
  HADOOP_SUBCMD_SUPPORTDAEMONIZATION=true
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.fs.http.server.HttpFSServerWebServer
  # shellcheck disable=SC2034

  hadoop_add_param HADOOP_OPTS "-Dhttpfs.home.dir" \
    "-Dhttpfs.home.dir=${HADOOP_HOME}"
  hadoop_add_param HADOOP_OPTS "-Dhttpfs.config.dir" \
    "-Dhttpfs.config.dir=${HTTPFS_CONFIG:-${HADOOP_CONF_DIR}}"
  hadoop_add_param HADOOP_OPTS "-Dhttpfs.log.dir" \
    "-Dhttpfs.log.dir=${HTTPFS_LOG:-${HADOOP_LOG_DIR}}"
  hadoop_add_param HADOOP_OPTS "-Dhttpfs.http.hostname" \
    "-Dhttpfs.http.hostname=${HTTPFS_HOST_NAME:-$(hostname -f)}"
  if [[ -n "${HTTPFS_SSL_ENABLED}" ]]; then
    hadoop_add_param HADOOP_OPTS "-Dhttpfs.ssl.enabled" \
      "-Dhttpfs.ssl.enabled=${HTTPFS_SSL_ENABLED}"
  fi

  if [[ "${HADOOP_DAEMON_MODE}" == "default" ]] ||
     [[ "${HADOOP_DAEMON_MODE}" == "start" ]]; then
    hadoop_mkdir "${HTTPFS_TEMP:-${HADOOP_HOME}/temp}"
  fi
}