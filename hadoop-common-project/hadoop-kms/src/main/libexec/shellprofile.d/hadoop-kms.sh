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

if [[ "${HADOOP_SHELL_EXECNAME}" = hadoop ]]; then
  hadoop_add_subcommand "kms" "run KMS, the Key Management Server"
fi

## @description  Command handler for kms subcommand
## @audience     private
## @stability    stable
## @replaceable  no
function hadoop_subcommand_kms
{
  if [[ -f "${HADOOP_CONF_DIR}/kms-env.sh" ]]; then
    # shellcheck disable=SC1090
    . "${HADOOP_CONF_DIR}/kms-env.sh"
  fi

  hadoop_deprecate_envvar KMS_CONFIG HADOOP_CONF_DIR
  hadoop_deprecate_envvar KMS_LOG HADOOP_LOG_DIR

  hadoop_using_envvar KMS_HTTP_PORT
  hadoop_using_envvar KMS_MAX_HTTP_HEADER_SIZE
  hadoop_using_envvar KMS_MAX_THREADS
  hadoop_using_envvar KMS_SSL_ENABLED
  hadoop_using_envvar KMS_SSL_KEYSTORE_FILE
  hadoop_using_envvar KMS_TEMP

  # shellcheck disable=SC2034
  HADOOP_SUBCMD_SUPPORTDAEMONIZATION=true
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.crypto.key.kms.server.KMSWebServer

  hadoop_add_param HADOOP_OPTS "-Dkms.config.dir=" \
    "-Dkms.config.dir=${HADOOP_CONF_DIR}"
  hadoop_add_param HADOOP_OPTS "-Dkms.log.dir=" \
    "-Dkms.log.dir=${HADOOP_LOG_DIR}"

  if [[ "${HADOOP_DAEMON_MODE}" == "default" ]] ||
     [[ "${HADOOP_DAEMON_MODE}" == "start" ]]; then
    hadoop_mkdir "${KMS_TEMP:-${HADOOP_HOME}/temp}"
  fi
}