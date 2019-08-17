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

# included in all the ozone scripts with source command
# should not be executed directly

function hadoop_subproject_init
{
  if [[ -z "${HADOOP_OZONE_ENV_PROCESSED}" ]]; then
    if [[ -e "${HADOOP_CONF_DIR}/ozone-env.sh" ]]; then
      . "${HADOOP_CONF_DIR}/ozone-env.sh"
      export HADOOP_OZONE_ENV_PROCESSED=true
    fi
  fi
  HADOOP_OZONE_HOME="${HADOOP_OZONE_HOME:-$HADOOP_HOME}"

}

if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
  _hd_this="${BASH_SOURCE-$0}"
  HADOOP_LIBEXEC_DIR=$(cd -P -- "$(dirname -- "${_hd_this}")" >/dev/null && pwd -P)
fi

# shellcheck source=./hadoop-common-project/hadoop-common/src/main/bin/hadoop-config.sh

if [[ -n "${HADOOP_COMMON_HOME}" ]] &&
   [[ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh" ]]; then
  . "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh"
elif [[ -e "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
elif [ -e "${HADOOP_HOME}/libexec/hadoop-config.sh" ]; then
  . "${HADOOP_HOME}/libexec/hadoop-config.sh"
else
  echo "ERROR: Hadoop common not found." 2>&1
  exit 1
fi

# HADOOP_OZONE_DELEGATED_CLASSES defines a list of classes which will be loaded by default
# class loader of application instead of isolated class loader. With this way we can solve
# incompatible problem when using hadoop3.x + ozone with older hadoop version.
#export HADOOP_OZONE_DELEGATED_CLASSES=

