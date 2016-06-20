#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

function hadoop_subproject_init
{
  local this
  local binparent
  local varlist

  if [[ -z "${HADOOP_HTTPFS_ENV_PROCESSED}" ]]; then
    if [[ -e "${HADOOP_CONF_DIR}/httpfs-env.sh" ]]; then
      . "${HADOOP_CONF_DIR}/httpfs-env.sh"
      export HADOOP_HTTPFS_ENV_PROCESSED=true
    fi
  fi

  export HADOOP_CATALINA_PREFIX=httpfs

  export HADOOP_CATALINA_TEMP="${HTTPFS_TEMP:-${HADOOP_HOME}/temp}"

  hadoop_deprecate_envvar HTTPFS_CONFIG HADOOP_CONF_DIR

  hadoop_deprecate_envvar HTTPFS_LOG HADOOP_LOG_DIR

  export HADOOP_CATALINA_CONFIG="${HADOOP_CONF_DIR}"
  export HADOOP_CATALINA_LOG="${HADOOP_LOG_DIR}"

  export HTTPFS_HTTP_HOSTNAME=${HTTPFS_HTTP_HOSTNAME:-$(hostname -f)}

  export HADOOP_CATALINA_HTTP_PORT="${HTTPFS_HTTP_PORT:-14000}"
  export HADOOP_CATALINA_ADMIN_PORT="${HTTPFS_ADMIN_PORT:-$((HADOOP_CATALINA_HTTP_PORT+1))}"
  export HADOOP_CATALINA_MAX_THREADS="${HTTPFS_MAX_THREADS:-150}"
  export HADOOP_CATALINA_MAX_HTTP_HEADER_SIZE="${HTTPFS_MAX_HTTP_HEADER_SIZE:-65536}"

  export HTTPFS_SSL_ENABLED=${HTTPFS_SSL_ENABLED:-false}

  export HADOOP_CATALINA_SSL_KEYSTORE_FILE="${HTTPFS_SSL_KEYSTORE_FILE:-${HOME}/.keystore}"

  export CATALINA_BASE="${CATALINA_BASE:-${HADOOP_HOME}/share/hadoop/httpfs/tomcat}"
  export HADOOP_CATALINA_HOME="${HTTPFS_CATALINA_HOME:-${CATALINA_BASE}}"

  export CATALINA_OUT="${CATALINA_OUT:-${HADOOP_LOG_DIR}/hadoop-${HADOOP_IDENT_STRING}-httpfs-${HOSTNAME}.out}"

  export CATALINA_PID="${CATALINA_PID:-${HADOOP_PID_DIR}/hadoop-${HADOOP_IDENT_STRING}-httpfs.pid}"

  if [[ -n "${HADOOP_SHELL_SCRIPT_DEBUG}" ]]; then
    varlist=$(env | egrep '(^HTTPFS|^CATALINA)' | cut -f1 -d= | grep -v _PASS)
    for i in ${varlist}; do
      hadoop_debug "Setting ${i} to ${!i}"
    done
  fi
}

if [[ -n "${HADOOP_COMMON_HOME}" ]] &&
   [[ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh" ]]; then
  . "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh"
elif [[ -e "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
elif [[ -e "${HADOOP_HOME}/libexec/hadoop-config.sh" ]]; then
  . "${HADOOP_HOME}/libexec/hadoop-config.sh"
else
  echo "ERROR: Hadoop common not found." 2>&1
  exit 1
fi
