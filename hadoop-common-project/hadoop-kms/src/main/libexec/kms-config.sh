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

  if [[ -z "${HADOOP_KMS_ENV_PROCESSED}" ]]; then
    if [[ -e "${HADOOP_CONF_DIR}/kms-env.sh" ]]; then
      . "${HADOOP_CONF_DIR}/kms-env.sh"
      export HADOOP_KMS_ENV_PROCESSED=true
    fi
  fi

  export HADOOP_CATALINA_PREFIX=kms

  export HADOOP_CATALINA_TEMP="${KMS_TEMP:-${HADOOP_HOME}/temp}"

  hadoop_deprecate_envvar KMS_CONFIG HADOOP_CONF_DIR

  hadoop_deprecate_envvar KMS_LOG HADOOP_LOG_DIR

  export HADOOP_CATALINA_CONFIG="${HADOOP_CONF_DIR}"
  export HADOOP_CATALINA_LOG="${HADOOP_LOG_DIR}"

  export HADOOP_CATALINA_HTTP_PORT="${KMS_HTTP_PORT:-9600}"
  export HADOOP_CATALINA_ADMIN_PORT="${KMS_ADMIN_PORT:-$((HADOOP_CATALINA_HTTP_PORT+1))}"
  export HADOOP_CATALINA_MAX_THREADS="${KMS_MAX_THREADS:-1000}"
  export HADOOP_CATALINA_MAX_HTTP_HEADER_SIZE="${KMS_MAX_HTTP_HEADER_SIZE:-65536}"

  export HADOOP_CATALINA_SSL_KEYSTORE_FILE="${KMS_SSL_KEYSTORE_FILE:-${HOME}/.keystore}"

  # this is undocumented, but older versions would rip the TRUSTSTORE_PASS out of the
  # CATALINA_OPTS
  # shellcheck disable=SC2086
  export KMS_SSL_TRUSTSTORE_PASS=${KMS_SSL_TRUSTSTORE_PASS:-"$(echo ${CATALINA_OPTS} | grep -o 'trustStorePassword=[^ ]*' | cut -f2 -d= )"}

  export CATALINA_BASE="${CATALINA_BASE:-${HADOOP_HOME}/share/hadoop/kms/tomcat}"
  export HADOOP_CATALINA_HOME="${KMS_CATALINA_HOME:-${CATALINA_BASE}}"

  export CATALINA_OUT="${CATALINA_OUT:-${HADOOP_LOG_DIR}/hadoop-${HADOOP_IDENT_STRING}-kms-${HOSTNAME}.out}"

  export CATALINA_PID="${CATALINA_PID:-${HADOOP_PID_DIR}/hadoop-${HADOOP_IDENT_STRING}-kms.pid}"

  if [[ -n "${HADOOP_SHELL_SCRIPT_DEBUG}" ]]; then
    varlist=$(env | egrep '(^KMS|^CATALINA)' | cut -f1 -d= | grep -v _PASS)
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
