#!/bin/bash
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

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

KMS_SILENT=${KMS_SILENT:-true}

HADOOP_LIBEXEC_DIR="${HADOOP_LIBEXEC_DIR:-${BASEDIR}/libexec}"
source ${HADOOP_LIBEXEC_DIR}/kms-config.sh


if [ "x$JAVA_LIBRARY_PATH" = "x" ]; then
  JAVA_LIBRARY_PATH="${HADOOP_LIBEXEC_DIR}/../lib/native/"
else
  JAVA_LIBRARY_PATH="${HADOOP_LIBEXEC_DIR}/../lib/native/:${JAVA_LIBRARY_PATH}"
fi

# The Java System property 'kms.http.port' it is not used by Kms,
# it is used in Tomcat's server.xml configuration file
#

print "Using   CATALINA_OPTS:       ${CATALINA_OPTS_DISP}"

catalina_opts="-Dproc_kms"
catalina_opts="${catalina_opts} -Dkms.log.dir=${KMS_LOG}"
catalina_opts="${catalina_opts} -Djava.library.path=${JAVA_LIBRARY_PATH}"

print "Adding to CATALINA_OPTS:     ${catalina_opts}"
print "Found KMS_SSL_KEYSTORE_PASS:     `echo ${KMS_SSL_KEYSTORE_PASS} | sed 's/./*/g'`"

export CATALINA_OPTS="${CATALINA_OPTS} ${catalina_opts}"

catalina_init_properties() {
  cp "${CATALINA_BASE}/conf/catalina-default.properties" \
    "${CATALINA_BASE}/conf/catalina.properties"
}

catalina_set_property() {
  local key=$1
  local value=$2
  [[ -z "${value}" ]] && return
  local disp_value="${3:-${value}}"
  print "Setting catalina property ${key} to ${disp_value}"
  echo "${key}=${value}" >> "${CATALINA_BASE}/conf/catalina.properties"
}

if [[ "${1}" = "start" || "${1}" = "run" ]]; then
  catalina_init_properties
  catalina_set_property "kms.home.dir" "${KMS_HOME}"
  catalina_set_property "kms.config.dir" "${KMS_CONFIG}"
  catalina_set_property "kms.temp.dir" "${KMS_TEMP}"
  catalina_set_property "kms.admin.port" "${KMS_ADMIN_PORT}"
  catalina_set_property "kms.http.port" "${KMS_HTTP_PORT}"
  catalina_set_property "kms.protocol" "${KMS_PROTOCOL}"
  catalina_set_property "kms.max.threads" "${KMS_MAX_THREADS}"
  catalina_set_property "kms.accept.count" "${KMS_ACCEPT_COUNT}"
  catalina_set_property "kms.acceptor.thread.count" \
    "${KMS_ACCEPTOR_THREAD_COUNT}"
  catalina_set_property "kms.max.http.header.size" \
    "${KMS_MAX_HTTP_HEADER_SIZE}"
  catalina_set_property "kms.ssl.client.auth" "${KMS_SSL_CLIENT_AUTH}"
  catalina_set_property "kms.ssl.enabled.protocols" \
    "${KMS_SSL_ENABLED_PROTOCOLS}"
  catalina_set_property "kms.ssl.ciphers" "${KMS_SSL_CIPHERS}"
  catalina_set_property "kms.ssl.keystore.file" "${KMS_SSL_KEYSTORE_FILE}"

  # Set a KEYSTORE_PASS if not already set
  KMS_SSL_KEYSTORE_PASS=${KMS_SSL_KEYSTORE_PASS:-password}
  catalina_set_property "kms.ssl.keystore.pass" \
    "${KMS_SSL_KEYSTORE_PASS}" "<redacted>"
fi

# A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
#
if [ "${1}" = "stop" ]; then
  export JAVA_OPTS=${CATALINA_OPTS}
fi

if [ "${KMS_SILENT}" != "true" ]; then
  exec "${KMS_CATALINA_HOME}/bin/catalina.sh" "$@"
else
  exec "${KMS_CATALINA_HOME}/bin/catalina.sh" "$@" > /dev/null
fi