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

HTTPFS_SILENT=${HTTPFS_SILENT:-true}

source ${HADOOP_LIBEXEC_DIR:-${BASEDIR}/libexec}/httpfs-config.sh

# The Java System property 'httpfs.http.port' it is not used by HttpFS,
# it is used in Tomcat's server.xml configuration file
#
print "Using   CATALINA_OPTS:       ${CATALINA_OPTS}"

catalina_opts="-Dproc_httpfs";
catalina_opts="${catalina_opts} -Dhttpfs.log.dir=${HTTPFS_LOG}"

print "Adding to CATALINA_OPTS:     ${catalina_opts}"

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
  catalina_set_property "httpfs.home.dir" "${HTTPFS_HOME}"
  catalina_set_property "httpfs.config.dir" "${HTTPFS_CONFIG}"
  catalina_set_property "httpfs.temp.dir" "${HTTPFS_TEMP}"
  catalina_set_property "httpfs.admin.port" "${HTTPFS_ADMIN_PORT}"
  catalina_set_property "httpfs.http.port" "${HTTPFS_HTTP_PORT}"
  catalina_set_property "httpfs.http.hostname" "${HTTPFS_HTTP_HOSTNAME}"
  catalina_set_property "httpfs.max.http.header.size" \
    "${HTTPFS_MAX_HTTP_HEADER_SIZE}"
  catalina_set_property "httpfs.ssl.enabled" "${HTTPFS_SSL_ENABLED}"
  catalina_set_property "httpfs.ssl.client.auth" "${HTTPFS_SSL_CLIENT_AUTH}"
  catalina_set_property "httpfs.ssl.enabled.protocols" \
    "${HTTPFS_SSL_ENABLED_PROTOCOLS}"
  catalina_set_property "httpfs.ssl.ciphers" "${HTTPFS_SSL_CIPHERS}"
  catalina_set_property "httpfs.ssl.keystore.file" \
    "${HTTPFS_SSL_KEYSTORE_FILE}"
  catalina_set_property "httpfs.ssl.keystore.pass" \
    "${HTTPFS_SSL_KEYSTORE_PASS}" "<redacted>"
fi

# A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
#
if [ "${1}" = "stop" ]; then
  export JAVA_OPTS=${CATALINA_OPTS}
fi

if [ "${HTTPFS_SILENT}" != "true" ]; then
  exec "${HTTPFS_CATALINA_HOME}/bin/catalina.sh" "$@"
else
  exec "${HTTPFS_CATALINA_HOME}/bin/catalina.sh" "$@" > /dev/null
fi

