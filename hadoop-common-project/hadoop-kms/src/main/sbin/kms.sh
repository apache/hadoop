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

# Mask the trustStorePassword
KMS_SSL_TRUSTSTORE_PASS=`echo $CATALINA_OPTS | grep -o 'trustStorePassword=[^ ]*' | awk -F'=' '{print $2}'`
CATALINA_OPTS_DISP=`echo ${CATALINA_OPTS} | sed -e 's/trustStorePassword=[^ ]*/trustStorePassword=***/'`
print "Using   CATALINA_OPTS:       ${CATALINA_OPTS_DISP}"

catalina_opts="-Dkms.home.dir=${KMS_HOME}";
catalina_opts="${catalina_opts} -Dkms.config.dir=${KMS_CONFIG}";
catalina_opts="${catalina_opts} -Dkms.log.dir=${KMS_LOG}";
catalina_opts="${catalina_opts} -Dkms.temp.dir=${KMS_TEMP}";
catalina_opts="${catalina_opts} -Dkms.admin.port=${KMS_ADMIN_PORT}";
catalina_opts="${catalina_opts} -Dkms.http.port=${KMS_HTTP_PORT}";
catalina_opts="${catalina_opts} -Dkms.max.threads=${KMS_MAX_THREADS}";
catalina_opts="${catalina_opts} -Dkms.ssl.keystore.file=${KMS_SSL_KEYSTORE_FILE}";
catalina_opts="${catalina_opts} -Djava.library.path=${JAVA_LIBRARY_PATH}";

print "Adding to CATALINA_OPTS:     ${catalina_opts}"
print "Found KMS_SSL_KEYSTORE_PASS:     `echo ${KMS_SSL_KEYSTORE_PASS} | sed 's/./*/g'`"

export CATALINA_OPTS="${CATALINA_OPTS} ${catalina_opts}"

# A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
#
if [ "${1}" = "stop" ]; then
  export JAVA_OPTS=${CATALINA_OPTS}
fi

# If ssl, the populate the passwords into ssl-server.xml before starting tomcat
if [ ! "${KMS_SSL_KEYSTORE_PASS}" = "" ] || [ ! "${KMS_SSL_TRUSTSTORE_PASS}" = "" ]; then
  # Set a KEYSTORE_PASS if not already set
  KMS_SSL_KEYSTORE_PASS=${KMS_SSL_KEYSTORE_PASS:-password}
  cat ${CATALINA_BASE}/conf/ssl-server.xml.conf \
    | sed 's/_kms_ssl_keystore_pass_/'${KMS_SSL_KEYSTORE_PASS}'/g' \
    | sed 's/_kms_ssl_truststore_pass_/'${KMS_SSL_TRUSTSTORE_PASS}'/g' > ${CATALINA_BASE}/conf/ssl-server.xml
fi 

exec ${KMS_CATALINA_HOME}/bin/catalina.sh "$@"
