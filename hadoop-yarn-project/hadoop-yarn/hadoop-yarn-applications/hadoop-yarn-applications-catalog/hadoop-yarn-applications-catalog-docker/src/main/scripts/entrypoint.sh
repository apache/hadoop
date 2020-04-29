#!/bin/bash -x

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

template_generator() {
  REGEX='(\$\{[a-zA-Z_][a-zA-Z_0-9]*\})'
  if [ -e "$2" ]; then
    mv -f "$2" "$2.bak"
  fi
  while IFS='' read -r line || [[ -n "$line" ]]; do
    while [[ "$line" =~ $REGEX ]] ; do
      LHS=${BASH_REMATCH[1]}
      RHS="$(eval echo "\"$LHS\"")"
      line=${line//$LHS/$RHS}
    done
    echo "$line" >> "$2"
  done < "$1"
}

export JAVA_HOME=/usr/lib/jvm/jre
export HADOOP_CONF_DIR=/etc/hadoop/conf

SOLR_OPTS=()

if [ "${SOLR_STORAGE_TYPE}" == "hdfs" ]; then
  SOLR_OPTS+=("-Dsolr.directoryFactory=HdfsDirectoryFactory")
  SOLR_OPTS+=("-Dsolr.lock.type=hdfs")
  if [ -e "$HADOOP_CONF_DIR" ]; then
    SOLR_OPTS+=("-Dsolr.hdfs.confdir=${HADOOP_CONF_DIR}")
  fi
fi

if [ "${SOLR_DATA_DIR}" != "" ]; then
  SOLR_OPTS+=("-Dsolr.data.dir=$SOLR_DATA_DIR")
 fi

if [ -e "$KEYTAB" ]; then
  SOLR_OPTS+=("-Dsolr.hdfs.security.kerberos.enabled=true")
  SOLR_OPTS+=("-Dsolr.hdfs.security.kerberos.keytabfile=${KEYTAB}")
  SOLR_OPTS+=("-Dsolr.hdfs.security.kerberos.principal=${PRINCIPAL}")
  export JAVA_OPTS="$JAVA_OPTS -Djava.security.auth.login.config=/etc/tomcat/jaas.config -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false"
  template_generator /etc/tomcat/jaas.config.template /etc/tomcat/jaas.config
fi

export SOLR_OPTS

/opt/apache/solr/bin/solr start "${SOLR_OPTS[@]}" -p 8983 -force
/opt/apache/solr/bin/solr create_core -c appcatalog -force
/opt/apache/solr/bin/post -c appcatalog /tmp/samples.xml
if [ -d /etc/hadoop/conf ]; then
  sed -i.bak 's/shared.loader=.*$/shared.loader=\/etc\/hadoop\/conf/g' /etc/tomcat/catalina.properties
fi

if [ -e "$SPNEGO_KEYTAB" ]; then
  sed -i.bak 's/authentication.type=.*$/authentication.type=kerberos/g' /etc/tomcat/catalina.properties
  sed -i.bak 's/simple.anonymous.allowed=.*$/simple.anonymous.allowed=false/g' /etc/tomcat/catalina.properties
  {
    if [ -z "$SPNEGO_PRINCIPAL" ]; then
      echo "kerberos.principal=HTTP/$HOSTNAME"
    else
      echo "kerberos.principal=$SPNEGO_PRINCIPAL"
    fi
    echo "kerberos.keytab=$SPNEGO_KEYTAB"
    echo "hostname=$HOSTNAME"
  } >> /etc/tomcat/catalina.properties
fi
/usr/libexec/tomcat/server start
