#!/bin/bash

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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
java=$JAVA_HOME/bin/java

. "$bin"/chukwa-config.sh
if [ ! -d ${CHUKWA_HOME}/opt/apache-tomcat-6.0.16 ]; then
  if [ -f ${CHUKWA_HOME}/opt/apache-tomcat-6.0.16.tar.gz ]; then
    tar fxz ${CHUKWA_HOME}/opt/apache-tomcat-6.0.16.tar.gz -C ${CHUKWA_HOME}/opt
  fi
fi

if [ ! -f ${CHUKWA_HOME}/opt/apache-tomcat-6.0.16/webapps/hicc-${CHUKWA_VERSION}.war ]; then
  if [ -f ${CHUKWA_HOME}/hicc-${CHUKWA_VERSION}.war ]; then
    cp ${CHUKWA_HOME}/hicc-${CHUKWA_VERSION}.war ${CHUKWA_HOME}/opt/apache-tomcat-6.0.16/webapps
  fi
fi 

# start data processors
"$bin"/chukwa-daemon.sh --config $CHUKWA_CONF_DIR --watchdog start processSinkFiles.sh watchdog

# start database admin script
"$bin"/chukwa-daemon.sh --config $CHUKWA_CONF_DIR start dbAdmin.sh
