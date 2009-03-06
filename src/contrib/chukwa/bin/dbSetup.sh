#!/bin/sh
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

pid=$$

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/chukwa-config.sh

EXP_DATE=`date +%Y-%m-%d`
echo -n "SETUP Database partition..."
echo "${pid}" > "$CHUKWA_PID_DIR/dbSetup.pid"
${JAVA_HOME}/bin/java -DCLUSTER=$1 -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 7 #>/dev/null 2>&1
${JAVA_HOME}/bin/java -DCLUSTER=$1 -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 30 >/dev/null 2>&1
${JAVA_HOME}/bin/java -DCLUSTER=$1 -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 91 >/dev/null 2>&1
${JAVA_HOME}/bin/java -DCLUSTER=$1 -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 365 >/dev/null 2>&1
echo "done"
rm -f "$CHUKWA_HOME/var/run/dbSetup.pid"
