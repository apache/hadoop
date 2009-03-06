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

if [ "$CHUKWA_IDENT_STRING" = "" ]; then
  export CHUKWA_IDENT_STRING="$USER"
fi

trap 'rm -f $CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-dbAdmin.sh.pid ${CHUKWA_PID_DIR}/dbAdmin.pid; exit 0' 1 2 15

CHUKWA_OPTS="-DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml"
CLASS_OPTS="-classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR}"
JVM_OPTS="-DAPP=dbAdmin -Dlog4j.configuration=chukwa-log4j.properties ${CHUKWA_OPTS} ${CLASS_OPTS}"

echo "${pid}" > "${CHUKWA_PID_DIR}/dbAdmin.pid"
while [ 1 ]
  do
    EXP_DATE=`date +%Y-%m-%d`
    start=`date +%s`
    cat ${CHUKWA_CONF_DIR}/jdbc.conf | \
    while read LINE; do
        CLUSTER=`echo ${LINE} | cut -f 1 -d'='`
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 7 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 30 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 91 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 365 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 3650 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.Aggregator &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.DataExpiration ${EXP_DATE} 7 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.DataExpiration ${EXP_DATE} 30 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.DataExpiration ${EXP_DATE} 91 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.DataExpiration ${EXP_DATE} 365 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.DataExpiration ${EXP_DATE} 3650 &
        ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} -Dlog4j.configuration=${CHUKWA_CONF_DIR}/nagios-alert.properties ${CHUKWA_OPTS} ${CLASS_OPTS} org.apache.hadoop.chukwa.util.WatchDog &
    done
    end=`date +%s`
    duration=$(( $end - $start ))
    if [ $duration -lt 300 ]; then
        sleep=$(( 300 - $duration ))
        SLEEP_COUNTER=`expr $sleep / 5`
        while [ $SLEEP_COUNTER -gt 1 ]; do
            sleep 5
            SLEEP_COUNTER=`expr $SLEEP_COUNTER - 1`
        done
    fi
done
