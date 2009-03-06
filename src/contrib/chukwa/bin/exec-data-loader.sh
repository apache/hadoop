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

. "$bin"/chukwa-config.sh

JVM_OPTS="-Xms4M -Xmx4M"

PARM=$1

function clean_up {
  echo -n "Shutting down $PARM..."
  if [ "X$PARM" == "Xsar" ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Sar-data-loader.pid`
  fi
  if [ "X$PARM" == "Xiostat" ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Iostat-data-loader.pid`
  fi
  if [ "X$PARM" == "Xtop" ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Top-data-loader.pid`
  fi
  if [ "X$PARM" == "Xdf" ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Df-data-loader.pid`
  fi
  if [ "X$PARM" == "Xnetstat" ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Netstat-data-loader.pid`
  fi
  if [ "X$PARM" == "Xpbsnodes" ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/PbsNodes-data-loader.pid`
  fi
  if [ "X$PARM" == "Xtorque" ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/TorqueDataLoader.pid`
  fi
  echo "done"
  exit 0
}

trap clean_up SIGHUP SIGINT SIGTERM

export SAR="sar -q -r -n ALL 55"
RELEASE=`lsb_release -r | cut -b 10-`
if [ "X${RELEASE}" = "X4" ]; then 
    export SAR="sar -q -r -n FULL 55"
fi

if [ "X$PARM" == "Xsar" ]; then
    ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Sar -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec $SAR &
fi

if [ "X$PARM" == "Xiostat" ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Iostat -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec iostat -x -k 55 2 &
fi

if [ "X$PARM" == "Xtop" ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Top -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec top -b -n 1 -c &
fi

if [ "X$PARM" == "Xdf" ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Df -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec df -l &
fi

if [ "X$PARM" == "Xnetstat" ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Netstat -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec ${CHUKWA_HOME}/bin/netstat.sh &
fi

if [ "X$PARM" == "Xpbsnodes" ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=PbsNodes -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec "${nodeActivityCmde}" &
fi

if [ "X$PARM" == "Xtorque" ]; then
  ${JAVA_HOME}/bin/java -DDOMAIN=${DOMAIN} -DTORQUE_SERVER=${TORQUE_SERVER} -DTORQUE_HOME=${TORQUE_HOME} -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Torque -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.mdl.TorqueDataLoader &
fi

pid=$!
wait $pid
