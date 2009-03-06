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

java=$JAVA_HOME/bin/java

if [ "X$1" = "Xstop" ]; then
  echo -n "Shutting down Torque Data Loader..."
  if [ -f ${CHUKWA_PID_DIR}/TorqueDataLoader.pid ]; then
    kill -TERM `cat ${CHUKWA_PID_DIR}/TorqueDataLoader.pid`
    rm -f ${CHUKWA_PID_DIR}/TorqueDataLoader.pid
  fi
  echo "done"
  exit 0
fi

min=`date +%M`


# start torque data loader
pidFile=$CHUKWA_PID_DIR/TorqueDataLoader.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep TorqueDataLoader | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      ${java} -DDOMAIN=${DOMAIN} -DTORQUE_SERVER=${TORQUE_SERVER} -DTORQUE_HOME=${TORQUE_HOME} -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Torque -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.mdl.TorqueDataLoader&
  fi 
else
      ${java} -DDOMAIN=${DOMAIN} -DTORQUE_SERVER=${TORQUE_SERVER} -DTORQUE_HOME=${TORQUE_HOME} -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Torque -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.mdl.TorqueDataLoader&
fi
