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

function stop {
  echo -n "Shutting down agent..."
  JETTY_PID=`${JPS} | grep ChukwaAgent | grep -v grep | grep -o "[^ ].*" | cut -f 1 -d" "`
  kill -TERM ${JETTY_PID} >&/dev/null
  echo "done"
  exit 0
}

trap stop SIGHUP SIGINT SIGTERM
echo "hadoop jar for agent is " ${HADOOP_JAR}

if [ "X$1" = "Xstop" ]; then
   stop
fi


${JAVA_HOME}/bin/java -Xms32M -Xmx64M -DAPP=agent -Dlog4j.configuration=chukwa-log4j.properties -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -classpath ${CLASSPATH}:${CHUKWA_AGENT}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent $@ &

wait $!
