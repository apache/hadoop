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

trap 'stop; exit 0' 1 2 15

function stop {
  echo -n "Shutting down Collector..."
  ${JPS} | grep CollectorStub | grep -v grep | grep -o '[^ ].*'| cut -f 1 -d" " | xargs kill -TERM >&/dev/null
  echo "done"
  exit 0
}

if [ "X$1" = "Xstop" ]; then
  stop
fi

${JAVA_HOME}/bin/java -DAPP=collector -Dlog4j.configuration=chukwa-log4j.properties -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.datacollection.collector.CollectorStub $@
