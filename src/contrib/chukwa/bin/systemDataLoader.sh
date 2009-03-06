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

trap 'shutdown' 1 2 15

function status {
  EXISTS=0
  RESULT=0
  pidFile="${CHUKWA_PID_DIR}/Sar-data-loader.pid"
  if [ -f $pidFile ]; then
    pid=`head ${pidFile}`
    ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
    if [ $ChildPIDRunningStatus -ge 1 ]; then
      EXISTS=1
    fi
  fi

  if [ ${EXISTS} -lt 1 ]; then
    echo "sar data loader is stopped."
    RESULT=1
  else
    echo "sar data loader is running."
  fi

  EXISTS=0
  pidFile="${CHUKWA_PID_DIR}/Iostat-data-loader.pid"
  if [ -f $pidFile ]; then
    pid=`head ${pidFile}`
    ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
    if [ $ChildPIDRunningStatus -ge 1 ]; then
      EXISTS=1
    fi
  fi

  if [ ${EXISTS} -lt 1 ]; then
    echo "iostat data loader is stopped."
    RESULT=1
  else
    echo "iostat data loader is running."
  fi

  EXISTS=0
  pidFile="${CHUKWA_PID_DIR}/Top-data-loader.pid"
  if [ -f $pidFile ]; then
    pid=`head ${pidFile}`
    ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
    if [ $ChildPIDRunningStatus -ge 1 ]; then
      EXISTS=1
    fi
  fi

  if [ ${EXISTS} -lt 1 ]; then
    echo "top data loader is stopped."
    RESULT=1
  else
    echo "top data loader is running."
  fi

  EXISTS=0
  pidFile="${CHUKWA_PID_DIR}/Df-data-loader.pid"
  if [ -f $pidFile ]; then
    pid=`head ${pidFile}`
    ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
    if [ $ChildPIDRunningStatus -ge 1 ]; then
      EXISTS=1
    fi
  fi

  if [ ${EXISTS} -lt 1 ]; then
    echo "df data loader is stopped."
    RESULT=1
  else
    echo "df data loader is running."
  fi

  EXISTS=0
  pidFile="${CHUKWA_PID_DIR}/Netstat-data-loader.pid"
  if [ -f $pidFile ]; then
    pid=`head ${pidFile}`
    ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
    if [ $ChildPIDRunningStatus -ge 1 ]; then
      EXISTS=1
    fi
  fi

  if [ ${EXISTS} -lt 1 ]; then
    echo "netstat data loader is stopped."
    RESULT=1
  else
    echo "netstat data loader is running."
  fi

  exit $RESULT
}

function shutdown {
  echo -n "Shutting down System Data Loader..."
  if [ -f ${CHUKWA_PID_DIR}/Sar-data-loader.pid ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Sar-data-loader.pid`
  fi
  if [ -f ${CHUKWA_PID_DIR}/Iostat-data-loader.pid ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Iostat-data-loader.pid`
  fi
  if [ -f ${CHUKWA_PID_DIR}/Top-data-loader.pid ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Top-data-loader.pid`
  fi
  if [ -f ${CHUKWA_PID_DIR}/Df-data-loader.pid ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Df-data-loader.pid`
  fi
  if [ -f ${CHUKWA_PID_DIR}/Netstat-data-loader.pid ]; then
    kill -9 `cat ${CHUKWA_PID_DIR}/Netstat-data-loader.pid`
  fi
  rm -f $CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-systemDataLoader.sh.pid
  echo "done"
  exit 0
}

if [ "X$1" = "Xstatus" ]; then
  status
fi

if [ "X$1" = "Xstop" ]; then
  shutdown
fi

echo -n "Starting System Data Loader..."

export SAR="sar -q -r -n ALL 55"
RELEASE=`lsb_release -r | cut -b 10-`
if [ "X${RELEASE}" = "X4" ]; then 
    export SAR="sar -q -r -n FULL 55"
fi

#test=`grep -q SysLog ${CHUKWA_HOME}/var/chukwa_checkpoint*`
#if [ "X${test}"="X1" ]; then
#  echo "add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped SysLog 0 /var/log/messages 0" | nc localhost 9093 >&/dev/null & disown -h 
#fi

EXISTS=0
pidFile="${CHUKWA_PID_DIR}/Sar-data-loader.pid"
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -ge 1 ]; then
    EXISTS=1
  fi
fi

if [ ${EXISTS} -lt 1 ]; then
    ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Sar -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec $SAR &
fi

EXISTS=0
pidFile="${CHUKWA_PID_DIR}/Iostat-data-loader.pid"
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -ge 1 ]; then
    EXISTS=1
  fi
fi

if [ ${EXISTS} -lt 1 ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Iostat -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec iostat -x -k 55 2 &
fi

EXISTS=0
pidFile="${CHUKWA_PID_DIR}/Top-data-loader.pid"
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -ge 1 ]; then
    EXISTS=1
  fi
fi

if [ ${EXISTS} -lt 1 ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Top -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec top -b -n 1 -c &
fi

EXISTS=0
pidFile="${CHUKWA_PID_DIR}/Df-data-loader.pid"
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -ge 1 ]; then
    EXISTS=1
  fi
fi

if [ ${EXISTS} -lt 1 ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Df -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec df -l &
fi

EXISTS=0
pidFile="${CHUKWA_PID_DIR}/Netstat-data-loader.pid"
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep ${pid} | grep Exec | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -ge 1 ]; then
    EXISTS=1
  fi
fi

if [ ${EXISTS} -lt 1 ]; then
  ${JAVA_HOME}/bin/java $JVM_OPTS -DPERIOD=60 -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DRECORD_TYPE=Netstat -Dlog4j.configuration=system-data-loader.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${TOOLS}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.inputtools.plugin.metrics.Exec ${CHUKWA_HOME}/bin/netstat.sh &
fi

echo "done"

