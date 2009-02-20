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
jps=$JAVA_HOME/bin/jps


min=`date +%M`


# start torque data loader
pidFile=$CHUKWA_HOME/var/run/TorqueDataLoader.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${jps} | grep ${pid} | grep TorqueDataLoader | grep -v grep | wc -l`
  #ChildPIDRunningStatus=`ps -ef | grep TorqueDataLoader | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=torque.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.TorqueDataLoader&
  fi 
else
      ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=torque.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.TorqueDataLoader&
fi
# start util data loader
pidFile=$CHUKWA_HOME/var/run/UtilDataLoader.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${jps} | grep ${pid} | grep UtilDataLoader | grep -v grep | wc -l`
  #ChildPIDRunningStatus=`ps -ef | grep UtilDataLoader | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=util.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.UtilDataLoader&
  fi
else 
      ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=util.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.UtilDataLoader&
fi

# start queue info data loader
pidFile=$CHUKWA_HOME/var/run/QueueInfoDataLoader.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${jps} | grep ${pid} | grep QueueInfoDataLoader | grep -v grep | wc -l`
  #ChildPIDRunningStatus=`ps -ef | grep QueueInfoDataLoader | grep -v grep | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=queueinfo.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.QueueInfoDataLoader&
  fi
else
      ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=queueinfo.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.QueueInfoDataLoader&
fi

# start map reduce log data loader
tenmin=`echo ${min} | cut -b 2-`
if [ "X${tenmin}" == "X0" ]; then
    pidFile=$CHUKWA_HOME/var/run/JobLogDataLoader.pid
    if [ -f $pidFile ]; then
        pid=`head ${pidFile}`
        ChildPIDRunningStatus=`${jps} | grep ${pid} | grep JobLogDataLoader |  wc -l`
        if [ $ChildPIDRunningStatus -lt 1 ]; then
            ${java} -Xms128m -Xmx1280m -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=joblog.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.JobLogDataLoader &
        fi
    else
        ${java} -Xms128m -Xmx1280m -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=joblog.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.mdl.JobLogDataLoader &
    fi
fi

# start node activity plugin
tenmin=`echo ${min} | cut -b 2-`
if [ "X${tenmin}" == "X0" ]; then
  pidFile=$CHUKWA_HOME/var/run/NodeActivityPlugin.pid
  if [ -f $pidFile ]; then
    pid=`head ${pidFile}`
    ChildPIDRunningStatus=`${jps} | grep ${pid} | grep NodeActivityMDL | wc -l`
    if [ $ChildPIDRunningStatus -lt 1 ]; then
       ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=nodeActivity.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.plugin.nodeactivity.NodeActivityMDL&
    fi
  else
      ${java} -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=nodeActivity.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${common} org.apache.hadoop.chukwa.sources.plugin.nodeactivity.NodeActivityMDL&
  fi
fi

# start database summary loader
tenmin=`echo ${min} | cut -b 2-`
if [ "X${tenmin}" == "X0" ]; then
    pidFile=$CHUKWA_HOME/var/run/DBSummaryLoader.pid
    if [ -f $pidFile ]; then
        pid=`head ${pidFile}`
        ChildPIDRunningStatus=`${jps} | grep ${pid} | grep DBSummaryLoader | wc -l`
        if [ $ChildPIDRunningStatus -lt 1 ]; then
            ${java} -Xms128m -Xmx1280m -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=log4j.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${ckit}:${common} org.apache.hadoop.chukwa.extraction.DBSummaryLoader &
        fi
    else
        ${java} -Xms128m -Xmx1280m -DCHUKWA_HOME=${CHUKWA_HOME} -Dlog4j.configuration=log4j.properties -classpath ${CLASSPATH}:${chukwa}:${ikit}:${ckit}:${common} org.apache.hadoop.chukwa.extraction.DBSummaryLoader &
    fi
fi

