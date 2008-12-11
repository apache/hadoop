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

trap 'remove_cron;rm -f $CHUKWA_HOME/var/run/chukwa-$CHUKWA_IDENT_STRING-processSinkFiles.sh.pid ${CHUKWA_HOME}/var/run/ProcessSinkFiles.pid; exit 0' 1 2 15
echo "${pid}" > "$CHUKWA_HOME/var/run/ProcessSinkFiles.pid"

HADOOP_CMDE="${HADOOP_HOME}/bin/hadoop "

function remove_cron {
    mkdir -p ${CHUKWA_HOME}/var/tmp >&/dev/null
    crontab -l | grep -v ${CHUKWA_HOME}/bin/hourlyRolling.sh > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    cat /tmp/cron.${CURRENT_DATE} | grep -v ${CHUKWA_HOME}/bin/dailyRolling.sh > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
    crontab ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
    rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
}

function add_cron {
    mkdir -p ${CHUKWA_HOME}/var/tmp >&/dev/null
    crontab -l > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    crontest=$?

    if [ "X${crontest}" != "X0" ]; then
      cat > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} << CRON
16 * * * * ${CHUKWA_HOME}/bin/hourlyRolling.sh >& ${CHUKWA_HOME}/logs/hourly.log
30 1 * * * ${CHUKWA_HOME}/bin/dailyRolling.sh >& ${CHUKWA_HOME}/logs/dailyRolling.log
CRON
    else
      grep -v "${CHUKWA_HOME}/bin/hourlyRolling.sh" ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}  | grep -v "${CHUKWA_HOME}/bin/dailyRolling.sh" > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
      mv ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2 ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
      cat >> ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} << CRON
16 * * * * ${CHUKWA_HOME}/bin/hourlyRolling.sh >& ${CHUKWA_HOME}/logs/hourly.log
30 1 * * * ${CHUKWA_HOME}/bin/dailyRolling.sh >& ${CHUKWA_HOME}/logs/dailyRolling.log
CRON
    fi

    # save crontab
    echo -n "Registering cron jobs.."
    crontab ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} > /dev/null 2>&1
    rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    echo "done"
}

if [ "X$1" = "Xstop" ]; then
  echo -n "Shutting down processSinkFiles.sh..."
  kill -TERM `cat ${CHUKWA_HOME}/var/run/ProcessSinkFiles.pid`
  echo "done"
  exit 0
fi

if [ "X$1" = "Xwatchdog" ]; then
  add_cron
fi

while [ 1 ]
 do
  debugDate=`date `
  startTime=`date +%s`
  now=`date +%Y%m%d_%H_%M%S`
  strDate=`date +%Y%m%d_%H_%M%S`
  srcDoneHdfsDir="/chukwa/postprocess/srcSink$now/"
  
  
  destArchiveDir=`date +%Y%m%d/%H/%M%S`

  echo "Running $strDate $now" >> "${CHUKWA_LOG_DIR}/mr.log"

  echo "srcDoneHdfsDir: $srcDoneHdfsDir " >> "${CHUKWA_LOG_DIR}/mr.log"

  $HADOOP_CMDE dfs -mkdir $srcDoneHdfsDir/doneFile
  echo "done with mkdir" >> "${CHUKWA_LOG_DIR}/mr.log"
 
  $HADOOP_CMDE dfs -mv "/chukwa/logs/*/*.done" ${srcDoneHdfsDir}/doneFile
  endMoveTime=`date +%s`
  moveDuration=$(( $endMoveTime - $startTime))
  echo "moveDuration $moveDuration" >> "${CHUKWA_LOG_DIR}/mr.log"
  debugDate=`date `
  echo "$debugDate done with mv logs" >> "${CHUKWA_LOG_DIR}/mr.log"
 
  # Build the archive
  $HADOOP_CMDE jar  ${CHUKWA_CORE} org.apache.hadoop.chukwa.extraction.archive.ChukwaArchiveBuilder Stream ${srcDoneHdfsDir}/doneFile /chukwa/archives/raw/${destArchiveDir}
  endArchiveTime=`date +%s`
  archiveDuration=$(( $endArchiveTime - $endMoveTime))
  echo "archiveDuration $archiveDuration" >> "${CHUKWA_LOG_DIR}/mr.log"
  debugDate=`date `
  echo "$debugDate done with chuckwaArchiveBuilder" >> "${CHUKWA_LOG_DIR}/mr.log"
  
  
  ## Archive available call all processors
  
  
  $HADOOP_CMDE jar  ${CHUKWA_CORE} org.apache.hadoop.chukwa.extraction.demux.Demux -Dmapred.compress.map.output=true -Dmapred.map.output.compression.codec=org.apache.hadoop.io.compress.LzoCodec -Dmapred.output.compress=true -Dmapred.output.compression.type=BLOCK -r 4 /chukwa/archives/raw/${destArchiveDir} ${srcDoneHdfsDir}/demux
  endDemuxTime=`date +%s`
  demuxDuration=$(( $endDemuxTime - $endArchiveTime))
  echo "demuxDuration $demuxDuration" >> "${CHUKWA_LOG_DIR}/mr.log"
  debugDate=`date `
  echo "$debugDate done with demux job" >> "${CHUKWA_LOG_DIR}/mr.log"
   
  ${JAVA_HOME}/bin/java -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -Dlog4j.configuration=log4j.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${tools}:${CHUKWA_HOME}/conf org.apache.hadoop.chukwa.extraction.database.DatabaseLoader "${srcDoneHdfsDir}/demux" SystemMetrics Df Hadoop_dfs Hadoop_jvm Hadoop_mapred Hadoop_rpc MSSRGraph MRJobCounters NodeActivity HodJob HodMachine Hadoop_dfs_FSDirectory Hadoop_dfs_FSNamesystem Hadoop_dfs_datanode Hadoop_dfs_namenode Hadoop_jvm_metrics Hadoop_mapred_job Hadoop_mapred_jobtracker Hadoop_mapred_shuffleOutput Hadoop_mapred_tasktracker Hadoop_rpc_metrics
  endDbLoaderTime=`date +%s`
  dbLoaderDuration=$(( $endDbLoaderTime - $endDemuxTime))
  echo "dbLoaderDuration $dbLoaderDuration" >> "${CHUKWA_LOG_DIR}/mr.log"
  debugDate=`date `
  echo "$debugDate done with dbLoader job" >> "${CHUKWA_LOG_DIR}/mr.log"
   
  $HADOOP_CMDE jar ${CHUKWA_CORE} org.apache.hadoop.chukwa.extraction.demux.MoveToRepository ${srcDoneHdfsDir}/demux ${chuwaRecordsRepository}
  endMoveToRepoTime=`date +%s`
  moveToRepoDuration=$(( $endMoveToRepoTime - $endDbLoaderTime))
  echo "moveToRepoDuration $moveToRepoDuration" >> "${CHUKWA_LOG_DIR}/mr.log"
  debugDate=`date `
  echo "$debugDate done with MoveToRepository" >> "${CHUKWA_LOG_DIR}/mr.log"
  
  now=`date +%s`
  strDate=`date +%m/%d/%y%n`
  debugDate=`date `
  echo "$debugDate Stopping ${strDate} ${now}" >> "${CHUKWA_LOG_DIR}/mr.log"
  
  endTime=`date +%s`
  duration=$(( $endTime - $startTime))
  echo "Duration: $duration s" >> "${CHUKWA_LOG_DIR}/mr.log"
  
  if [ $duration -lt 300 ]; then
   sleepTime=$(( 300 - $duration)) 
   echo "Sleep: $sleepTime s" >> "${CHUKWA_LOG_DIR}/mr.log"
   SLEEP_COUNTER=`expr $sleepTime / 5`
   while [ $SLEEP_COUNTER -gt 1 ]; do
       sleep 5
       SLEEP_COUNTER=`expr $SLEEP_COUNTER - 1`
   done
  fi
done

