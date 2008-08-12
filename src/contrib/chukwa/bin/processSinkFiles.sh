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

echo "${pid}" > "${CHUKWA_PID_DIR}/ProcessSinkFiles.pid"

HADOOP_CONF_DIR="${HADOOP_HOME}/conf/"
HADOOP_CMDE="${HADOOP_HOME}/bin/hadoop "

while [ 1 ]
do 
  
  
  now=`date +%s`
  strDate=`date +%m/%d/%y%n`
  srcDoneHdfsDir="/chukwa/tmp/srcDone$now/"
  srcEventHdfsDir="/chukwa/tmp/srcEvent$now/"

  echo "Running $strDate $now" >> "${CHUKWA_LOG_DIR}/mr.log"

  echo "srcDoneHdfsDir: $srcDoneHdfsDir srcEventHdfsDir: $srcEventHdfsDir" >> "${MR_LOG}"

  $HADOOP_CMDE dfs -mkdir $srcDoneHdfsDir
  echo "done with mkdir" >> "${CHUKWA_LOG_DIR}/mr.log"
 
  $HADOOP_CMDE dfs -mv "/chukwa/logs/*/*.done" $srcDoneHdfsDir
  echo "done with mv logs" >> "${CHUKWA_LOG_DIR}/mr.log"
 
  $HADOOP_CMDE jar ${chukwaCore} org.apache.hadoop.chukwa.extraction.demux.Demux -r 2 $srcDoneHdfsDir $srcEventHdfsDir
  echo "done with demux job" >> "${CHUKWA_LOG_DIR}/mr.log"
 
  $HADOOP_CMDE jar ${chukwaCore} org.apache.hadoop.chukwa.extraction.demux.MoveOrMergeLogFile $srcEventHdfsDir ${chuwaRecordsRepository}
  echo "done with MoveOrMergeLogFile" >> "${CHUKWA_LOG_DIR}/mr.log"

  now=`date +%s`
  strDate=`date +%m/%d/%y%n`
  echo "Stopping ${strDate} ${now}" >> "${CHUKWA_LOG_DIR}/mr.log"

 sleep 300

done
