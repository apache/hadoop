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

echo "${pid}" > "$CHUKWA_HOME/var/run/buildDailyArchive.pid"

HADOOP_CONF_DIR="${HADOOP_HOME}/conf/"
HADOOP_CMDE="${HADOOP_HOME}/bin/hadoop "

while [ 1 ]
 do
  now=`date +%s`
  strDate=`date +%m/%d/%y%n`
  srcHourly="/chukwa/postprocess/srcDaily$now/"

  echo "Running $strDate $now" >> "${CHUKWA_LOG_DIR}/daily.log"

  echo "srcHourly: $srcHourly " >> "${CHUKWA_LOG_DIR}/daily.log"

  $HADOOP_CMDE dfs -mkdir $srcHourly/hourly
  echo "done with mkdir" >> "${CHUKWA_LOG_DIR}/daily.log"
 
  $HADOOP_CMDE dfs -mv "/chukwa/archives/hourly/*.arc" ${srcHourly}/hourly/
  echo "done with mv archives" >> "${CHUKWA_LOG_DIR}/daily.log"
 
  # Build the archive
  $HADOOP_CMDE jar ${CHUKWA_CORE} org.apache.hadoop.chukwa.extraction.archive.ChuckwaArchiveBuilder Daily $srcHourly/hourly $srcHourly/daily
  echo "done with chuckwaArchiveBuilder" >> "${CHUKWA_LOG_DIR}/daily.log"
  
   ## Hourly Archive available call all processors
   ##############  ############## 
  
   ##############  ############## 
  
  
  ############## MERGE or MOVE ##############
  
  ############## MERGE or MOVE ##############
  
  
  now=`date +%s`
  strDate=`date +%m/%d/%y%n`
  echo "Stopping ${strDate} ${now}" >> "${CHUKWA_LOG_DIR}/daily.log"

  sleep 36000
done
