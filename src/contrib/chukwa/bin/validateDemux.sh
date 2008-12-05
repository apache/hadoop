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

echo "hadoop jar for agent is " ${HADOOP_JAR}
now=`date +%Y%m%d_%H_%M%S`
hdfsDir="/test_$now/"

HADOOP_CMDE="${HADOOP_HOME}/bin/hadoop "

$HADOOP_CMDE dfs -mkdir ${hdfsDir}

echo "Moving data to HDFS: ${hdfsDir}"

$HADOOP_CMDE dfs -put ${CHUKWA_HOME}/data/demuxData ${hdfsDir}/

echo "demuxTestData: "
$HADOOP_CMDE dfs -ls ${hdfsDir}/demuxData/input
exitCode=$?
echo "ls ExitCode: ${exitCode} "
 
$HADOOP_CMDE jar  ${CHUKWA_CORE} org.apache.hadoop.chukwa.extraction.demux.Demux -Dmapred.compress.map.output=true -Dmapred.map.output.compression.codec=org.apache.hadoop.io.compress.LzoCodec -Dmapred.output.compress=true -Dmapred.output.compression.type=BLOCK -r 4 ${hdfsDir}/demuxData/input ${hdfsDir}/demuxData/output
exitCode=$?
echo "Demux ExitCode: ${exitCode} "

${JAVA_HOME}/bin/java -Xms10M -Xmx32M -classpath /tmp/chukwaTest.jar:${CLASSPATH}:${HADOOP_JAR}:${COMMON} org.apache.hadoop.chukwa.validationframework.DemuxDirectoryValidator -hdfs ${hdfsDir}/demuxData/gold ${hdfsDir}/demuxData/output
exitCode=$?
echo "Validation ExitCode: ${exitCode} "

