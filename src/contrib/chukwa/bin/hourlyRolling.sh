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

HADOOP_OPTS="$HADOOP_OPTS -DAPP=hourlyRolling -Dlog4j.configuration=chukwa-log4j.properties -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} "
export HADOOP_OPTS
export HADOOP_CONF_DIR
HADOOP_CMDE="${HADOOP_HOME}/bin/hadoop "

  $HADOOP_CMDE jar ${CHUKWA_CORE} org.apache.hadoop.chukwa.extraction.demux.HourlyChukwaRecordRolling rollInSequence true deleteRawdata true


