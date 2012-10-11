#!/usr/bin/env bash

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

# included in all the mapred scripts with source command
# should not be executed directly

bin=`which "$0"`
bin=`dirname "${bin}"`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
if [ -e "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]; then
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
elif [ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh" ]; then
  . "$HADOOP_COMMON_HOME"/libexec/hadoop-config.sh
elif [ -e "${HADOOP_COMMON_HOME}/bin/hadoop-config.sh" ]; then
  . "$HADOOP_COMMON_HOME"/bin/hadoop-config.sh
elif [ -e "${HADOOP_HOME}/bin/hadoop-config.sh" ]; then
  . "$HADOOP_HOME"/bin/hadoop-config.sh
elif [ -e "${HADOOP_MAPRED_HOME}/bin/hadoop-config.sh" ]; then
  . "$HADOOP_MAPRED_HOME"/bin/hadoop-config.sh
else
  echo "Hadoop common not found."
  exit
fi

# Only set locally to use in HADOOP_OPTS. No need to export.
# The following defaults are useful when somebody directly invokes bin/mapred.
HADOOP_MAPRED_LOG_DIR=${HADOOP_MAPRED_LOG_DIR:-${HADOOP_MAPRED_HOME}/logs}
HADOOP_MAPRED_LOGFILE=${HADOOP_MAPRED_LOGFILE:-hadoop.log}
HADOOP_MAPRED_ROOT_LOGGER=${HADOOP_MAPRED_ROOT_LOGGER:-INFO,console}

HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.dir=$HADOOP_MAPRED_LOG_DIR"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.file=$HADOOP_MAPRED_LOGFILE"
export HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.root.logger=${HADOOP_MAPRED_ROOT_LOGGER}"


