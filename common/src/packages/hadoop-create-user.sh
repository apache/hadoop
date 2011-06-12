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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

if [ "$HADOOP_HOME" != "" ]; then
  echo "Warning: \$HADOOP_HOME is deprecated."
  echo
fi

. "$bin"/../libexec/hadoop-config.sh

usage() {
  echo "
usage: $0 <parameters>
  Require parameter:
     -u <username>                                 Create user on HDFS
  Optional parameters:
     -h                                            Display this message
  "
  exit 1
}

# Parse script parameters
if [ $# != 2 ] ; then
    usage
    exit 1
fi

while getopts "hu:" OPTION
do
  case $OPTION in
    u)
      SETUP_USER=$2; shift 2
      ;; 
    h)
      usage
      ;; 
    --)
      shift ; break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1 
      ;;
  esac
done 

# Create user directory on HDFS
export SETUP_USER
export SETUP_PATH=/user/${SETUP_USER}
export HADOOP_PREFIX
export HADOOP_CONF_DIR

su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir ${SETUP_PATH}' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chown ${SETUP_USER}:${SETUP_USER} ${SETUP_PATH}' hdfs

if [ "$?" == "0" ]; then
  echo "User directory has been setup: ${SETUP_PATH}"
fi
