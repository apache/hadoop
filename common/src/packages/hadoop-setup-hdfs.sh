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
     -c <clusterid>                                Set cluster identifier for HDFS
  Optional parameters:
     -h                                            Display this message
  "
  exit 1
}

if [ $# != 2 ] ; then
    usage
    exit 1
fi

while getopts "hc:" OPTION
do
  case $OPTION in
    c)
      SETUP_CLUSTER=$2; shift 2
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

export HADOOP_PREFIX
export HADOOP_CONF_DIR
export SETUP_CLUSTER

# Start namenode and initialize file system structure
echo "Setup Hadoop Distributed File System"
echo
echo "Formatting namenode"
echo
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} namenode -format -clusterid ${SETUP_CLUSTER}' hdfs
echo
echo "Starting namenode process"
echo
/etc/init.d/hadoop-namenode start
echo
echo "Initialize HDFS file system: "
echo

su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir /jobtracker' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chown mapred:mapred /jobtracker' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir /user/mapred' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chown mapred:mapred /user/mapred' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir /tmp' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chmod 777 /tmp' hdfs

if [ $? -eq 0 ]; then
  echo "Completed."
else
  echo "Unknown error occurred, check hadoop logs for details."
fi

echo
echo "Please startup datanode processes: /etc/init.d/hadoop-datanode start"
