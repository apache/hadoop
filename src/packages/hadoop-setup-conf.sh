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

  Optional parameters:
     --auto                                                          Setup path and configuration automatically
     --default                                                       Setup configuration as default
     --conf-dir=/etc/hadoop                                          Set configuration directory
     --datanode-dir=/var/lib/hadoop/hdfs/datanode                    Set datanode directory
     -h                                                              Display this message
     --jobtracker-url=hostname:9001                                  Set jobtracker url
     --log-dir=/var/log/hadoop                                       Set log directory
     --pid-dir=/var/run/hadoop                                       Set pid directory
     --hdfs-dir=/var/lib/hadoop/hdfs                                 Set hdfs directory
     --mapred-dir=/var/lib/hadoop/mapred                             Set mapreduce directory
     --namenode-dir=/var/lib/hadoop/hdfs/namenode                    Set namenode directory
     --namenode-url=hdfs://hostname:9000/                            Set namenode url
     --replication=3                                                 Set replication factor
     --taskscheduler=org.apache.hadoop.mapred.JobQueueTaskScheduler  Set task scheduler
  "
  exit 1
}

template_generator() {
  REGEX='(\$\{[a-zA-Z_][a-zA-Z_0-9]*\})'
  cat $1 |
  while read line ; do
    while [[ "$line" =~ $REGEX ]] ; do
      LHS=${BASH_REMATCH[1]}
      RHS="$(eval echo "\"$LHS\"")"
      line=${line//$LHS/$RHS}
    done
    echo $line >> $2
  done
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'auto' \
  -l 'conf-dir:' \
  -l 'default' \
  -l 'hdfs-dir:' \
  -l 'namenode-dir:' \
  -l 'datanode-dir:' \
  -l 'mapred-dir:' \
  -l 'namenode-url:' \
  -l 'jobtracker-url:' \
  -l 'log-dir:' \
  -l 'pid-dir:' \
  -l 'replication:' \
  -l 'taskscheduler:' \
  -o 'h' \
  -- "$@") 
  
if [ $? != 0 ] ; then
    usage
fi

# Make sure the HADOOP_LOG_DIR is not picked up from user environment.
unset HADOOP_LOG_DIR
  
eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --auto)
      AUTOSETUP=1
      AUTOMATED=1
      shift
      ;; 
    --conf-dir)
      HADOOP_CONF_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --default)
      AUTOMATED=1; shift
      ;;
    -h)
      usage
      ;; 
    --hdfs-dir)
      HADOOP_HDFS_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --namenode-dir)
      HADOOP_NN_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --datanode-dir)
      HADOOP_DN_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --mapred-dir)
      HADOOP_MAPRED_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --namenode-url)
      HADOOP_NN_HOST=$2; shift 2
      AUTOMATED=1
      ;; 
    --jobtracker-url)
      HADOOP_JT_HOST=$2; shift 2
      AUTOMATED=1
      ;; 
    --log-dir)
      HADOOP_LOG_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --pid-dir)
      HADOOP_PID_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --replication)
      HADOOP_REPLICATION=$2; shift 2
      AUTOMATED=1
      ;; 
    --taskscheduler)
      HADOOP_TASK_SCHEDULER=$2; shift 2
      AUTOMATED=1
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

AUTOSETUP=${AUTOSETUP:-1}
JAVA_HOME=${JAVA_HOME:-/usr/java/default}
HADOOP_NN_HOST=${HADOOP_NN_HOST:-hdfs://`hostname`:9000/}
HADOOP_NN_DIR=${HADOOP_NN_DIR:-/var/lib/hadoop/hdfs/namenode}
HADOOP_DN_DIR=${HADOOP_DN_DIR:-/var/lib/hadoop/hdfs/datanode}
HADOOP_JT_HOST=${HADOOP_JT_HOST:-`hostname`:9001}
HADOOP_HDFS_DIR=${HADOOP_HDFS_DIR:-/var/lib/hadoop/hdfs}
HADOOP_MAPRED_DIR=${HADOOP_MAPRED_DIR:-/var/lib/hadoop/mapred}
HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/var/log/hadoop}
HADOOP_PID_DIR=${HADOOP_PID_DIR:-/var/log/hadoop}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop}
HADOOP_REPLICATION=${HADOOP_RELICATION:-3}
HADOOP_TASK_SCHEDULER=${HADOOP_TASK_SCHEDULER:-org.apache.hadoop.mapred.JobQueueTaskScheduler}

if [ "${AUTOMATED}" != "1" ]; then
  echo "Setup Hadoop Configuration"
  echo
  echo -n "Where would you like to put config directory? (${HADOOP_CONF_DIR}) "
  read USER_HADOOP_CONF_DIR
  echo -n "Where would you like to put log directory? (${HADOOP_LOG_DIR}) "
  read USER_HADOOP_LOG_DIR
  echo -n "Where would you like to put pid directory? (${HADOOP_PID_DIR}) "
  read USER_HADOOP_PID_DIR
  echo -n "What is the url of the namenode? (${HADOOP_NN_HOST}) "
  read USER_HADOOP_NN_HOST
  echo -n "Where would you like to put namenode data directory? (${HADOOP_NN_DIR}) "
  read USER_HADOOP_NN_DIR
  echo -n "Where would you like to put datanode data directory? (${HADOOP_DN_DIR}) "
  read USER_HADOOP_DN_DIR
  echo -n "What is the url of the jobtracker? (${HADOOP_JT_HOST}) "
  read USER_HADOOP_JT_HOST
  echo -n "Where would you like to put jobtracker/tasktracker data directory? (${HADOOP_MAPRED_DIR}) "
  read USER_HADOOP_MAPRED_DIR
  echo -n "Where is JAVA_HOME directory? (${JAVA_HOME}) "
  read USER_JAVA_HOME
  echo -n "Would you like to create directories/copy conf files to localhost? (Y/n) "
  read USER_AUTOSETUP
  echo
  JAVA_HOME=${USER_USER_JAVA_HOME:-$JAVA_HOME}
  HADOOP_NN_HOST=${USER_HADOOP_NN_HOST:-$HADOOP_NN_HOST}
  HADOOP_NN_DIR=${USER_HADOOP_NN_DIR:-$HADOOP_NN_DIR}
  HADOOP_DN_DIR=${USER_HADOOP_DN_DIR:-$HADOOP_DN_DIR}
  HADOOP_JT_HOST=${USER_HADOOP_JT_HOST:-$HADOOP_JT_HOST}
  HADOOP_HDFS_DIR=${USER_HADOOP_HDFS_DIR:-$HADOOP_HDFS_DIR}
  HADOOP_MAPRED_DIR=${USER_HADOOP_MAPRED_DIR:-$HADOOP_MAPRED_DIR}
  HADOOP_TASK_SCHEDULER=${HADOOP_TASK_SCHEDULER:-org.apache.hadoop.mapred.JobQueueTaskScheduler}
  HADOOP_LOG_DIR=${USER_HADOOP_LOG_DIR:-$HADOOP_LOG_DIR}
  HADOOP_PID_DIR=${USER_HADOOP_PID_DIR:-$HADOOP_PID_DIR}
  HADOOP_CONF_DIR=${USER_HADOOP_CONF_DIR:-$HADOOP_CONF_DIR}
  AUTOSETUP=${USER_AUTOSETUP:-y}
  echo "Review your choices:"
  echo
  echo "Config directory            : ${HADOOP_CONF_DIR}"
  echo "Log directory               : ${HADOOP_LOG_DIR}"
  echo "PID directory               : ${HADOOP_PID_DIR}"
  echo "Namenode url                : ${HADOOP_NN_HOST}"
  echo "Namenode directory          : ${HADOOP_NN_DIR}"
  echo "Datanode directory          : ${HADOOP_DN_DIR}"
  echo "Jobtracker url              : ${HADOOP_JT_HOST}"
  echo "Mapreduce directory         : ${HADOOP_MAPRED_DIR}"
  echo "Task scheduler              : ${HADOOP_TASK_SCHEDULER}"
  echo "JAVA_HOME directory         : ${JAVA_HOME}"
  echo "Create dirs/copy conf files : ${AUTOSETUP}"
  echo
  echo -n "Proceed with generate configuration? (y/N) "
  read CONFIRM
  if [ "${CONFIRM}" != "y" ]; then
    echo "User aborted setup, exiting..."
    exit 1
  fi
fi

rm -f core-site.xml >/dev/null
rm -f hdfs-site.xml >/dev/null
rm -f mapred-site.xml >/dev/null
rm -f hadoop-env.sh >/dev/null

template_generator ${HADOOP_HOME}/templates/conf/core-site.xml core-site.xml
template_generator ${HADOOP_HOME}/templates/conf/hdfs-site.xml hdfs-site.xml
template_generator ${HADOOP_HOME}/templates/conf/mapred-site.xml mapred-site.xml
template_generator ${HADOOP_HOME}/templates/conf/hadoop-env.sh hadoop-env.sh

chown root:hadoop hadoop-env.sh
chmod 755 hadoop-env.sh

if [ "${AUTOSETUP}" == "1" -o "${AUTOSETUP}" == "y" ]; then
  mkdir -p ${HADOOP_HDFS_DIR}
  mkdir -p ${HADOOP_NN_DIR}
  mkdir -p ${HADOOP_DN_DIR}
  mkdir -p ${HADOOP_MAPRED_DIR}
  mkdir -p ${HADOOP_CONF_DIR}
  mkdir -p ${HADOOP_LOG_DIR}
  mkdir -p ${HADOOP_LOG_DIR}/hdfs
  mkdir -p ${HADOOP_LOG_DIR}/mapred
  mkdir -p ${HADOOP_PID_DIR}
  chown hdfs:hadoop ${HADOOP_HDFS_DIR}
  chown hdfs:hadoop ${HADOOP_NN_DIR}
  chown hdfs:hadoop ${HADOOP_DN_DIR}
  chown mapred:hadoop ${HADOOP_MAPRED_DIR}
  chown root:hadoop ${HADOOP_LOG_DIR}
  chmod 775 ${HADOOP_LOG_DIR}
  chmod 775 ${HADOOP_PID_DIR}
  chown hdfs:hadoop ${HADOOP_LOG_DIR}/hdfs
  chown mapred:hadoop ${HADOOP_LOG_DIR}/mapred
  cp -f *.xml ${HADOOP_CONF_DIR}
  cp -f hadoop-env.sh ${HADOOP_CONF_DIR}
  echo "Configuration setup is completed."
  if [[ "$HADOOP_NN_HOST" =~ "`hostname`" ]]; then
    echo "Proceed to run hadoop-setup-hdfs.sh on namenode."
  fi
else
  echo
  echo "Configuration file has been generated, please copy:"
  echo
  echo "core-site.xml"
  echo "hdfs-site.xml"
  echo "mapred-site.xml"
  echo "hadoop-env.sh"
  echo
  echo " to ${HADOOP_CONF_DIR} on all nodes, and proceed to run hadoop-setup-hdfs.sh on namenode."
fi
