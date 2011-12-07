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

this="${BASH_SOURCE-$0}"
export HADOOP_PREFIX=`dirname "$this"`/..

usage() {
  echo "
usage: $0 <parameters>

  Optional parameters:
     --auto                                                          Setup path and configuration automatically
     --default                                                       Setup configuration as default
     --conf-dir=/etc/hadoop                                          Set configuration directory
     --datanode-dir=/var/lib/hadoop/hdfs/datanode                    Set datanode directory
     --group=hadoop                                                  Set Hadoop group name
     -h                                                              Display this message
     --hdfs-user=hdfs                                                Set HDFS user
     --jobtracker-host=hostname                                      Set jobtracker host
     --namenode-host=hostname                                        Set namenode host
     --secondarynamenode-host=hostname                               Set secondary namenode host
     --kerberos-realm=KERBEROS.EXAMPLE.COM                           Set Kerberos realm
     --kinit-location=/usr/kerberos/bin/kinit                        Set kinit location
     --keytab-dir=/etc/security/keytabs                              Set keytab directory
     --log-dir=/var/log/hadoop                                       Set log directory
     --pid-dir=/var/run/hadoop                                       Set pid directory
     --hdfs-dir=/var/lib/hadoop/hdfs                                 Set HDFS directory
     --hdfs-user-keytab=/home/hdfs/hdfs.keytab                       Set HDFS user key tab
     --mapred-dir=/var/lib/hadoop/mapred                             Set mapreduce directory
     --mapreduce-user=mr                                             Set mapreduce user
     --mapreduce-user-keytab=/home/mr/hdfs.keytab                    Set mapreduce user key tab
     --namenode-dir=/var/lib/hadoop/hdfs/namenode                    Set namenode directory
     --replication=3                                                 Set replication factor
     --taskscheduler=org.apache.hadoop.mapred.JobQueueTaskScheduler  Set task scheduler
     --datanodes=hostname1,hostname2,...                             SET the datanodes
     --tasktrackers=hostname1,hostname2,...                          SET the tasktrackers
     --dfs-webhdfs-enabled=false|true                                Enable webhdfs
     --dfs-support-append=false|true                                 Enable append
     --hadoop-proxy-users='user1:groups:hosts;user2:groups:hosts'    Setup proxy users for hadoop
     --hbase-user=hbase                                              User which hbase is running as. Defaults to hbase
     --mapred-cluster-map-memory-mb=memory                           Virtual memory of a map slot for the MR framework. Defaults to -1
     --mapred-cluster-reduce-memory-mb=memory                        Virtual memory, of a reduce slot for the MR framework. Defaults to -1
     --mapred-cluster-max-map-memory-mb=memory                       Maximum virtual memory of a single map task. Defaults to -1
                                                                     This value should be set to (mapred.tasktracker.map.tasks.maximum * mapred.cluster.map.memory.mb)
     --mapred-cluster-max-reduce-memory-mb=memory                    maximum virtual memory of a single reduce task. Defaults to -1
                                                                     This value should be set to (mapred.tasktracker.reduce.tasks.maximum * mapred.cluster.reduce.memory.mb)
     --mapred-job-map-memory-mb=memory                               Virtual memory of a single map slot for a job. Defaults to -1
                                                                     This value should be <= mapred.cluster.max.map.memory.mb
     --mapred-job-reduce-memory-mb=memory                            Virtual memory, of a single reduce slot for a job. Defaults to -1
                                                                     This value should be <= mapred.cluster.max.reduce.memory.mb
     --dfs-datanode-dir-perm=700                                     Set the permission for the datanode data directories. Defaults to 700
     --dfs-block-local-path-access-user=user                         User for which you want to enable shortcircuit read.
     --dfs-client-read-shortcircuit=true/false                       Enable shortcircuit read for the client. Will default to true if the shortcircuit user is set.
     --dfs-client-read-shortcircuit-skip-checksum=false/true         Disable checking of checksum when shortcircuit read is taking place. Defaults to false.
  "
  exit 1
}

check_permission() {
  TARGET=$1
  OWNER="0"
  RESULT=0
  while [ "$TARGET" != "/" ]; do
    if [ "`uname`" = "Darwin" ]; then
      OWNER=`stat -f %u $TARGET`
    else
      OWNER=`stat -c %u $TARGET`
    fi
    if [ "$OWNER" != "0" ]; then
      RESULT=1
      break
    fi
    TARGET=`dirname $TARGET`
  done
  return $RESULT
}

template_generator() {
  REGEX='(\$\{[a-zA-Z_][a-zA-Z_0-9]*\})'
  if [ -e $2 ]; then
    mv -f $2 "$2.bak"
  fi
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

#########################################
# Function to modify a value of a field in an xml file
# Params: $1 is the file with full path; $2 is the property, $3 is the new value
#########################################
function addPropertyToXMLConf
{
  #read the file name with full path
  local file=$1
  #get the property name
  local property=$2
  #get what value should be set for that
  local propValue=$3
  #get the description
  local desc=$4
  #get the value for the final tag
  local finalVal=$5

  #create the property text, make sure the / are escaped
  propText="<property>\n<name>$property<\/name>\n<value>$propValue<\/value>\n"
  #if description is not empty add it
  if [ ! -z $desc ]
  then
    propText="${propText}<description>$desc<\/description>\n"
  fi
  
  #if final is not empty add it
  if [ ! -z $finalVal ]
  then
    propText="${propText}final>$finalVal<\/final>\n"
  fi

  #add the ending tag
  propText="${propText}<\/property>\n"

  #add the property to the file
  endText="<\/configuration>"
  #add the text using sed at the end of the file
  sed -i "s|$endText|$propText$endText|" $file
}

##########################################
# Function to setup up the short circuit read settings
#########################################
function setupShortCircuitRead
{
  local conf_file="${HADOOP_CONF_DIR}/hdfs-site.xml"
  #if the shortcircuit user is not set then return
  if [ -z $DFS_BLOCK_LOCAL_PATH_ACCESS_USER ]
  then
    return
  fi
  
  #set the defaults if values not present
  DFS_CLIENT_READ_SHORTCIRCUIT=${DFS_CLIENT_READ_SHORTCIRCUIT:-false}
  DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM=${DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM:-false}

  #add the user to the conf file
  addPropertyToXMLConf "$conf_file" "dfs.block.local-path-access.user" "$DFS_BLOCK_LOCAL_PATH_ACCESS_USER"
  addPropertyToXMLConf "$conf_file" "dfs.client.read.shortcircuit" "$DFS_CLIENT_READ_SHORTCIRCUIT"
  addPropertyToXMLConf "$conf_file" "dfs.client.read.shortcircuit.skip.checksum" "$DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM"
}

##########################################
# Function to setup up the proxy user settings
#########################################
function setupProxyUsers
{
  local conf_file="${HADOOP_CONF_DIR}/core-site.xml"
  #if hadoop proxy users are sent, setup hadoop proxy
  if [ ! -z $HADOOP_PROXY_USERS ]
  then
    oldIFS=$IFS
    IFS=';'
    #process each proxy config
    for proxy in $HADOOP_PROXY_USERS
    do
      #get the user, group and hosts information for each proxy
      IFS=':'
      arr=($proxy)
      user="${arr[0]}"
      groups="${arr[1]}"
      hosts="${arr[2]}"
      #determine the property names and values
      proxy_groups_property="hadoop.proxyuser.${user}.groups"
      proxy_groups_val="$groups"
      addPropertyToXMLConf "$conf_file" "$proxy_groups_property" "$proxy_groups_val"
      proxy_hosts_property="hadoop.proxyuser.${user}.hosts"
      proxy_hosts_val="$hosts"
      addPropertyToXMLConf "$conf_file" "$proxy_hosts_property" "$proxy_hosts_val"
      IFS=';'
    done
    IFS=$oldIFS
  fi
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'auto' \
  -l 'java-home:' \
  -l 'conf-dir:' \
  -l 'default' \
  -l 'group:' \
  -l 'hdfs-dir:' \
  -l 'namenode-dir:' \
  -l 'datanode-dir:' \
  -l 'mapred-dir:' \
  -l 'namenode-host:' \
  -l 'secondarynamenode-host:' \
  -l 'jobtracker-host:' \
  -l 'log-dir:' \
  -l 'pid-dir:' \
  -l 'replication:' \
  -l 'taskscheduler:' \
  -l 'hdfs-user:' \
  -l 'hdfs-user-keytab:' \
  -l 'mapreduce-user:' \
  -l 'mapreduce-user-keytab:' \
  -l 'keytab-dir:' \
  -l 'kerberos-realm:' \
  -l 'kinit-location:' \
  -l 'datanodes:' \
  -l 'tasktrackers:' \
  -l 'dfs-webhdfs-enabled:' \
  -l 'hadoop-proxy-users:' \
  -l 'dfs-support-append:' \
  -l 'hbase-user:' \
  -l 'mapred-cluster-map-memory-mb:' \
  -l 'mapred-cluster-reduce-memory-mb:' \
  -l 'mapred-cluster-max-map-memory-mb:' \
  -l 'mapred-cluster-max-reduce-memory-mb:' \
  -l 'mapred-job-map-memory-mb:' \
  -l 'mapred-job-reduce-memory-mb:' \
  -l 'dfs-datanode-dir-perm:' \
  -l 'dfs-block-local-path-access-user:' \
  -l 'dfs-client-read-shortcircuit:' \
  -l 'dfs-client-read-shortcircuit-skip-checksum:' \
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
    --java-home)
      JAVA_HOME=$2; shift 2
      AUTOMATED=1
      ;; 
    --conf-dir)
      HADOOP_CONF_DIR=$2; shift 2
      AUTOMATED=1
      ;; 
    --default)
      AUTOMATED=1; shift
      ;;
    --group)
      HADOOP_GROUP=$2; shift 2
      AUTOMATED=1
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
    --namenode-host)
      HADOOP_NN_HOST=$2; shift 2
      AUTOMATED=1
      ;; 
    --secondarynamenode-host)
      HADOOP_SNN_HOST=$2; shift 2
      AUTOMATED=1
      ;; 
    --jobtracker-host)
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
    --hdfs-user)
      HADOOP_HDFS_USER=$2; shift 2
      AUTOMATED=1
      ;;
    --mapreduce-user)
      HADOOP_MR_USER=$2; shift 2
      AUTOMATED=1
      ;;
    --keytab-dir)
      KEYTAB_DIR=$2; shift 2
      AUTOMATED=1
      ;;
    --hdfs-user-keytab)
      HDFS_KEYTAB=$2; shift 2
      AUTOMATED=1
      ;;
    --mapreduce-user-keytab)
      MR_KEYTAB=$2; shift 2
      AUTOMATED=1
      ;;
    --kerberos-realm)
      KERBEROS_REALM=$2; shift 2
      SECURITY_TYPE="kerberos"
      AUTOMATED=1
      ;;
    --kinit-location)
      KINIT=$2; shift 2
      AUTOMATED=1
      ;;
    --datanodes)
      DATANODES=$2; shift 2
      AUTOMATED=1
      DATANODES=$(echo $DATANODES | tr ',' ' ')
      ;;
    --tasktrackers)
      TASKTRACKERS=$2; shift 2
      AUTOMATED=1
      TASKTRACKERS=$(echo $TASKTRACKERS | tr ',' ' ')
      ;;
    --dfs-webhdfs-enabled)
      DFS_WEBHDFS_ENABLED=$2; shift 2
      AUTOMATED=1
      ;;
    --hadoop-proxy-users)
      HADOOP_PROXY_USERS=$2; shift 2
      AUTOMATED=1
      ;;
    --dfs-support-append)
      DFS_SUPPORT_APPEND=$2; shift 2
      AUTOMATED=1
      ;;
    --hbase-user)
      HBASE_USER=$2; shift 2
      AUTOMATED=1
      ;;
    --mapred-cluster-map-memory-mb)
      MAPRED_CLUSTER_MAP_MEMORY_MB=$2; shift 2
      AUTOMATED=1
      ;;
    --mapred-cluster-reduce-memory-mb)
      MAPRED_CLUSTER_REDUCE_MEMORY_MB=$2; shift 2
      AUTOMATED=1
      ;;
    --mapred-cluster-max-map-memory-mb)
      MAPRED_CLUSTER_MAX_MAP_MEMORY_MB=$2; shift 2
      AUTOMATED=1
      ;;
    --mapred-cluster-max-reduce-memory-mb)
      MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB=$2; shift 2
      AUTOMATED=1
      ;;
    --mapred-job-map-memory-mb)
      MAPRED_JOB_MAP_MEMORY_MB=$2; shift 2
      AUTOMATED=1
      ;;
    --mapred-job-reduce-memory-mb)
      MAPRED_JOB_REDUCE_MEMORY_MB=$2; shift 2
      AUTOMATED=1
      ;;
    --dfs-datanode-dir-perm)
      DFS_DATANODE_DIR_PERM=$2; shift 2
      AUTOMATED=1
      ;;
    --dfs-block-local-path-access-user)
      DFS_BLOCK_LOCAL_PATH_ACCESS_USER=$2; shift 2
      AUTOMATED=1
      ;;
    --dfs-client-read-shortcircuit)
      DFS_CLIENT_READ_SHORTCIRCUIT=$2; shift 2
      AUTOMATED=1
      ;;
    --dfs-client-read-shortcircuit-skip-checksum)
      DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM=$2; shift 2
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
HADOOP_GROUP=${HADOOP_GROUP:-hadoop}
HADOOP_NN_HOST=${HADOOP_NN_HOST:-`hostname`}
HADOOP_SNN_HOST=${HADOOP_SNN_HOST:-`hostname`}
HADOOP_NN_DIR=${HADOOP_NN_DIR:-/var/lib/hadoop/hdfs/namenode}
HADOOP_DN_DIR=${HADOOP_DN_DIR:-/var/lib/hadoop/hdfs/datanode}
HADOOP_JT_HOST=${HADOOP_JT_HOST:-`hostname`}
HADOOP_HDFS_DIR=${HADOOP_HDFS_DIR:-/var/lib/hadoop/hdfs}
HADOOP_MAPRED_DIR=${HADOOP_MAPRED_DIR:-/var/lib/hadoop/mapred}
HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/var/log/hadoop}
HADOOP_PID_DIR=${HADOOP_PID_DIR:-/var/log/hadoop}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop}
HADOOP_REPLICATION=${HADOOP_RELICATION:-3}
HADOOP_TASK_SCHEDULER=${HADOOP_TASK_SCHEDULER:-org.apache.hadoop.mapred.JobQueueTaskScheduler}
HADOOP_HDFS_USER=${HADOOP_HDFS_USER:-hdfs}
HADOOP_MR_USER=${HADOOP_MR_USER:-mr}
DFS_WEBHDFS_ENABLED=${DFS_WEBHDFS_ENABLED:-false}
DFS_SUPPORT_APPEND=${DFS_SUPPORT_APPEND:-false}
HBASE_USER=${HBASE_USER:-hbase}
MAPRED_CLUSTER_MAP_MEMORY_MB=${MAPRED_CLUSTER_MAP_MEMORY_MB:--1}
MAPRED_CLUSTER_REDUCE_MEMORY_MB=${MAPRED_CLUSTER_REDUCE_MEMORY_MB:--1} 
MAPRED_CLUSTER_MAX_MAP_MEMORY_MB=${MAPRED_CLUSTER_MAX_MAP_MEMORY_MB:--1} 
MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB=${MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB:--1} 
MAPRED_JOB_MAP_MEMORY_MB=${MAPRED_JOB_MAP_MEMORY_MB:--1} 
MAPRED_JOB_REDUCE_MEMORY_MB=${MAPRED_JOB_REDUCE_MEMORY_MB:--1} 
KEYTAB_DIR=${KEYTAB_DIR:-/etc/security/keytabs}
HDFS_KEYTAB=${HDFS_KEYTAB:-/home/hdfs/hdfs.keytab}
MR_KEYTAB=${MR_KEYTAB:-/home/mr/mr.keytab}
KERBEROS_REALM=${KERBEROS_REALM:-KERBEROS.EXAMPLE.COM}
SECURITY_TYPE=${SECURITY_TYPE:-simple}
#deault the data dir perm to 700
DFS_DATANODE_DIR_PERM=${DFS_DATANODE_DIR_PERM:-700}
KINIT=${KINIT:-/usr/kerberos/bin/kinit}
if [ "${SECURITY_TYPE}" = "kerberos" ]; then
  TASK_CONTROLLER="org.apache.hadoop.mapred.LinuxTaskController"
  HADOOP_DN_ADDR="0.0.0.0:1019"
  HADOOP_DN_HTTP_ADDR="0.0.0.0:1022"
  SECURITY="true"
  HADOOP_SECURE_DN_USER=${HADOOP_HDFS_USER}
else
  TASK_CONTROLLER="org.apache.hadoop.mapred.DefaultTaskController"
  HADOOP_DN_ADDR="0.0.0.0:50010"
  HADOOP_DN_HTTP_ADDR="0.0.0.0:50075"
  SECURITY="false"
  HADOOP_SECURE_DN_USER=""
fi

#unset env vars
unset HADOOP_CLIENT_OPTS HADOOP_NAMENODE_OPTS HADOOP_JOBTRACKER_OPTS HADOOP_TASKTRACKER_OPTS HADOOP_DATANODE_OPTS HADOOP_SECONDARYNAMENODE_OPTS HADOOP_JAVA_PLATFORM_OPTS

if [ "${AUTOMATED}" != "1" ]; then
  echo "Setup Hadoop Configuration"
  echo
  echo -n "Where would you like to put config directory? (${HADOOP_CONF_DIR}) "
  read USER_HADOOP_CONF_DIR
  echo -n "Where would you like to put log directory? (${HADOOP_LOG_DIR}) "
  read USER_HADOOP_LOG_DIR
  echo -n "Where would you like to put pid directory? (${HADOOP_PID_DIR}) "
  read USER_HADOOP_PID_DIR
  echo -n "What is the host of the namenode? (${HADOOP_NN_HOST}) "
  read USER_HADOOP_NN_HOST
  echo -n "Where would you like to put namenode data directory? (${HADOOP_NN_DIR}) "
  read USER_HADOOP_NN_DIR
  echo -n "Where would you like to put datanode data directory? (${HADOOP_DN_DIR}) "
  read USER_HADOOP_DN_DIR
  echo -n "What is the host of the jobtracker? (${HADOOP_JT_HOST}) "
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
  echo "Namenode host               : ${HADOOP_NN_HOST}"
  echo "Namenode directory          : ${HADOOP_NN_DIR}"
  echo "Datanode directory          : ${HADOOP_DN_DIR}"
  echo "Jobtracker host             : ${HADOOP_JT_HOST}"
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

if [ "${AUTOSETUP}" == "1" -o "${AUTOSETUP}" == "y" ]; then
  if [ -d ${KEYTAB_DIR} ]; then
    chmod 700 ${KEYTAB_DIR}/*
    chown ${HADOOP_MR_USER}:${HADOOP_GROUP} ${KEYTAB_DIR}/[jt]t.service.keytab
    chown ${HADOOP_HDFS_USER}:${HADOOP_GROUP} ${KEYTAB_DIR}/[dns]n.service.keytab
  fi
  chmod 755 -R ${HADOOP_PREFIX}/sbin/*hadoop*
  chmod 755 -R ${HADOOP_PREFIX}/bin/hadoop
  chmod 755 -R ${HADOOP_PREFIX}/libexec/hadoop-config.sh
  mkdir -p /home/${HADOOP_MR_USER}
  chown ${HADOOP_MR_USER}:${HADOOP_GROUP} /home/${HADOOP_MR_USER}
  HDFS_DIR=`echo ${HADOOP_HDFS_DIR} | sed -e 's/,/ /g'`
  mkdir -p ${HDFS_DIR}
  if [ -e ${HADOOP_NN_DIR} ]; then
    rm -rf ${HADOOP_NN_DIR}
  fi
  DATANODE_DIR=`echo ${HADOOP_DN_DIR} | sed -e 's/,/ /g'`
  MAPRED_DIR=`echo ${HADOOP_MAPRED_DIR} | sed -e 's/,/ /g'`
  mkdir -p ${MAPRED_DIR}
  mkdir -p ${HADOOP_CONF_DIR}
  check_permission ${HADOOP_CONF_DIR}
  if [ $? == 1 ]; then
    echo "Full path to ${HADOOP_CONF_DIR} should be owned by root."
    exit 1
  fi

  mkdir -p ${HADOOP_LOG_DIR}
  #create the log sub dir for diff users
  mkdir -p ${HADOOP_LOG_DIR}/${HADOOP_HDFS_USER}
  mkdir -p ${HADOOP_LOG_DIR}/${HADOOP_MR_USER}

  mkdir -p ${HADOOP_PID_DIR}
  chown ${HADOOP_HDFS_USER}:${HADOOP_GROUP} ${HDFS_DIR}
  chown ${HADOOP_MR_USER}:${HADOOP_GROUP} ${MAPRED_DIR}
  chown ${HADOOP_HDFS_USER}:${HADOOP_GROUP} ${HADOOP_LOG_DIR}
  chmod 775 ${HADOOP_LOG_DIR}
  chmod 775 ${HADOOP_PID_DIR}
  chown root:${HADOOP_GROUP} ${HADOOP_PID_DIR}

  #change the permission and the owner
  chmod 755 ${HADOOP_LOG_DIR}/${HADOOP_HDFS_USER}
  chown ${HADOOP_HDFS_USER}:${HADOOP_GROUP} ${HADOOP_LOG_DIR}/${HADOOP_HDFS_USER}
  chmod 755 ${HADOOP_LOG_DIR}/${HADOOP_MR_USER}
  chown ${HADOOP_MR_USER}:${HADOOP_GROUP} ${HADOOP_LOG_DIR}/${HADOOP_MR_USER}

  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-env.sh ${HADOOP_CONF_DIR}/hadoop-env.sh
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-policy.xml ${HADOOP_CONF_DIR}/hadoop-policy.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/commons-logging.properties ${HADOOP_CONF_DIR}/commons-logging.properties
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/mapred-queue-acls.xml ${HADOOP_CONF_DIR}/mapred-queue-acls.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/taskcontroller.cfg ${HADOOP_CONF_DIR}/taskcontroller.cfg
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/capacity-scheduler.xml ${HADOOP_CONF_DIR}/capacity-scheduler.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/log4j.properties ${HADOOP_CONF_DIR}/log4j.properties
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-metrics2.properties ${HADOOP_CONF_DIR}/hadoop-metrics2.properties

  #setup up the proxy users
  setupProxyUsers

  #setup short circuit read
  setupShortCircuitRead

  #set the owner of the hadoop dir to root
  chown root ${HADOOP_PREFIX}
  chown root:${HADOOP_GROUP} ${HADOOP_CONF_DIR}/hadoop-env.sh
  chmod 755 ${HADOOP_CONF_DIR}/hadoop-env.sh
  
  #set taskcontroller
  chown root:${HADOOP_GROUP} ${HADOOP_CONF_DIR}/taskcontroller.cfg
  chmod 400 ${HADOOP_CONF_DIR}/taskcontroller.cfg
  chown root:${HADOOP_GROUP} ${HADOOP_PREFIX}/bin/task-controller
  chmod 6050 ${HADOOP_PREFIX}/bin/task-controller

  #generate the slaves file and include and exclude files for hdfs and mapred
  echo '' > ${HADOOP_CONF_DIR}/slaves
  echo '' > ${HADOOP_CONF_DIR}/dfs.include
  echo '' > ${HADOOP_CONF_DIR}/dfs.exclude
  echo '' > ${HADOOP_CONF_DIR}/mapred.include
  echo '' > ${HADOOP_CONF_DIR}/mapred.exclude
  for dn in $DATANODES
  do
    echo $dn >> ${HADOOP_CONF_DIR}/slaves
    echo $dn >> ${HADOOP_CONF_DIR}/dfs.include
  done
  for tt in $TASKTRACKERS
  do
    echo $tt >> ${HADOOP_CONF_DIR}/mapred.include
  done

  echo "Configuration setup is completed."
  if [[ "$HADOOP_NN_HOST" =~ "`hostname`" ]]; then
    echo "Proceed to run hadoop-setup-hdfs.sh on namenode."
  fi
else
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-env.sh ${HADOOP_CONF_DIR}/hadoop-env.sh
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-policy.xml ${HADOOP_CONF_DIR}/hadoop-policy.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/commons-logging.properties ${HADOOP_CONF_DIR}/commons-logging.properties
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/mapred-queue-acls.xml ${HADOOP_CONF_DIR}/mapred-queue-acls.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/taskcontroller.cfg ${HADOOP_CONF_DIR}/taskcontroller.cfg
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/capacity-scheduler.xml ${HADOOP_CONF_DIR}/capacity-scheduler.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/log4j.properties ${HADOOP_CONF_DIR}/log4j.properties
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-metrics2.properties ${HADOOP_CONF_DIR}/hadoop-metrics2.properties

  #setup up the proxy users
  setupProxyUsers

  #setup shortcircuit read
  setupShortCircuitRead

  chown root:${HADOOP_GROUP} ${HADOOP_CONF_DIR}/hadoop-env.sh
  chmod 755 ${HADOOP_CONF_DIR}/hadoop-env.sh
  #set taskcontroller
  chown root:${HADOOP_GROUP} ${HADOOP_CONF_DIR}/taskcontroller.cfg
  chmod 400 ${HADOOP_CONF_DIR}/taskcontroller.cfg
  chown root:${HADOOP_GROUP} ${HADOOP_PREFIX}/bin/task-controller
  chmod 6050 ${HADOOP_PREFIX}/bin/task-controller
  
  #generate the slaves file and include and exclude files for hdfs and mapred
  echo '' > ${HADOOP_CONF_DIR}/slaves
  echo '' > ${HADOOP_CONF_DIR}/dfs.include
  echo '' > ${HADOOP_CONF_DIR}/dfs.exclude
  echo '' > ${HADOOP_CONF_DIR}/mapred.include
  echo '' > ${HADOOP_CONF_DIR}/mapred.exclude
  for dn in $DATANODES
  do
    echo $dn >> ${HADOOP_CONF_DIR}/slaves
    echo $dn >> ${HADOOP_CONF_DIR}/dfs.include
  done
  for tt in $TASKTRACKERS
  do
    echo $tt >> ${HADOOP_CONF_DIR}/mapred.include
  done
  
  echo
  echo "Configuration file has been generated in:"
  echo
  echo "${HADOOP_CONF_DIR}/core-site.xml"
  echo "${HADOOP_CONF_DIR}/hdfs-site.xml"
  echo "${HADOOP_CONF_DIR}/mapred-site.xml"
  echo "${HADOOP_CONF_DIR}/hadoop-env.sh"
  echo "${HADOOP_CONF_DIR}/hadoop-policy.xml"
  echo "${HADOOP_CONF_DIR}/commons-logging.properties"
  echo "${HADOOP_CONF_DIR}/taskcontroller.cfg"
  echo "${HADOOP_CONF_DIR}/capacity-scheduler.xml"
  echo "${HADOOP_CONF_DIR}/log4j.properties"
  echo "${HADOOP_CONF_DIR}/hadoop-metrics2.properties"
  echo
  echo " to ${HADOOP_CONF_DIR} on all nodes, and proceed to run hadoop-setup-hdfs.sh on namenode."
fi
