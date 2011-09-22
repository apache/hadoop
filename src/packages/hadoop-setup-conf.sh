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
  "
  exit 1
}

check_permission() {
  TARGET=$1
  OWNER="0"
  RESULT=0
  while [ "$TARGET" != "/" ]; do
    PARENT=`dirname $TARGET`
    NAME=`basename $TARGET`
    OWNER=`ls -ln $PARENT | grep $NAME| awk '{print $3}'`
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
KEYTAB_DIR=${KEYTAB_DIR:-/etc/security/keytabs}
HDFS_KEYTAB=${HDFS_KEYTAB:-/home/hdfs/hdfs.keytab}
MR_KEYTAB=${MR_KEYTAB:-/home/mr/mr.keytab}
KERBEROS_REALM=${KERBEROS_REALM:-KERBEROS.EXAMPLE.COM}
SECURITY_TYPE=${SECURITY_TYPE:-simple}
KINIT=${KINIT:-/usr/kerberos/bin/kinit}
if [ "${SECURITY_TYPE}" = "kerberos" ]; then
  TASK_CONTROLLER="org.apache.hadoop.mapred.LinuxTaskController"
  HADOOP_DN_ADDR="0.0.0.0:1019"
  HADOOP_DN_HTTP_ADDR="0.0.0.0:1022"
  SECURITY="true"
  HADOOP_SECURE_DN_USER=${HADOOP_HDFS_USER}
else
  TASK_CONTROLLER="org.apache.hadoop.mapred.DefaultTaskController"
  HADDOP_DN_ADDR="0.0.0.0:50010"
  HADOOP_DN_HTTP_ADDR="0.0.0.0:50075"
  SECURITY="false"
  HADOOP_SECURE_DN_USER=""
fi

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
  mkdir -p ${DATANODE_DIR}
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
  chown ${HADOOP_HDFS_USER}:${HADOOP_GROUP} ${DATANODE_DIR}
  chmod 700 -R ${DATANODE_DIR}
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

  if [ -e ${HADOOP_CONF_DIR}/core-site.xml ]; then
    mv -f ${HADOOP_CONF_DIR}/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/hdfs-site.xml ]; then
    mv -f ${HADOOP_CONF_DIR}/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/mapred-site.xml ]; then
    mv -f ${HADOOP_CONF_DIR}/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/hadoop-env.sh ]; then
    mv -f ${HADOOP_CONF_DIR}/hadoop-env.sh ${HADOOP_CONF_DIR}/hadoop-env.sh.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/hadoop-policy.xml ]; then
    mv -f ${HADOOP_CONF_DIR}/hadoop-policy.xml ${HADOOP_CONF_DIR}/hadoop-policy.xml.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/mapred-queue-acls.xml ]; then
    mv -f ${HADOOP_CONF_DIR}/mapred-queue-acls.xml ${HADOOP_CONF_DIR}/mapred-queue-acls.xml.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/commons-logging.properties ]; then
    mv -f ${HADOOP_CONF_DIR}/commons-logging.properties ${HADOOP_CONF_DIR}/commons-logging.properties.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/taskcontroller.cfg  ]; then
    mv -f ${HADOOP_CONF_DIR}/taskcontroller.cfg  ${HADOOP_CONF_DIR}/taskcontroller.cfg.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/slaves  ]; then
    mv -f ${HADOOP_CONF_DIR}/slaves  ${HADOOP_CONF_DIR}/slaves.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/dfs.include  ]; then
    mv -f ${HADOOP_CONF_DIR}/dfs.include  ${HADOOP_CONF_DIR}/dfs.include.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/dfs.exclude  ]; then
    mv -f ${HADOOP_CONF_DIR}/dfs.exclude  ${HADOOP_CONF_DIR}/dfs.exclude.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/mapred.include  ]; then
    mv -f ${HADOOP_CONF_DIR}/mapred.include  ${HADOOP_CONF_DIR}/mapred.include.bak
  fi
  if [ -e ${HADOOP_CONF_DIR}/mapred.exclude  ]; then
    mv -f ${HADOOP_CONF_DIR}/mapred.exclude  ${HADOOP_CONF_DIR}/mapred.exclude.bak
  fi

  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-env.sh ${HADOOP_CONF_DIR}/hadoop-env.sh
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-policy.xml ${HADOOP_CONF_DIR}/hadoop-policy.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/commons-logging.properties ${HADOOP_CONF_DIR}/commons-logging.properties
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/mapred-queue-acls.xml ${HADOOP_CONF_DIR}/mapred-queue-acls.xml
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/taskcontroller.cfg ${HADOOP_CONF_DIR}/taskcontroller.cfg
  if [ ! -e ${HADOOP_CONF_DIR}/capacity-scheduler.xml ]; then
    template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/capacity-scheduler.xml ${HADOOP_CONF_DIR}/capacity-scheduler.xml
  fi
  if [ ! -e ${HADOOP_CONF_DIR}/log4j.properties ]; then
    cp ${HADOOP_PREFIX}/share/hadoop/templates/conf/log4j.properties ${HADOOP_CONF_DIR}/log4j.properties
  fi
  if [ ! -e ${HADOOP_CONF_DIR}/hadoop-metrics2.properties ]; then
    cp ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-metrics2.properties ${HADOOP_CONF_DIR}/hadoop-metrics2.properties
  fi

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
  template_generator ${HADOOP_PREFIX}/share/hadoop/templates/conf/hadoop-metrics2.properties ${HADOOP_CONF_DIR}/hadoop-metrics2.properties
  
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
  echo
  echo " to ${HADOOP_CONF_DIR} on all nodes, and proceed to run hadoop-setup-hdfs.sh on namenode."
fi
