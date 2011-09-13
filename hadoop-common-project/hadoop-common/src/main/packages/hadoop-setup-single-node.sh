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

# Script for setup HDFS file system for single node deployment

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
     --default                   Setup system as default
     -h                          Display this message
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
  -l 'default' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

if [ -e /etc/hadoop/hadoop-env.sh ]; then
  . /etc/hadoop/hadoop-env.sh
fi

eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --default)
      AUTOMATED=1; shift
      ;;
    -h)
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

if [ "${AUTOMATED}" != "1" ]; then
  echo "Welcome to Hadoop single node setup wizard"
  echo
  echo -n "Would you like to use default single node configuration? (y/n) "
  read SET_CONFIG
  echo -n "Would you like to format name node? (y/n) "
  read SET_FORMAT
  echo -n "Would you like to setup default directory structure? (y/n) "
  read SET_MKDIR
  echo -n "Would you like to start up Hadoop? (y/n) "
  read STARTUP
  echo -n "Would you like to start up Hadoop on reboot? (y/n) "
  read SET_REBOOT
  echo
  echo "Review your choices:"
  echo
  echo "Setup single node configuration    : ${SET_CONFIG}"
  echo "Format namenode                    : ${SET_FORMAT}"
  echo "Setup default file system structure: ${SET_MKDIR}"
  echo "Start up Hadoop                    : ${STARTUP}"
  echo "Start up Hadoop on reboot          : ${SET_REBOOT}"
  echo
  echo -n "Proceed with setup? (y/n) "
  read CONFIRM
  if [ "${CONFIRM}" != "y" ]; then
    echo "User aborted setup, exiting..."
    exit 1
  fi
else
  SET_CONFIG="y"
  SET_FORMAT="y"
  SET_MKDIR="y"
  STARTUP="y"
  SET_REBOOT="y"
fi

AUTOMATED=${AUTOMATED:-0}
SET_CONFIG=${SET_CONFIG:-y}
SET_FORMAT=${SET_FORMAT:-n}
SET_MKDIR=${SET_MKDIR:-y}
STARTUP=${STARTUP:-y}
SET_REBOOT=${SET_REBOOT:-y}

# Make sure system is not already started
/etc/init.d/hadoop-namenode stop 2>/dev/null >/dev/null
/etc/init.d/hadoop-datanode stop 2>/dev/null >/dev/null
/etc/init.d/hadoop-jobtracker stop 2>/dev/null >/dev/null
/etc/init.d/hadoop-tasktracker stop 2>/dev/null >/dev/null

if [ "${SET_CONFIG}" == "y" ]; then
  JAVA_HOME=${JAVA_HOME:-/usr/java/default}
  HADOOP_NN_HOST=${HADOOP_NN_HOST:-localhost}
  HADOOP_NN_DIR=${HADOOP_NN_DIR:-/var/lib/hadoop/hdfs/namenode}
  HADOOP_DN_DIR=${HADOOP_DN_DIR:-/var/lib/hadoop/hdfs/datanode}
  HADOOP_JT_HOST=${HADOOP_JT_HOST:-localhost}
  HADOOP_HDFS_DIR=${HADOOP_MAPRED_DIR:-/var/lib/hadoop/hdfs}
  HADOOP_MAPRED_DIR=${HADOOP_MAPRED_DIR:-/var/lib/hadoop/mapred}
  HADOOP_PID_DIR=${HADOOP_PID_DIR:-/var/run/hadoop}
  HADOOP_LOG_DIR="/var/log/hadoop"
  HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop}
  HADOOP_REPLICATION=${HADOOP_RELICATION:-1}
  ${HADOOP_PREFIX}/sbin/hadoop-setup-conf.sh --auto \
    --hdfs-user=hdfs \
    --mapreduce-user=mapred \
    --conf-dir=${HADOOP_CONF_DIR} \
    --datanode-dir=${HADOOP_DN_DIR} \
    --hdfs-dir=${HADOOP_HDFS_DIR} \
    --jobtracker-host=${HADOOP_JT_HOST} \
    --log-dir=${HADOOP_LOG_DIR} \
    --pid-dir=${HADOOP_PID_DIR} \
    --mapred-dir=${HADOOP_MAPRED_DIR} \
    --namenode-dir=${HADOOP_NN_DIR} \
    --namenode-host=${HADOOP_NN_HOST} \
    --replication=${HADOOP_REPLICATION}
fi

if [ ! -e ${HADOOP_NN_DIR} ]; then
  rm -rf ${HADOOP_HDFS_DIR} 2>/dev/null >/dev/null
  mkdir -p ${HADOOP_HDFS_DIR}
  chmod 755 ${HADOOP_HDFS_DIR}
  chown hdfs:hadoop ${HADOOP_HDFS_DIR}
  /etc/init.d/hadoop-namenode format
elif [ "${SET_FORMAT}" == "y" ]; then
  rm -rf ${HADOOP_HDFS_DIR} 2>/dev/null >/dev/null
  mkdir -p ${HADOOP_HDFS_DIR}
  chmod 755 ${HADOOP_HDFS_DIR}
  chown hdfs:hadoop ${HADOOP_HDFS_DIR}
  rm -rf ${HADOOP_NN_DIR}
  /etc/init.d/hadoop-namenode format
fi

/etc/init.d/hadoop-namenode start
/etc/init.d/hadoop-datanode start

su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -mkdir /user/mapred' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -chown mapred:mapred /user/mapred' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -mkdir /tmp' hdfs
su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -chmod 777 /tmp' hdfs

/etc/init.d/hadoop-jobtracker start
/etc/init.d/hadoop-tasktracker start

if [ "${SET_REBOOT}" == "y" ]; then
  if [ -e /etc/debian_version ]; then
    ln -sf ../init.d/hadoop-namenode /etc/rc2.d/S90hadoop-namenode
    ln -sf ../init.d/hadoop-datanode /etc/rc2.d/S91hadoop-datanode
    ln -sf ../init.d/hadoop-jobtracker /etc/rc2.d/S92hadoop-jobtracker
    ln -sf ../init.d/hadoop-tasktracker /etc/rc2.d/S93hadoop-tasktracker
    ln -sf ../init.d/hadoop-namenode /etc/rc6.d/S10hadoop-namenode
    ln -sf ../init.d/hadoop-datanode /etc/rc6.d/S11hadoop-datanode
    ln -sf ../init.d/hadoop-jobtracker /etc/rc6.d/S12hadoop-jobtracker
    ln -sf ../init.d/hadoop-tasktracker /etc/rc6.d/S13hadoop-tasktracker
  elif [ -e /etc/redhat-release ]; then
    /sbin/chkconfig hadoop-namenode --add
    /sbin/chkconfig hadoop-datanode --add
    /sbin/chkconfig hadoop-jobtracker --add
    /sbin/chkconfig hadoop-tasktracker --add
    /sbin/chkconfig hadoop-namenode on
    /sbin/chkconfig hadoop-datanode on
    /sbin/chkconfig hadoop-jobtracker on
    /sbin/chkconfig hadoop-tasktracker on
  fi
fi

if [ "${STARTUP}" != "y" ]; then
  /etc/init.d/hadoop-namenode stop
  /etc/init.d/hadoop-datanode stop
  /etc/init.d/hadoop-jobtracker stop
  /etc/init.d/hadoop-tasktracker stop
fi
