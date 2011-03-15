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

# This script configures hbase-env.sh and symlinkis directories for 
# relocating RPM locations.

usage() {
  echo "
usage: $0 <parameters>
  Required parameters:
     --prefix=PREFIX             path to install into

  Optional parameters:
     --arch=i386                 OS Architecture
     --bin-dir=PREFIX/bin        Executable directory
     --conf-dir=/etc/hbase       Configuration directory
     --log-dir=/var/log/hbase    Log directory
     --pid-dir=/var/run          PID file location
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'arch:' \
  -l 'prefix:' \
  -l 'bin-dir:' \
  -l 'conf-dir:' \
  -l 'lib-dir:' \
  -l 'log-dir:' \
  -l 'pid-dir:' \
  -l 'uninstall' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --arch)
      ARCH=$2 ; shift 2
      ;;
    --prefix)
      PREFIX=$2 ; shift 2
      ;;
    --bin-dir)
      BIN_DIR=$2 ; shift 2
      ;;
    --log-dir)
      LOG_DIR=$2 ; shift 2
      ;;
    --lib-dir)
      LIB_DIR=$2 ; shift 2
      ;;
    --conf-dir)
      CONF_DIR=$2 ; shift 2
      ;;
    --pid-dir)
      PID_DIR=$2 ; shift 2
      ;;
    --uninstall)
      UNINSTALL=1; shift
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

for var in PREFIX; do
  if [ -z "$(eval "echo \$$var")" ]; then
    echo Missing param: $var
    usage
  fi
done

ARCH=${ARCH:-i386}
BIN_DIR=${BIN_DIR:-$PREFIX/share/hbase/bin}
CONF_DIR=${CONF_DIR:-$PREFIX/conf}
LIB_DIR=${LIB_DIR:-$PREFIX/lib}
LOG_DIR=${LOG_DIR:-$PREFIX/var/log}
PID_DIR=${PID_DIR:-$PREFIX/var/run}
UNINSTALL=${UNINSTALL:-0}

if [ "${ARCH}" != "i386" ]; then
  LIB_DIR=${LIB_DIR}64
fi

. /etc/default/hadoop-env.sh
. /etc/default/zookeeper-env.sh

if [ "${UNINSTALL}" -eq "1" ]; then
  # Remove symlinks
  if [ "${BIN_DIR}" != "${PREFIX}/share/hbase/bin" ]; then
    for var in `ls ${PREFIX}/share/hbase/bin`; do
      rm -f ${BIN_DIR}/${var}
    done
  fi
  if [ -f /etc/default/hbase-env.sh ]; then
    rm -f /etc/default/hbase-env.sh
  fi
  if [ "${CONF_DIR}" != "${PREFIX}/share/hbase/conf" ]; then
    rm -f ${PREFIX}/share/hbase/conf
  fi

  rm -f ${PREFIX}/share/hbase/sbin/hbase-master
  rm -f ${PREFIX}/share/hbase/sbin/hbase-regionserver
  rm -f /etc/init.d/hbase-master
  rm -f /etc/init.d/hbase-regionserver

else
  # Create symlinks
  if [ "${BIN_DIR}" != "${PREFIX}/share/hbase/bin" ]; then
    for var in `ls ${PREFIX}/share/hbase/bin`; do
      ln -sf ${PREFIX}/share/hbase/bin/${var} ${BIN_DIR}/${var}
    done
  fi
  if [ "${CONF_DIR}" != "${PREFIX}/share/hbase/conf" ]; then
    ln -sf ${CONF_DIR} ${PREFIX}/share/hbase/conf
  fi

  chmod 755 ${PREFIX}/share/hbase/sbin/*

  ln -sf ${PREFIX}/share/hbase/sbin/hbase-master /etc/init.d/hbase-master
  ln -sf ${PREFIX}/share/hbase/sbin/hbase-regionserver /etc/init.d/hbase-regionserver

  ln -sf ${CONF_DIR}/hbase-env.sh /etc/default/hbase-env.sh
  ln -sf ${CONF_DIR}/hbase-env.sh /etc/profile.d/hbase-env.sh

  if [ -d ${HADOOP_HOME} ]; then
    HADOOP_JARS=`ls ${HADOOP_HOME}/*.jar | tr '\n' ':'`
  fi

  if [ -d ${ZOOKEEPER_HOME}/share/zookeeper ]; then
    ZOOKEEPER_JARS=`ls ${ZOOKEEPER_HOME}/share/zookeeper/*.jar | tr '\n' ':'`
  fi

  mkdir -p ${PID_DIR}
  mkdir -p ${LOG_DIR}
  chown hbase ${PID_DIR}
  chown hbase ${LOG_DIR}

  TFILE="/tmp/$(basename $0).$$.tmp"
  grep -v "^export HBASE_HOME" ${CONF_DIR}/hbase-env.sh | \
  grep -v "^export HBASE_CONF_DIR" | \
  grep -v "^export HBASE_CLASSPATH" | \
  grep -v "^export HBASE_MANAGES_ZK" | \
  grep -v "^export HBASE_IDENT_STRING" | \
  grep -v "^export HBASE_PID_DIR" | \
  grep -v "^export HBASE_LOG_DIR" | \
  grep -v "^export JAVA_HOME" > ${TFILE}
  if [ -z "${JAVA_HOME}" ]; then
    if [ -e /etc/lsb-release ]; then
      JAVA_HOME=`update-alternatives --config java | grep java | cut -f2 -d':' | cut -f2 -d' ' | sed -e 's/\/bin\/java//'`
    else
      JAVA_HOME=/usr/java/default
    fi
  fi
  if [ "${JAVA_HOME}xxx" != "xxx" ]; then
    echo "export JAVA_HOME=${JAVA_HOME}" >> ${TFILE}
  fi
  echo "export HBASE_IDENT_STRING=\`whoami\`" >> ${TFILE}
  echo "export HBASE_HOME=${PREFIX}/share/hbase" >> ${TFILE}
  echo "export HBASE_CONF_DIR=${CONF_DIR}" >> ${TFILE}
  echo "export HBASE_CLASSPATH=${CONF_DIR}:${HADOOP_CONF_DIR}:${HADOOP_JARS}:${ZOOKEEPER_JARS}" >> ${TFILE}
  echo "export HBASE_MANAGES_ZK=false" >> ${TFILE}
  echo "export HBASE_PID_DIR=${PID_DIR}" >> ${TFILE}
  echo "export HBASE_LOG_DIR=${LOG_DIR}" >> ${TFILE}
  cp ${TFILE} ${CONF_DIR}/hbase-env.sh
  rm -f ${TFILE}
fi
