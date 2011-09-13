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

# This script configures hadoop-env.sh and symlinkis directories for 
# relocating RPM locations.

usage() {
  echo "
usage: $0 <parameters>
  Required parameters:
     --prefix=PREFIX             path to install into

  Optional parameters:
     --arch=i386                 OS Architecture
     --bin-dir=PREFIX/bin        Executable directory
     --conf-dir=/etc/hadoop      Configuration directory
     --log-dir=/var/log/hadoop   Log directory
     --pid-dir=/var/run          PID file location
     --sbin-dir=PREFIX/sbin      System executable directory
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
  -l 'arch:' \
  -l 'prefix:' \
  -l 'bin-dir:' \
  -l 'conf-dir:' \
  -l 'lib-dir:' \
  -l 'log-dir:' \
  -l 'pid-dir:' \
  -l 'sbin-dir:' \
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
    --sbin-dir)
      SBIN_DIR=$2 ; shift 2
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
HADOOP_PREFIX=$PREFIX
HADOOP_BIN_DIR=${BIN_DIR:-$PREFIX/bin}
HADOOP_CONF_DIR=${CONF_DIR:-$PREFIX/etc/hadoop}
HADOOP_LIB_DIR=${LIB_DIR:-$PREFIX/lib}
HADOOP_LOG_DIR=${LOG_DIR:-$PREFIX/var/log}
HADOOP_PID_DIR=${PID_DIR:-$PREFIX/var/run}
HADOOP_SBIN_DIR=${SBIN_DIR:-$PREFIX/sbin}
UNINSTALL=${UNINSTALL:-0}

if [ "${ARCH}" != "i386" ]; then
  HADOOP_LIB_DIR=${HADOOP_LIB_DIR}64
fi

if [ "${UNINSTALL}" -eq "1" ]; then
  # Remove symlinks
  if [ "${HADOOP_CONF_DIR}" != "${HADOOP_PREFIX}/etc/hadoop" ]; then
    rm -rf ${HADOOP_PREFIX}/etc/hadoop
  fi
  rm -f /etc/default/hadoop-env.sh
  if [ -d /etc/profile.d ]; then
    rm -f /etc/profile.d/hadoop-env.sh
  fi
else
  # Create symlinks
  if [ "${HADOOP_CONF_DIR}" != "${HADOOP_PREFIX}/etc/hadoop" ]; then
    mkdir -p ${HADOOP_PREFIX}/etc
    ln -sf ${HADOOP_CONF_DIR} ${HADOOP_PREFIX}/etc/hadoop
  fi
  ln -sf ${HADOOP_CONF_DIR}/hadoop-env.sh /etc/default/hadoop-env.sh
  if [ -d /etc/profile.d ]; then
    ln -sf ${HADOOP_CONF_DIR}/hadoop-env.sh /etc/profile.d/hadoop-env.sh
  fi

  mkdir -p ${HADOOP_LOG_DIR}
  chown root:hadoop ${HADOOP_LOG_DIR}
  chmod 775 ${HADOOP_LOG_DIR}

  if [ ! -d ${HADOOP_PID_DIR} ]; then
    mkdir -p ${HADOOP_PID_DIR}
    chown root:hadoop ${HADOOP_PID_DIR}
    chmod 775 ${HADOOP_PID_DIR}
  fi

  TFILE="/tmp/$(basename $0).$$.tmp"
  if [ -z "${JAVA_HOME}" ]; then
    if [ -e /etc/debian_version ]; then
      JAVA_HOME=`update-alternatives --config java | grep java | cut -f2 -d':' | cut -f2 -d' ' | sed -e 's/\/bin\/java//'`
    else
      JAVA_HOME=/usr/java/default
    fi
  fi
  template_generator ${HADOOP_CONF_DIR}/hadoop-env.sh.template $TFILE
  cp ${TFILE} ${CONF_DIR}/hadoop-env.sh
  rm -f ${TFILE}
fi
