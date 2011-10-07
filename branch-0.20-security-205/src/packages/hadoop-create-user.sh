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
this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

if [ "$HADOOP_HOME" != "" ]; then
  echo "Warning: \$HADOOP_HOME is deprecated."
  echo
fi

. "$bin"/../libexec/hadoop-config.sh

usage() {
  echo "
usage: $0 <parameters>
  Require parameter:
     --config /etc/hadoop                                  Location of Hadoop configuration file
     -u <username>                                         Create user on HDFS
  Optional parameters:
     -h                                                    Display this message
     --kerberos-realm=KERBEROS.EXAMPLE.COM                 Set Kerberos realm
     --super-user=hdfs                                     Set super user id
     --super-user-keytab=/etc/security/keytabs/hdfs.keytab Set super user keytab location
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'kerberos-realm:' \
  -l 'super-user:' \
  -l 'super-user-keytab:' \
  -o 'h' \
  -o 'u' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
    exit 1
fi

create_user() {
  if [ "${SETUP_USER}" = "" ]; then
    break
  fi
  HADOOP_HDFS_USER=${HADOOP_HDFS_USER:-hdfs}
  export HADOOP_PREFIX
  export HADOOP_CONF_DIR
  export JAVA_HOME
  export SETUP_USER=${SETUP_USER}
  export SETUP_PATH=/user/${SETUP_USER}

  if [ ! "${KERBEROS_REALM}" = "" ]; then
    # locate kinit cmd
    if [ -e /etc/lsb-release ]; then
      KINIT_CMD="/usr/bin/kinit -kt ${HDFS_USER_KEYTAB} ${HADOOP_HDFS_USER}"
    else
      KINIT_CMD="/usr/kerberos/bin/kinit -kt ${HDFS_USER_KEYTAB} ${HADOOP_HDFS_USER}"
    fi
    su -c "${KINIT_CMD}" ${HADOOP_HDFS_USER}
  fi

  su -c "${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir ${SETUP_PATH}" ${HADOOP_HDFS_USER}
  su -c "${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chown ${SETUP_USER}:${SETUP_USER} ${SETUP_PATH}" ${HADOOP_HDFS_USER}
  su -c "${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chmod 711 ${SETUP_PATH}" ${HADOOP_HDFS_USER}

  if [ "$?" == "0" ]; then
    echo "User directory has been setup: ${SETUP_PATH}"
  fi
}

eval set -- "${OPTS}"
while true; do
  case "$1" in
    -u)
      shift
      ;;
    --kerberos-realm)
      KERBEROS_REALM=$2; shift 2
      ;;
    --super-user)
      HADOOP_HDFS_USER=$2; shift 2
      ;;
    --super-user-keytab)
      HDFS_USER_KEYTAB=$2; shift 2
      ;;
    -h)
      usage
      ;; 
    --)
      while shift; do
        SETUP_USER=$1
        create_user
      done
      break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1 
      ;;
  esac
done 

