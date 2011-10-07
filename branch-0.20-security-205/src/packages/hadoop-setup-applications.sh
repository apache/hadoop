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

. "$bin"/../libexec/hadoop-config.sh

usage() {
  echo "
usage: $0 <parameters>
  Require parameter:
     --config /etc/hadoop                                  Location of Hadoop configuration file
     --apps=<csl of apps:user hcat:hcat,hbase,hive:user>   Apps you want to setup on hdfs
                                                           If user is not specified, app name
                                                           will be used as the user name as well
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
  -l 'apps:' \
  -o 'h' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
    exit 1
fi

function setup_apps
{
  if [ -z $APPS ] 
  then
    usage
    break
  fi

  #if super user is not set default to hdfs
  HADOOP_HDFS_USER=${HADOOP_HDFS_USER:-hdfs}

  if [ ! "${KERBEROS_REALM}" = "" ]; then
    # locate kinit cmd
    if [ -e /etc/lsb-release ]; then
      KINIT_CMD="/usr/bin/kinit -kt ${HDFS_USER_KEYTAB} ${HADOOP_HDFS_USER}"
    else
      KINIT_CMD="/usr/kerberos/bin/kinit -kt ${HDFS_USER_KEYTAB} ${HADOOP_HDFS_USER}"
    fi
    su -c "${KINIT_CMD}" ${HADOOP_HDFS_USER}
  fi
  #process each app
  oldIFS=$IFS 
  IFS=','
  for app in $APPS
  do
    IFS=":"
    arr=($app)
    app=${arr[0]}
    user=${arr[1]}
    IFS=','
    #if user is empty, default it to app
    if [ -z $user ]
    then
      user=$app
    fi

    path="/apps/${app}"

    #create the dir
    cmd="su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -mkdir ${path}' ${HADOOP_HDFS_USER}"
    echo $cmd
    eval $cmd

    #make owner to be the app
    cmd="su -c '${HADOOP_PREFIX}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -chown ${user} ${path}' ${HADOOP_HDFS_USER}"
    echo $cmd
    eval $cmd

    if [ "$?" == "0" ]; then
      echo "App directory has been setup: ${path}"
    fi
  done
  IFS=$oldIFS
}

eval set -- "${OPTS}"
while true; do
  case "$1" in
    --apps)
      APPS=$2; shift 2
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
      shift ; break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1 
      ;;
  esac
done

setup_apps
