#!/usr/bin/env bash
##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
set -e

#To avoid docker volume permission problems
sudo chmod o+rwx /data

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$DIR/envtoconf.py --destination /opt/hadoop/etc/hadoop
echo "Setting up environment!"

if [ -n "$SLEEP_SECONDS" ]; then
   echo "Sleeping for $SLEEP_SECONDS seconds"
   sleep $SLEEP_SECONDS
fi

if [ -n "$KERBEROS_ENABLED" ]; then
	echo "Setting up kerberos!!"
	KERBEROS_SERVER=${KERBEROS_SERVER:-krb5}
	ISSUER_SERVER=${ISSUER_SERVER:-$KERBEROS_SERVER\:8081}
	echo "KDC ISSUER_SERVER => $ISSUER_SERVER"

	while true
	do
	  STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://$ISSUER_SERVER/keytab/test/test)
	  if [ $STATUS -eq 200 ]; then
		echo "Got 200, KDC service ready!!"
		break
	  else
		echo "Got $STATUS :( KDC service not ready yet..."
	  fi
	  sleep 5
	done

	export HOST_NAME=`hostname -f`
	for NAME in ${KERBEROS_KEYTABS}; do
	   echo "Download $NAME/$HOSTNAME@EXAMPLE.COM keytab file to $CONF_DIR/$NAME.keytab"
	   wget http://$ISSUER_SERVER/keytab/$HOST_NAME/$NAME -O $CONF_DIR/$NAME.keytab
	   KERBEROS_ENABLED=true
	done

	cat $DIR/krb5.conf |  sed "s/SERVER/$KERBEROS_SERVER/g" | sudo tee /etc/krb5.conf
fi

#To avoid docker volume permission problems
sudo chmod o+rwx /data

if [ -n "$ENSURE_NAMENODE_DIR" ]; then
   CLUSTERID_OPTS=""
   if [ -n "$ENSURE_NAMENODE_CLUSTERID" ]; then
      CLUSTERID_OPTS="-clusterid $ENSURE_NAMENODE_CLUSTERID"
   fi
   if [ ! -d "$ENSURE_NAMENODE_DIR" ]; then
      /opt/hadoop/bin/hdfs namenode -format -force $CLUSTERID_OPTS
        fi
fi

if [ -n "$ENSURE_STANDBY_NAMENODE_DIR" ]; then
   if [ ! -d "$ENSURE_STANDBY_NAMENODE_DIR" ]; then
      /opt/hadoop/bin/hdfs namenode -bootstrapStandby
    fi
fi

if [ -n "$ENSURE_SCM_INITIALIZED" ]; then
   if [ ! -f "$ENSURE_SCM_INITIALIZED" ]; then
      # Improve om and scm start up options
      /opt/hadoop/bin/ozone scm --init || /opt/hadoop/bin/ozone scm -init
   fi
fi


if [ -n "$ENSURE_OM_INITIALIZED" ]; then
   if [ ! -f "$ENSURE_OM_INITIALIZED" ]; then
      # To make sure SCM is running in dockerized environment we will sleep
      # Could be removed after HDFS-13203
      echo "Waiting 15 seconds for SCM startup"
      sleep 15
      # Improve om and scm start up options
      /opt/hadoop/bin/ozone om --init || /opt/hadoop/bin/ozone om -createObjectStore
   fi
fi


# The KSM initialization block will go away eventually once
# we have completed renaming KSM to OzoneManager (OM).
#
if [ -n "$ENSURE_KSM_INITIALIZED" ]; then
   if [ ! -f "$ENSURE_KSM_INITIALIZED" ]; then
      # To make sure SCM is running in dockerized environment we will sleep
      # Could be removed after HDFS-13203
      echo "Waiting 15 seconds for SCM startup"
      sleep 15
      /opt/hadoop/bin/ozone ksm -createObjectStore
   fi
fi


# Supports byteman script to instrument hadoop process with byteman script
#
#
if [ -n "$BYTEMAN_SCRIPT" ] || [ -n "$BYTEMAN_SCRIPT_URL" ]; then

  export PATH=$PATH:$BYTEMAN_DIR/bin

  if [ ! -z "$BYTEMAN_SCRIPT_URL" ]; then
    sudo wget $BYTEMAN_SCRIPT_URL -O /tmp/byteman.btm
    export BYTEMAN_SCRIPT=/tmp/byteman.btm
  fi

  if [ ! -f "$BYTEMAN_SCRIPT" ]; then
    echo "ERROR: The defined $BYTEMAN_SCRIPT does not exist!!!"
    exit -1
  fi

  AGENT_STRING="-javaagent:/opt/byteman.jar=script:$BYTEMAN_SCRIPT"
  export HADOOP_OPTS="$AGENT_STRING $HADOOP_OPTS"
  echo "Process is instrumented with adding $AGENT_STRING to HADOOP_OPTS"
fi


$@
