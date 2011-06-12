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


# Runs a HdfsProxy as a daemon.
#
# Environment Variables
#
#   HDFSPROXY_CONF_DIR  Alternate conf dir. Default is ${HDFSPROXY_HOME}/conf.
#   HDFSPROXY_MASTER    host:path where hdfsproxy code should be rsync'd from
#   HDFSPROXY_PID_DIR   The pid files are stored. /tmp by default.
#   HDFSPROXY_IDENT_STRING   A string representing this instance of hdfsproxy. $USER by default
#   HDFSPROXY_NICENESS The scheduling priority for daemons. Defaults to 0.
#		TOMCAT_HOME_DIR tomcat home directory.
##

usage="Usage: hdfsproxy-tomcat-server.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) "

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hdfsproxy-config.sh

# get arguments
startStop=$1
shift


if [ -f "${HDFSPROXY_CONF_DIR}/hdfsproxy-env.sh" ]; then
  . "${HDFSPROXY_CONF_DIR}/hdfsproxy-env.sh"
fi


if [ "$HDFSPROXY_IDENT_STRING" = "" ]; then
  export HDFSPROXY_IDENT_STRING="$USER"
fi


# Set default scheduling priority
if [ "$HDFSPROXY_NICENESS" = "" ]; then
    export HDFSPROXY_NICENESS=0
fi

case $startStop in

  (start)
    if [ "$HDFSPROXY_MASTER" != "" ]; then
      echo rsync from $HDFSPROXY_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $HDFSPROXY_MASTER/ "$HDFSPROXY_HOME"
    fi

    echo starting hdfsproxy tomcat server
    cd "$HDFSPROXY_HOME"
    nohup nice -n $HDFSPROXY_NICENESS "$TOMCAT_HOME_DIR"/bin/startup.sh >& /dev/null &
    sleep 1
    ;;
          
  (stop)

    echo stopping hdfsproxy tomcat server
    cd "$HDFSPROXY_HOME"
    nohup nice -n $HDFSPROXY_NICENESS "$TOMCAT_HOME_DIR"/bin/shutdown.sh >& /dev/null &
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


