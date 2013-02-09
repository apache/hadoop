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


# Runs a yarn command as a daemon.
#
# Environment Variables
#
#   YARN_CONF_DIR  Alternate conf dir. Default is ${HADOOP_YARN_HOME}/conf.
#   YARN_LOG_DIR   Where log files are stored.  PWD by default.
#   YARN_MASTER    host:path where hadoop code should be rsync'd from
#   YARN_PID_DIR   The pid files are stored. /tmp by default.
#   YARN_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   YARN_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: yarn-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <yarn-command> "

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $HADOOP_LIBEXEC_DIR/yarn-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

hadoop_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${YARN_CONF_DIR}/yarn-env.sh" ]; then
  . "${YARN_CONF_DIR}/yarn-env.sh"
fi

if [ "$YARN_IDENT_STRING" = "" ]; then
  export YARN_IDENT_STRING="$USER"
fi

# get log directory
if [ "$YARN_LOG_DIR" = "" ]; then
  export YARN_LOG_DIR="$HADOOP_YARN_HOME/logs"
fi

if [ ! -w "$YARN_LOG_DIR" ] ; then
  mkdir -p "$YARN_LOG_DIR"
  chown $YARN_IDENT_STRING $YARN_LOG_DIR 
fi

if [ "$YARN_PID_DIR" = "" ]; then
  YARN_PID_DIR=/tmp
fi

# some variables
export YARN_LOGFILE=yarn-$YARN_IDENT_STRING-$command-$HOSTNAME.log
export YARN_ROOT_LOGGER=${YARN_ROOT_LOGGER:-INFO,RFA}
log=$YARN_LOG_DIR/yarn-$YARN_IDENT_STRING-$command-$HOSTNAME.out
pid=$YARN_PID_DIR/yarn-$YARN_IDENT_STRING-$command.pid
YARN_STOP_TIMEOUT=${YARN_STOP_TIMEOUT:-5}

# Set default scheduling priority
if [ "$YARN_NICENESS" = "" ]; then
    export YARN_NICENESS=0
fi

case $startStop in

  (start)

    [ -w "$YARN_PID_DIR" ] || mkdir -p "$YARN_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$YARN_MASTER" != "" ]; then
      echo rsync from $YARN_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $YARN_MASTER/ "$HADOOP_YARN_HOME"
    fi

    hadoop_rotate_log $log
    echo starting $command, logging to $log
    cd "$HADOOP_YARN_HOME"
    nohup nice -n $YARN_NICENESS "$HADOOP_YARN_HOME"/bin/yarn --config $YARN_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1
    # capture the ulimit output
    echo "ulimit -a" >> $log
    ulimit -a >> $log 2>&1
    head -30 "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $command
        kill $TARGET_PID
        sleep $YARN_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$command did not stop gracefully after $YARN_STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


