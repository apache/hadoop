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


#
# Environment Variables
#
#   HADOOP_JHS_LOGGER  Hadoop JobSummary logger.
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_MAPRED_HOME}/conf.
#   HADOOP_MAPRED_PID_DIR   The pid files are stored. /tmp by default.
#   HADOOP_MAPRED_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: mr-jobhistory-daemon.sh [--config <conf-dir>] (start|stop) <mapred-command> "

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
if [ -e ${HADOOP_LIBEXEC_DIR}/mapred-config.sh ]; then
  . $HADOOP_LIBEXEC_DIR/mapred-config.sh
fi

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

if [ "$HADOOP_MAPRED_IDENT_STRING" = "" ]; then
  export HADOOP_MAPRED_IDENT_STRING="$USER"
fi

export HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME:-${HADOOP_PREFIX}}
export HADOOP_MAPRED_LOGFILE=mapred-$HADOOP_MAPRED_IDENT_STRING-$command-$HOSTNAME.log
export HADOOP_MAPRED_ROOT_LOGGER=${HADOOP_MAPRED_ROOT_LOGGER:-INFO,RFA}
export HADOOP_JHS_LOGGER=${HADOOP_JHS_LOGGER:-INFO,JSA}

if [ -f "${HADOOP_CONF_DIR}/mapred-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/mapred-env.sh"
fi

mkdir -p "$HADOOP_MAPRED_LOG_DIR"
chown $HADOOP_MAPRED_IDENT_STRING $HADOOP_MAPRED_LOG_DIR

if [ "$HADOOP_MAPRED_PID_DIR" = "" ]; then
  HADOOP_MAPRED_PID_DIR=/tmp
fi

HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.id.str=$HADOOP_MAPRED_IDENT_STRING"

log=$HADOOP_MAPRED_LOG_DIR/mapred-$HADOOP_MAPRED_IDENT_STRING-$command-$HOSTNAME.out
pid=$HADOOP_MAPRED_PID_DIR/mapred-$HADOOP_MAPRED_IDENT_STRING-$command.pid

HADOOP_MAPRED_STOP_TIMEOUT=${HADOOP_MAPRED_STOP_TIMEOUT:-5}

# Set default scheduling priority
if [ "$HADOOP_MAPRED_NICENESS" = "" ]; then
  export HADOOP_MAPRED_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$HADOOP_MAPRED_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    hadoop_rotate_log $log
    echo starting $command, logging to $log
    cd "$HADOOP_MAPRED_HOME"
    nohup nice -n $HADOOP_MAPRED_NICENESS "$HADOOP_MAPRED_HOME"/bin/mapred --config $HADOOP_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $command
        kill $TARGET_PID
        sleep $HADOOP_MAPRED_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$command did not stop gracefully after $HADOOP_MAPRED_STOP_TIMEOUT seconds: killing with kill -9"
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
