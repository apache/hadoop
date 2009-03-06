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


# Runs a Chukwa command as a daemon.
#
# Environment Variables
#
#   CHUKWA_CONF_DIR  Alternate conf dir. Default is ${CHUKWA_HOME}/conf.
#   CHUKWA_LOG_DIR   Where log files are stored.  PWD by default.
#   CHUKWA_MASTER    host:path where chukwa code should be rsync'd from
#   CHUKWA_PID_DIR   The pid files are stored. ${CHUKWA_HOME}/var/tmp by default.
#   CHUKWA_IDENT_STRING   A string representing this instance of chukwa. $USER by default
#   CHUKWA_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: chukwa-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <chukwa-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/chukwa-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

chukwa_rotate_log ()
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

if [ -f "${CHUKWA_CONF_DIR}/chukwa-env.sh" ]; then
  . "${CHUKWA_CONF_DIR}/chukwa-env.sh"
fi

# get log directory
if [ "$CHUKWA_LOG_DIR" = "" ]; then
  export CHUKWA_LOG_DIR="$CHUKWA_HOME/logs"
fi
mkdir -p "$CHUKWA_LOG_DIR"

if [ "$CHUKWA_PID_DIR" = "" ]; then
  CHUKWA_PID_DIR=$CHUKWA_HOME/var/run
fi

if [ "$CHUKWA_IDENT_STRING" = "" ]; then
  export CHUKWA_IDENT_STRING="$USER"
fi

# some variables
export CHUKWA_LOGFILE=chukwa-$CHUKWA_IDENT_STRING-$command-$HOSTNAME.log
export CHUKWA_ROOT_LOGGER="INFO,DRFA"
log=$CHUKWA_LOG_DIR/chukwa-$CHUKWA_IDENT_STRING-$command-$HOSTNAME.out
pid=$CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-$command.pid

# Set default scheduling priority
if [ "$CHUKWA_NICENESS" = "" ]; then
    export CHUKWA_NICENESS=0
fi

case $startStop in

  (start)
    MAIL=`cat ${CHUKWA_CONF_DIR}/alert.conf`

    RANDOM=`date '+%s'`
    PARTROL_HOUR=$[($RANDOM % 24)]
    if [ ${PARTROL_HOUR} -gt 12 ]; then
        PARTROL_HOUR2=$[${PARTROL_HOUR}-12]
    else 
        PARTROL_HOUR2=$[${PARTROL_HOUR}+12]
    fi
    if [ -n "${WATCHDOG}" ]; then
        mkdir -p ${CHUKWA_HOME}/var/tmp >&/dev/null
        crontab -l > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
        crontest=$?

        if [ "X${crontest}" != "X0" ]; then
          echo "MAILTO=${MAIL}" > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
        else
          grep -v "${CHUKWA_HOME}/bin/watchdog.sh" ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} | grep -v MAILTO | grep -v "cat ${CHUKWA_HOME}/var/run/watchdog.out" | grep -v ${CHUKWA_HOME}/tools/expire.sh > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
          echo "MAILTO=${MAIL}" > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
          cat ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2 >> ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
          rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
        fi
        cat >> ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} << CRON
*/5 * * * * ${CHUKWA_HOME}/bin/watchdog.sh --config ${CHUKWA_CONF_DIR}  > ${CHUKWA_HOME}/var/run/watchdog.out
1 ${PARTROL_HOUR},${PARTROL_HOUR2} * * * /bin/bash -c "cat ${CHUKWA_HOME}/var/run/watchdog.out; cat /dev/null > ${CHUKWA_HOME}/var/run/watchdog.out"
15 3 * * * ${CHUKWA_HOME}/tools/expire.sh 10 ${CHUKWA_LOG_DIR} nowait
CRON

        # save crontab
        echo -n "Registering watchdog.."
        mkdir -p ${CHUKWA_HOME}/var/tmp >&/dev/null
        crontab ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} > /dev/null 2>&1
        rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
        echo "done"
    fi

    mkdir -p "$CHUKWA_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$CHUKWA_MASTER" != "" ]; then
      echo rsync from $CHUKWA_MASTER
      rsync -a -e ssh --delete --exclude=.svn $CHUKWA_MASTER/ "$CHUKWA_HOME"
    fi

    chukwa_rotate_log $log
    echo starting $command, logging to $log
    cd "$CHUKWA_HOME"
    nohup nice -n $CHUKWA_NICENESS "$CHUKWA_HOME"/bin/chukwa -config $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ "${WATCHDOG}" != "" ]; then
        # remove watchdog
        crontab -l | grep -v ${CHUKWA_HOME}/bin/watchdog.sh | grep -v ${CHUKWA_HOME}/var/run/watchdog.out | grep -v ${CHUKWA_HOME}/tools/expire.sh > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
        crontab ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
        rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    fi

    if [ -f $CHUKWA_HOME/bin/$command ]; then
      $CHUKWA_HOME/bin/$command stop
      rm -f $pid
    else
      if [ -f $pid ]; then
        if kill -0 `cat $pid` > /dev/null 2>&1; then
          echo stopping $command
          kill `cat $pid`
          rm -f $pid
        else
          echo no $command to stop
        fi
      else
        echo no $command to stop
      fi
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


