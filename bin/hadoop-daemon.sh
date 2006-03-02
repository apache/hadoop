#!/bin/bash
# 
# Runs a Hadoop command as a daemon.
#
# Environment Variables
#
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HADOOP_LOG_DIR   Where log files are stored.  PWD by default.
#   HADOOP_MASTER    host:path where hadoop code should be rsync'd from
#   HADOOP_PID_DIR   The pid files are stored. /tmp by default.
#   HADOOP_IDENT_STRING   A string representing this instance of hadoop. $USER by default
##

usage="Usage: hadoop-daemon [start|stop] [hadoop-command] [args...]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

# get arguments
startStop=$1
shift
command=$1
shift

# resolve links - $0 may be a softlink
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# the root of the Hadoop installation
HADOOP_HOME=`dirname "$this"`/..

# Allow alternate conf dir location.
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  source "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# get log directory
if [ "$HADOOP_LOG_DIR" = "" ]; then
  HADOOP_LOG_DIR="$HADOOP_HOME/logs"
  mkdir -p "$HADOOP_LOG_DIR"
fi

if [ "$HADOOP_PID_DIR" = "" ]; then
  HADOOP_PID_DIR=/tmp
fi

if [ "$HADOOP_IDENT_STRING" = "" ]; then
  HADOOP_IDENT_STRING=$USER
fi

# some variables
log=$HADOOP_LOG_DIR/hadoop-$HADOOP_IDENT_STRING-$command-`hostname`.log
pid=$HADOOP_PID_DIR/hadoop-$HADOOP_IDENT_STRING-$command.pid

case $startStop in

  (start)

    if [ -f $pid ]; then
      if [ -a /proc/`cat $pid` ]; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$HADOOP_MASTER" != "" ]; then
      echo rsync from $HADOOP_MASTER
      rsync -a -e ssh --delete --exclude=.svn $HADOOP_MASTER/ "$HADOOP_HOME"
    fi

    cd "$HADOOP_HOME"
    echo starting $command, logging to $log
    nohup bin/hadoop $command "$@" >& "$log" < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if [ -a /proc/`cat $pid` ]; then
        echo stopping $command
        kill `cat $pid`
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


