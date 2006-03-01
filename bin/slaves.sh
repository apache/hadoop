#!/bin/bash
# 
# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   HADOOP_SLAVES    File naming remote hosts.  Default is ~/.slaves
##

usage="Usage: slaves.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

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

if [ -f "$HADOOP_HOME/conf/hadoop-env.sh" ]; then
  source "${HADOOP_HOME}/conf/hadoop-env.sh"
fi

if [ "$HADOOP_SLAVES" = "" ]; then
  export HADOOP_SLAVES="$HADOOP_HOME/conf/slaves"
fi

if [ "$HADOOP_SSH_OPTS" = "" ]; then
  export HADOOP_SSH_OPTS="-o ConnectTimeout=1"
fi

for slave in `cat "$HADOOP_SLAVES"`; do
 ssh $HADOOP_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
done

wait
