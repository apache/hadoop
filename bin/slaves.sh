#!/bin/bash
# 
# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   HADOOP_SLAVES    File naming remote hosts.  Default is ~/.slaves
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HADOOP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
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

# Allow alternate conf dir location.
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:=$HADOOP_HOME/conf}"

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  source "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

if [ "$HADOOP_SLAVES" = "" ]; then
  export HADOOP_SLAVES="${HADOOP_CONF_DIR}/slaves"
fi

# By default, forward HADOOP_CONF_DIR environment variable to the
# remote slave. Remote slave must have following added to its
# /etc/ssh/sshd_config:
#   AcceptEnv HADOOP_CONF_DIR
# See'man ssh_config for more on SendEnv and AcceptEnv.
if [ "$HADOOP_SSH_OPTS" = "" ]; then
  export HADOOP_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HADOOP_CONF_DIR"
fi

for slave in `cat "$HADOOP_SLAVES"`; do
 ssh $HADOOP_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$HADOOP_SLAVE_SLEEP" != "" ]; then
   sleep $HADOOP_SLAVE_SLEEP
 fi
done

wait
