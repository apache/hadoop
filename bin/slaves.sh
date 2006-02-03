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

if [ "$HADOOP_SLAVES" = "" ]; then
  export HADOOP_SLAVES=$HOME/.slaves
fi

for slave in `cat $HADOOP_SLAVES`; do
 ssh -o ConnectTimeout=1 $slave "$@" \
   2>&1 | sed "s/^/$slave: /" &
done

wait
