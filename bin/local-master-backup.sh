#!/bin/sh
# This is used for starting multiple masters on the same machine.
# run it from hbase-dir/ just like 'bin/hbase'
# Supports up to 10 masters (limitation = overlapping ports)

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin" >/dev/null && pwd`

if [ $# -lt 2 ]; then
  S=`basename "${BASH_SOURCE-$0}"`
  echo "Usage: $S [start|stop] offset(s)"
  echo ""
  echo "    e.g. $S start 1"
  exit
fi

# sanity check: make sure your master opts don't use ports [i.e. JMX/DBG]
export HBASE_MASTER_OPTS=" "

run_master () {
  DN=$2
  export HBASE_IDENT_STRING="$USER-$DN"
  HBASE_MASTER_ARGS="\
    --backup \
    -D hbase.master.port=`expr 60000 + $DN` \
    -D hbase.master.info.port=`expr 60010 + $DN`"
  "$bin"/hbase-daemon.sh $1 master $HBASE_MASTER_ARGS
}

cmd=$1
shift;

for i in $*
do
  run_master  $cmd $i
done
