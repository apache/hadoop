#!/bin/sh
# This is used for starting multiple regionservers on the same machine.
# run it from hbase-dir/ just like 'bin/hbase'
# Supports up to 100 regionservers (limitation = overlapping ports)

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin" >/dev/null && pwd`

if [ $# -lt 2 ]; then
  S=`basename "${BASH_SOURCE-$0}"`
  echo "Usage: $S [start|stop] offset(s)"
  echo ""
  echo "    e.g. $S start 1 2"
  exit
fi

# sanity check: make sure your regionserver opts don't use ports [i.e. JMX/DBG]
export HBASE_REGIONSERVER_OPTS=" "

run_regionserver () {
  DN=$2
  export HBASE_IDENT_STRING="$USER-$DN"
  HBASE_REGIONSERVER_ARGS="\
    -D hbase.regionserver.port=`expr 60200 + $DN` \
    -D hbase.regionserver.info.port=`expr 60300 + $DN`"
  "$bin"/hbase-daemon.sh $1 regionserver $HBASE_REGIONSERVER_ARGS
}

cmd=$1
shift;

for i in $*
do
  run_regionserver  $cmd $i
done
