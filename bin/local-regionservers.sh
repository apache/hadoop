#!/bin/sh
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
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
