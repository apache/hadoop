#!/usr/bin/env bash
#
#/**
# * Copyright 2011 The Apache Software Foundation
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
 
# Move regions off a server then stop it.  Optionally restart and reload.
# Turn off the balancer before running this script.
function usage {
  echo "Usage: graceful_stop.sh [--config <conf-dir>] [--restart] [--reload] <hostname>" 
  echo " restart     If we should restart after graceful stop"
  echo " reload      Move offloaded regions back on to the stopped server"
  echo " debug       Move offloaded regions back on to the stopped server"
  echo " hostname    Hostname of server we are to stop"
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
# This will set HBASE_HOME, etc.
. "$bin"/hbase-config.sh
# Get arguments
restart=
reload=
debug=
while [ $# -gt 0 ]
do
  case "$1" in
    --restart)  restart=true; shift;;
    --reload)   reload=true; shift;;
    --debug)    debug="--debug"; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;	# terminate while loop
  esac
done

# "$@" contains the rest. Must be at least the hostname left.
if [ $# -lt 1 ]; then
  usage
fi

hostname=$1
filename="/tmp/$hostname"
# Run the region mover script.
echo "Unloading $hostname region(s)"
HBASE_NOEXEC=true "$bin"/hbase org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug unload $hostname
echo "Unloaded $hostname region(s)"
# Stop the server. Have to put hostname into its own little file for hbase-daemons.sh
hosts="/tmp/$(basename $0).$$.tmp"
echo $hostname >> $hosts
"$bin"/hbase-daemons.sh --hosts ${hosts} stop regionserver
if [ "$restart" != "" ]; then
  "$bin"/hbase-daemons.sh --hosts ${hosts} start regionserver
  if [ "$reload" != "" ]; then
    echo "Reloading $hostname region(s)"
    HBASE_NOEXEC=true "$bin"/hbase org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug load $hostname
    echo "Reloaded $hostname region(s)"
  fi
fi

# Cleanup tmp files.
trap "rm -f  "/tmp/$(basename $0).*.tmp" &> /dev/null" EXIT
