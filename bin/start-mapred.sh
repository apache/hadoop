#!/bin/bash

# Start hadoop map reduce daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# start mapred daemons
# start jobtracker first to minimize connection errors at startup
"$bin"/hadoop-daemon.sh start jobtracker
"$bin"/hadoop-daemons.sh start tasktracker
