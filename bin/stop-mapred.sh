#!/bin/bash

# Stop hadoop map reduce daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

"$bin"/hadoop-daemon.sh stop jobtracker
"$bin"/hadoop-daemons.sh stop tasktracker

