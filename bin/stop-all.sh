#!/bin/sh

# Stop all hadoop daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

"$bin"/stop-mapred.sh
"$bin"/stop-dfs.sh
