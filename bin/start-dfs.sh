#!/usr/bin/env bash

# Start hadoop dfs daemons.
# Optinally upgrade or rollback dfs state.
# Run this on master node.
##

usage="Usage: start-dfs.sh [-upgrade|-rollback]"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hadoop-config.sh

# get arguments
if [ $# -ge 1 ]; then
	nameStartOpt=$1
	shift
	case $nameStartOpt in
	  (-upgrade)
	  	;;
	  (-rollback) 
	  	dataStartOpt=$nameStartOpt
	  	;;
	  (*)
		  echo $usage
		  exit 1
	    ;;
	esac
fi

# start dfs daemons
# start namenode after datanodes, to minimize time namenode is up w/o data
# note: datanodes will log connection errors until namenode starts
"$bin"/hadoop-daemon.sh --config $HADOOP_CONF_DIR start namenode $nameStartOpt
"$bin"/hadoop-daemons.sh --config $HADOOP_CONF_DIR start datanode $dataStartOpt
"$bin"/hadoop-daemons.sh --config $HADOOP_CONF_DIR --hosts masters start secondarynamenode
