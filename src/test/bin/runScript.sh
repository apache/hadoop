#!/bin/bash

TZ=PDT8PST  date
TZ=EDT5EST  date

if [ $# -eq 0 ]
then
	echo "No arguments given. Exiting." 1>&2
	exit 1
fi
	if [ -z "$GIT_REPO" -a -z "$SVN_REPO" ]
	then
		echo "Serious error: neither SVN_REPO nor GIT_REPO were specified." 1>&2
		exit 1
	fi
	if [ -n "$GIT_REPO" -a -n "$SVN_REPO" ]
	then
		echo "Serious error: both SVN_REPO and GIT_REPO were specified." 1>&2
		exit 1
	fi
	if [ -n "$GIT_REPO" ]
	then
		if [ -d hudson ]
		then
	    		(cd hudson && git pull)
		else
	    	# git clone ssh://gwbl2001.blue.ygrid.yahoo.com/grid/0/gs/gridre/hudson.git
	    	git clone $GIT_REPO
		fi
	fi
	if [ -n "$SVN_REPO" ]
	then
		svn co $SVN_REPO
	fi

echo  ============  hostname = `hostname`

echo "This script does not yet unset JAVA_CMD, LD_LIBRARY_PATH, and does not yet set JAVA_HOME." | fmt
set -x
exec $*
