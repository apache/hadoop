#!/bin/bash

TZ=PDT8PST  date
TZ=EDT5EST  date

if [ $# -eq 0 ]
then
	echo "No arguments given. Exiting." 1>&2
	exit 1
fi
if [ "$GIT_REPO_NOT_NECESSARY" != true ]
then
	if [ -z "$GIT_REPO" ]
	then
		echo "Serious error: GIT_REPO was not specified." 1>&2
		exit 1
	fi
	if [ -d hudson ]
	then
	    (cd hudson && git pull)
	else
	    # git clone ssh://gwbl2001.blue.ygrid.yahoo.com/grid/0/gs/gridre/hudson.git
	    git clone $GIT_REPO
	fi
fi

echo  ============  hostname = `hostname`

echo "This script does not yet unset JAVA_CMD, LD_LIBRARY_PATH, and does not yet set JAVA_HOME." | fmt
set -x
exec $*
