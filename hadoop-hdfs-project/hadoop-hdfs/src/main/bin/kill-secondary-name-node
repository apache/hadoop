#!/bin/sh

# HDP configures the SecondaryNameNode JVM process to call this script on
# OutOfMemoryError.  This will find and kill the SecondaryNameNode process.  It's
# potentially dangerous to keep running the SecondaryNameNode after an
# OutOfMemoryError.  There is a risk that a file system change could be applied
# to the in-memory metadata but not made persistent to the edit log.  HDFS
# contains legacy code that catches OutOfMemoryError, so we need to use this
# script to kill it externally.

SNNPID=$("$JAVA_HOME"/bin/jps | grep -E '^[0-9]+[ ]+SecondaryNameNode$' | awk '{print $1}')

if [ $? -gt 0 ]; then
  echo "ERROR: Command failed while looking for SecondaryNameNode PID."
  exit 1
fi

if [ -z "$SNNPID" ];
then
  echo "ERROR: Could not find a SecondaryNameNode PID."
  exit 1
fi

kill -9 "$SNNPID"

if [ $? -gt 0 ]; then
  echo "ERROR: Kill command failed."
  exit 1
fi
