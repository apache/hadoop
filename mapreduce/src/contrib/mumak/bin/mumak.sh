#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# resolve links - $0 may be a softlink
project=mumak
HADOOP_VERSION= 

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
bin=`cd "$bin"; pwd`
script=`basename $this`
this="$bin/$script"

MUMAK_HOME=`dirname $bin`
if [ -d "$MUMAK_HOME/../../../build/classes" ]; then
  HADOOP_HOME=`cd $MUMAK_HOME/../../.. ; pwd`
  IN_RELEASE=0
else
  HADOOP_HOME=`cd $MUMAK_HOME/../.. ; pwd`
  IN_RELEASE=1
  
  MAPRED_JAR=$HADOOP_HOME/hadoop-mapred-${HADOOP_VERSION}.jar
  if [ ! -e $MAPRED_JAR ]; then
    echo "Error: Cannot find $MAPRED_JAR."
    exit 1
  fi
fi

# parse command line option
if [ $# -gt 1 ]
then
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    HADOOP_CONF_DIR=$confdir
  fi
fi

# Allow alternate conf dir location.
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# Define HADOOP_COMMON_HOME
if [ "$HADOP_CORE_HOME" = "" ]; then
  HADOOP_COMMON_HOME=$HADOOP_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1200m 

# Setting classpath
# Mumak needs to have the followinw classes and resources in place (roughly in this
# order):
# Mumak's conf directory (log4j.properties), must override Hadoop's conf dir.
# Hadoop's conf directory
# Mumak classes (including aspectj-generated classes) (or mumak jar), must
#     override MapReduce project classes or jar..
# MapReduce project classes (mapred jar)
# MapReduce webapps files (included in mapred jar)
# MapReduce tools classes (or mapred-tools jar)
# Hadoop Common jar
# Hadoop Common test jar
# Depending 3rd party jars
CLASSPATH=${MUMAK_HOME}/conf:${HADOOP_CONF_DIR}:$JAVA_HOME/lib/tools.jar

if [ $IN_RELEASE = 0 ]; then
  CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/build/contrib/${project}/classes
  CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/build/classes
  CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/build
  CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/build/tools
  # add libs to CLASSPATH
  for f in $HADOOP_HOME/lib/hadoop-core-*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done

  for f in $HADOOP_HOME/build/ivy/lib/${project}/common/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done

  for f in $HADOOP_HOME/build/ivy/lib/${project}/test/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done
else
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME;
  for f in $HADOOP_HOME/lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done
  CLASSPATH=${CLASSPATH}:$MUMAK_HOME/hadoop-${HADOOP_VERSION}-${project}.jar
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/hadoop-mapred-${HADOOP_VERSION}.jar
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/hadoop-mapred-tools-${HADOOP_VERSION}.jar
fi

# check envvars which might override default args
if [ "$HADOOP_HEAPSIZE" != "" ]; then
  #echo "run with heapsize $HADOOP_HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$HADOOP_HEAPSIZE""m"
  #echo $JAVA_HEAP_MAX
fi

# default log directory & file
if [ "$HADOOP_LOG_DIR" = "" ]; then
  HADOOP_LOG_DIR="$HADOOP_HOME/logs"
fi

# default policy file for service-level authorization
if [ "$HADOOP_POLICYFILE" = "" ]; then
  HADOOP_POLICYFILE="hadoop-policy.xml"
fi

# setup 'java.library.path' for native-hadoop code if necessary
JAVA_LIBRARY_PATH=''
if [ -d "${HADOOP_COMMON_HOME}/build/native" -o -d "${HADOOP_COMMON_HOME}/lib/native" ]; then
  JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} -Xmx32m org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`
  
  if [ -d "$HADOOP_COMMON_HOME/build/native" ]; then
    JAVA_LIBRARY_PATH=${HADOOP_COMMON_HOME}/build/native/${JAVA_PLATFORM}/lib
  fi
  
  if [ -d "${HADOOP_COMMON_HOME}/lib/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_COMMON_HOME}/lib/native/${JAVA_PLATFORM}
    else
      JAVA_LIBRARY_PATH=${HADOOP_COMMON_HOME}/lib/native/${JAVA_PLATFORM}
    fi
  fi
fi

HADOOP_OPTS="$HADOOP_OPTS -Dmumak.log.dir=$HADOOP_LOG_DIR"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.dir=$HADOOP_LOG_DIR"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.tmp.dir=$HADOOP_LOG_DIR/tmp"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
fi  
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.policy.file=$HADOOP_POLICYFILE"

function print_usage(){
  echo "Usage: $script [--config dir] trace.json topology.json"
}

if [ $# != 2 ]; then
  print_usage
  exit
fi

exec "$JAVA" -enableassertions $JAVA_HEAP_MAX $HADOOP_OPTS -classpath "$CLASSPATH" org.apache.hadoop.mapred.SimulatorEngine -conf=${MUMAK_HOME}/conf/${project}.xml "$@"
