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

# included in all the hadoop scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink

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
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

. ${bin}/../conf/chukwa-env.sh

export HADOOP_HOME="${HADOOP_HOME:-${bin}/../../../..}"

# the root of the Chukwa installation
if [ -z $CHUKWA_HOME ] ; then
CHUKWA_HOME=`dirname "$this"`/..
export CHUKWA_HOME=`cd $CHUKWA_HOME; pwd`
fi

chukwaVersion=`cat ${CHUKWA_HOME}/bin/VERSION`
DEFAULT_CHUKWA_HOME=${CHUKWA_HOME}/logs/
export CHUKWA_LOG_DIR="${CHUKWA_LOG_DIR:-$DEFAULT_CHUKWA_HOME}"
if [ ! -d $CHUKWA_LOG_DIR ]; then
  mkdir -p $CHUKWA_LOG_DIR
fi

export chuwaRecordsRepository="/chukwa/repos/demo"

export DATACONFIG=${CHUKWA_HOME}/conf/mdl.xml
common=`ls ${CHUKWA_HOME}/lib/*.jar`
export common=`echo ${common} | sed 'y/ /:/'`

#chukwaCore=${HADOOP_HOME}/build/contrib/chukwa/chukwa-core-${chukwaVersion}.jar
chukwaCore=${HADOOP_HOME}/build/contrib/chukwa
if [ -a $chukwaCore ] ; then
  export chukwaCore
else
  echo ${chukwaCore} does not exist
  export chukwaCore=${CHUKWA_HOME}/chukwa-core-${chukwaVersion}.jar
fi

#chukwaAgent=${HADOOP_HOME}/build/contrib/chukwa/chukwa-agent-${chukwaVersion}.jar
chukwaAgent=${HADOOP_HOME}/build/contrib/chukwa
if [ -a $chukwaAgent ] ; then
  export chukwaAgent
else
  echo ${chukwaAgent} does not exist
  export chukwaAgent=${CHUKWA_HOME}/chukwa-agent-${chukwaVersion}.jar
fi

echo chukwaCore is ${chukwaCore} and chukwaAgent is ${chukwaAgent}

export CURRENT_DATE=`date +%Y%m%d%H%M`
export TS_CONFIG=${CHUKWA_HOME}/conf/ts
export tomcat=${CHUKWA_HOME}/opt/apache-tomcat-6.0.16
if [ -d ${HADOOP_HOME}/build/classes ]; then
  DEFAULT_HADOOP_JAR=${HADOOP_HOME}/build/classes
# this doesn't work, but needs to be replaced with something that does
#elif [ls ${HADOOP_HOME}/build/hadoop-*-core.jar` ]; then
#  echo setting DEFAULT_HADOOP_JAR to `ls ${HADOOP_HOME}/build/hadoop-*-core.jar`
#  DEFAULT_HADOOP_JAR=`ls ${HADOOP_HOME}/build/hadoop-*-core.jar`
else
  DEFAULT_HADOOP_JAR=${CHUKWA_HOME}/hadoopjars/hadoop-0.18.0-core.jar
fi
export HADOOP_JAR=${HADOOP_JAR:-$DEFAULT_HADOOP_JAR}

echo
echo HADOOP_JAR is $HADOOP_JAR
echo

export CHUKWA_LOG_DIR="${CHUKWA_HOME}/logs/"
DEFAULT_PID_DIR=${CHUKWA_HOME}/var/run
export CHUKWA_PID_DIR="${CHUKWA_PID_DIR:-$DEFAULT_PID_DIR}"
export chuwaRecordsRepository="/chukwa/repos/demo"

