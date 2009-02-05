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


# the root of the Chukwa installation
export CHUKWA_HOME=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
          then
              shift
              confdir=$1
              shift
              export CHUKWA_CONF_DIR=$confdir
    fi
fi

#check to see it is specified whether to use the slaves or the
# masters file
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        slavesfile=$1
        shift
        export CHUKWA_SLAVES="${CHUKWA_CONF_DIR}/$slavesfile"
    fi
fi

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--watchdog" = "$1" ]
          then
              shift
              WATCHDOG="true"
    fi
fi

if [ -z ${CHUKWA_LOG_DIR} ]; then
    export CHUKWA_LOG_DIR="$CHUKWA_HOME/logs"
fi

if [ -z ${CHUKWA_PID_DIR} ]; then
    export CHUKWA_PID_DIR="${CHUKWA_HOME}/var/run"
fi

CHUKWA_VERSION=`cat ${CHUKWA_HOME}/bin/VERSION`

# Allow alternate conf dir location.
if [ -z "$CHUKWA_CONF_DIR" ]; then
    CHUKWA_CONF_DIR="${CHUKWA_CONF_DIR:-$CHUKWA_HOME/conf}"
    export CHUKWA_CONF_DIR=${CHUKWA_HOME}/conf
fi

if [ -f "${CHUKWA_CONF_DIR}/chukwa-env.sh" ]; then
  . "${CHUKWA_CONF_DIR}/chukwa-env.sh"
fi

export DATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml
COMMON=`ls ${CHUKWA_HOME}/lib/*.jar ${CHUKWA_HOME}/hadoopjars/commons*.jar`
export COMMON=`echo ${COMMON} | sed 'y/ /:/'`
export CHUKWA_CORE=${CHUKWA_HOME}/chukwa-core-${CHUKWA_VERSION}.jar
export CHUKWA_AGENT=${CHUKWA_HOME}/chukwa-agent-${CHUKWA_VERSION}.jar
export CURRENT_DATE=`date +%Y%m%d%H%M`

if [ -z ${HADOOP_JAR} ]; then
  if [ -z ${HADOOP_HOME} ]; then
        export HADOOP_HOME=../../..
    fi
    if [ -d ${HADOOP_HOME} ]; then
        export HADOOP_JAR=`ls ${HADOOP_HOME}/hadoop-*-core.jar`
        if [ -z ${HADOOP_JAR} ]; then
            echo "Please make sure hadoop-*-core.jar exists in ${HADOOP_HOME}"
            exit -1
        fi
    else
        if [ -d ${CHUKWA_HOME}/hadoopjars ]; then
            echo "WARNING: neither HADOOP_HOME nor HADOOP_JAR is set we we are reverting to defaults in $CHUKWA_HOME/hadoopjars dir"
            export HADOOP_JAR=`ls ${CHUKWA_HOME}/hadoopjars/hadoop-*-core.jar`
        else
            echo "Please make sure hadoop-*-core.jar exists in ${CHUKWA_HOME}/hadoopjars"
            exit -1
        fi
    fi
fi

if [ -z "$JAVA_HOME" ] ; then
  echo ERROR! You forgot to set JAVA_HOME in conf/chukwa-env.sh   
fi

export JPS="ps ax"

