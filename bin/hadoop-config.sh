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

# Resolve links ($0 may be a softlink) and convert a relative path
# to an absolute path.  NB: The -P option requires bash built-ins
# or POSIX:2001 compliant cd and pwd.
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# the root of the Hadoop installation
#TODO: change the env variable when dir structure is changed
export HADOOP_HOME=`dirname "$this"`/..
export HADOOP_COMMON_HOME="${HADOOP_HOME}"
#export HADOOP_HOME=`dirname "$this"`/../..
#export HADOOP_COMMON_HOME="${HADOOP_COMMON_HOME:-`dirname "$this"`/..}"

#check to see if the conf dir is given as an optional argument
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
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"

# User can specify hostnames or a file where the hostnames are (not both)
if [[ ( "$HADOOP_SLAVES" != '' ) && ( "$HADOOP_SLAVE_NAMES" != '' ) ]] ; then
  echo \
    "Error: Please specify one variable HADOOP_SLAVES or " \
    "HADOOP_SLAVE_NAME and not both."
  exit 1
fi

# Process command line options that specify hosts or file with host
# list
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        export HADOOP_SLAVES="${HADOOP_CONF_DIR}/$$1"
        shift
    elif [ "--hostnames" = "$1" ]
    then
        shift
        export HADOOP_SLAVE_NAMES=$1
        shift
    fi
fi

# User can specify hostnames or a file where the hostnames are (not both)
# (same check as above but now we know it's command line options that cause
# the problem)
if [[ ( "$HADOOP_SLAVES" != '' ) && ( "$HADOOP_SLAVE_NAMES" != '' ) ]] ; then
  echo \
    "Error: Please specify one of --hosts or --hostnames options and not both."
  exit 1
fi

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# check if net.ipv6.bindv6only is set to 1
bindv6only=$(/sbin/sysctl -n net.ipv6.bindv6only 2> /dev/null)
if [ -n "$bindv6only" ] && [ "$bindv6only" -eq "1" ] && [ "$HADOOP_ALLOW_IPV6" != "yes" ]
then
  echo "Error: \"net.ipv6.bindv6only\" is set to 1 - Java networking could be broken"
  echo "For more info: http://wiki.apache.org/hadoop/HadoopIPv6"
  exit 1
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. This interacts badly with the many threads that
# we use in Hadoop. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

# some Java parameters
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
  
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m 

# check envvars which might override default args
if [ "$HADOOP_HEAPSIZE" != "" ]; then
  #echo "run with heapsize $HADOOP_HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$HADOOP_HEAPSIZE""m"
  #echo $JAVA_HEAP_MAX
fi

# CLASSPATH initially contains $HADOOP_CONF_DIR
CLASSPATH="${HADOOP_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# for developers, add Hadoop classes to CLASSPATH
if [ -d "$HADOOP_COMMON_HOME/build/classes" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME/build/classes
fi
if [ -d "$HADOOP_COMMON_HOME/build/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME/build
fi
if [ -d "$HADOOP_COMMON_HOME/build/test/classes" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME/build/test/classes
fi
if [ -d "$HADOOP_COMMON_HOME/build/test/core/classes" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME/build/test/core/classes
fi

# so that filenames w/ spaces are handled correctly in loops below
IFS=

# for releases, add core hadoop jar & webapps to CLASSPATH
if [ -d "$HADOOP_COMMON_HOME/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME
fi
for f in $HADOOP_COMMON_HOME/hadoop-*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# add libs to CLASSPATH
for f in $HADOOP_COMMON_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

if [ -d "$HADOOP_COMMON_HOME/build/ivy/lib/Hadoop-Common/common" ]; then
for f in $HADOOP_COMMON_HOME/build/ivy/lib/Hadoop-Common/common/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
fi

if [ -d "$HADOOP_COMMON_HOME/build/ivy/lib/Hadoop-Hdfs/common" ]; then
for f in $HADOOP_COMMON_HOME/build/ivy/lib/Hadoop-Hdfs/common/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
fi

if [ -d "$HADOOP_COMMON_HOME/build/ivy/lib/Hadoop/common" ]; then
for f in $HADOOP_COMMON_HOME/build/ivy/lib/Hadoop/common/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
fi

for f in $HADOOP_COMMON_HOME/lib/jsp-2.1/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# add user-specified CLASSPATH last
if [ "$HADOOP_CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:${HADOOP_CLASSPATH}
fi

# default log directory & file
if [ "$HADOOP_LOG_DIR" = "" ]; then
  HADOOP_LOG_DIR="$HADOOP_HOME/logs"
fi
if [ "$HADOOP_LOGFILE" = "" ]; then
  HADOOP_LOGFILE='hadoop.log'
fi

# default policy file for service-level authorization
if [ "$HADOOP_POLICYFILE" = "" ]; then
  HADOOP_POLICYFILE="hadoop-policy.xml"
fi

# restore ordinary behaviour
unset IFS

# cygwin path translation
if $cygwin; then
  HADOOP_COMMON_HOME=`cygpath -w "$HADOOP_COMMON_HOME"`
  HADOOP_LOG_DIR=`cygpath -w "$HADOOP_LOG_DIR"`
  JAVA_LIBRARY_PATH=`cygpath -w "$JAVA_LIBRARY_PATH"`
fi

# setup 'java.library.path' for native-hadoop code if necessary

if [ -d "${HADOOP_COMMON_HOME}/build/native" -o -d "${HADOOP_COMMON_HOME}/lib/native" ]; then
  JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} -Xmx32m ${HADOOP_JAVA_PLATFORM_OPTS} org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`
  
  if [ -d "$HADOOP_COMMON_HOME/build/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
        JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_COMMON_HOME}/build/native/${JAVA_PLATFORM}/lib
    else
        JAVA_LIBRARY_PATH=${HADOOP_COMMON_HOME}/build/native/${JAVA_PLATFORM}/lib
    fi
  fi
  
  if [ -d "${HADOOP_COMMON_HOME}/lib/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_COMMON_HOME}/lib/native/${JAVA_PLATFORM}
    else
      JAVA_LIBRARY_PATH=${HADOOP_COMMON_HOME}/lib/native/${JAVA_PLATFORM}
    fi
  fi
fi

# cygwin path translation
if $cygwin; then
  JAVA_LIBRARY_PATH=`cygpath -p "$JAVA_LIBRARY_PATH"`
fi

HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.dir=$HADOOP_LOG_DIR"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.file=$HADOOP_LOGFILE"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.home.dir=$HADOOP_COMMON_HOME"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.id.str=$HADOOP_IDENT_STRING"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.root.logger=${HADOOP_ROOT_LOGGER:-INFO,console}"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER:-INFO,console}"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
fi  
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.policy.file=$HADOOP_POLICYFILE"

# Disable ipv6 as it can cause issues
HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"

# put hdfs in classpath if present
if [ "$HADOOP_HDFS_HOME" = "" ]; then
  if [ -d "${HADOOP_HOME}/hdfs" ]; then
    HADOOP_HDFS_HOME=$HADOOP_HOME/hdfs
    #echo Found HDFS installed at $HADOOP_HDFS_HOME
  fi
fi

if [ -d "${HADOOP_HDFS_HOME}" ]; then

  if [ -d "$HADOOP_HDFS_HOME/webapps" ]; then
    CLASSPATH=${CLASSPATH}:$HADOOP_HDFS_HOME
  fi
  
  if [ -d "${HADOOP_HDFS_HOME}/conf" ]; then
    CLASSPATH=${CLASSPATH}:${HADOOP_HDFS_HOME}/conf
  fi
  
  for f in $HADOOP_HDFS_HOME/hadoop-hdfs-*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done

  # add libs to CLASSPATH
  for f in $HADOOP_HDFS_HOME/lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done
  
  if [ -d "$HADOOP_HDFS_HOME/build/classes" ]; then
    CLASSPATH=${CLASSPATH}:$HADOOP_HDFS_HOME/build/classes
  fi
fi

# cygwin path translation
if $cygwin; then
  HADOOP_HDFS_HOME=`cygpath -w "$HADOOP_HDFS_HOME"`
fi

# set mapred home if mapred is present
if [ "$HADOOP_MAPRED_HOME" = "" ]; then
  if [ -d "${HADOOP_HOME}/mapred" ]; then
    HADOOP_MAPRED_HOME=$HADOOP_HOME/mapred
    #echo Found MAPRED installed at $HADOOP_MAPRED_HOME
  fi
fi

if [ -d "${HADOOP_MAPRED_HOME}" ]; then

  if [ -d "$HADOOP_MAPRED_HOME/webapps" ]; then
    CLASSPATH=${CLASSPATH}:$HADOOP_MAPRED_HOME
  fi

  if [ -d "${HADOOP_MAPRED_HOME}/conf" ]; then
    CLASSPATH=${CLASSPATH}:${HADOOP_MAPRED_HOME}/conf
  fi
  
  for f in $HADOOP_MAPRED_HOME/hadoop-mapred-*.jar; do
    CLASSPATH=${CLASSPATH}:$f
  done

  for f in $HADOOP_MAPRED_HOME/lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f
  done

  if [ -d "$HADOOP_MAPRED_HOME/build/classes" ]; then
    CLASSPATH=${CLASSPATH}:$HADOOP_MAPRED_HOME/build/classes
  fi

  if [ -d "$HADOOP_MAPRED_HOME/build/tools" ]; then
    CLASSPATH=${CLASSPATH}:$HADOOP_MAPRED_HOME/build/tools
  fi

  for f in $HADOOP_MAPRED_HOME/hadoop-mapred-tools-*.jar; do
    TOOL_PATH=${TOOL_PATH}:$f;
  done
  for f in $HADOOP_MAPRED_HOME/build/hadoop-mapred-tools-*.jar; do
    TOOL_PATH=${TOOL_PATH}:$f;
  done
fi

# cygwin path translation
if $cygwin; then
  HADOOP_MAPRED_HOME=`cygpath -w "$HADOOP_MAPRED_HOME"`
  TOOL_PATH=`cygpath -p -w "$TOOL_PATH"`
fi


