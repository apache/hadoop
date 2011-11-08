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
# See HADOOP-6255 for directory structure layout
HADOOP_DEFAULT_PREFIX=`dirname "$this"`/..
HADOOP_PREFIX=${HADOOP_PREFIX:-$HADOOP_DEFAULT_PREFIX}
export HADOOP_PREFIX

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
if [ -e "${HADOOP_PREFIX}/conf/hadoop-env.sh" ]; then
  DEFAULT_CONF_DIR="conf"
else
  DEFAULT_CONF_DIR="etc/hadoop"
fi

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_PREFIX/$DEFAULT_CONF_DIR}"

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

# Attempt to set JAVA_HOME if it is not set
if [[ -z $JAVA_HOME ]]; then
  # On OSX use java_home (or /Library for older versions)
  if [ "Darwin" == "$(uname -s)" ]; then
    if [ -x /usr/libexec/java_home ]; then
      export JAVA_HOME=($(/usr/libexec/java_home))
    else
      export JAVA_HOME=(/Library/Java/Home)
    fi
  fi

  # Bail if we did not detect it
  if [[ -z $JAVA_HOME ]]; then
    echo "Error: JAVA_HOME is not set and could not be found." 1>&2
    exit 1
  fi
fi

JAVA=$JAVA_HOME/bin/java
# some Java parameters
JAVA_HEAP_MAX=-Xmx1000m 

# check envvars which might override default args
if [ "$HADOOP_HEAPSIZE" != "" ]; then
  #echo "run with heapsize $HADOOP_HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$HADOOP_HEAPSIZE""m"
  #echo $JAVA_HEAP_MAX
fi

# CLASSPATH initially contains $HADOOP_CONF_DIR
CLASSPATH="${HADOOP_CONF_DIR}"

# so that filenames w/ spaces are handled correctly in loops below
IFS=

# for releases, add core hadoop jar & webapps to CLASSPATH
if [ -d "$HADOOP_PREFIX/share/hadoop/common/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_PREFIX/share/hadoop/common/webapps
fi

if [ -d "$HADOOP_PREFIX/share/hadoop/common/lib" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_PREFIX/share/hadoop/common/lib'/*'
fi

CLASSPATH=${CLASSPATH}:$HADOOP_PREFIX/share/hadoop/common'/*'

# add user-specified CLASSPATH last
if [ "$HADOOP_CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:${HADOOP_CLASSPATH}
fi

# default log directory & file
if [ "$HADOOP_LOG_DIR" = "" ]; then
  HADOOP_LOG_DIR="$HADOOP_PREFIX/logs"
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
  HADOOP_PREFIX=`cygpath -w "$HADOOP_PREFIX"`
  HADOOP_LOG_DIR=`cygpath -w "$HADOOP_LOG_DIR"`
  JAVA_LIBRARY_PATH=`cygpath -w "$JAVA_LIBRARY_PATH"`
fi

# setup 'java.library.path' for native-hadoop code if necessary

if [ -d "${HADOOP_PREFIX}/build/native" -o -d "${HADOOP_PREFIX}/lib/native" ]; then
  JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} -Xmx32m ${HADOOP_JAVA_PLATFORM_OPTS} org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`
  
  if [ -d "$HADOOP_PREFIX/build/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
        JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_PREFIX}/build/native/${JAVA_PLATFORM}/lib
    else
        JAVA_LIBRARY_PATH=${HADOOP_PREFIX}/build/native/${JAVA_PLATFORM}/lib
    fi
  fi
  
  if [ -d "${HADOOP_PREFIX}/lib/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_PREFIX}/lib/native/${JAVA_PLATFORM}
    else
      JAVA_LIBRARY_PATH=${HADOOP_PREFIX}/lib/native/${JAVA_PLATFORM}
    fi
  fi
fi

if [ -e "${HADOOP_PREFIX}/lib/libhadoop.a" ]; then
  JAVA_LIBRARY_PATH=${HADOOP_PREFIX}/lib
fi

# cygwin path translation
if $cygwin; then
  JAVA_LIBRARY_PATH=`cygpath -p "$JAVA_LIBRARY_PATH"`
fi

HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.dir=$HADOOP_LOG_DIR"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.file=$HADOOP_LOGFILE"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.home.dir=$HADOOP_PREFIX"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.id.str=$HADOOP_IDENT_STRING"
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.root.logger=${HADOOP_ROOT_LOGGER:-INFO,console}"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
fi  
HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.policy.file=$HADOOP_POLICYFILE"

# Disable ipv6 as it can cause issues
HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"

# put hdfs in classpath if present
if [ "$HADOOP_HDFS_HOME" = "" ]; then
  if [ -d "${HADOOP_PREFIX}/share/hadoop/hdfs" ]; then
    HADOOP_HDFS_HOME=$HADOOP_PREFIX
  fi
fi

if [ -d "$HADOOP_HDFS_HOME/share/hadoop/hdfs/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HDFS_HOME/share/hadoop/hdfs
fi

if [ -d "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib'/*'
fi

CLASSPATH=${CLASSPATH}:$HADOOP_HDFS_HOME/share/hadoop/hdfs'/*'

# cygwin path translation
if $cygwin; then
  HADOOP_HDFS_HOME=`cygpath -w "$HADOOP_HDFS_HOME"`
fi

# cygwin path translation
if $cygwin; then
  TOOL_PATH=`cygpath -p -w "$TOOL_PATH"`
fi


