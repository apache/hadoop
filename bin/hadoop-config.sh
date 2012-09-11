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

this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=`dirname "$this"`
script=`basename "$this"`
config_bin=`cd "$config_bin"; pwd`
this="$config_bin/$script"

# the root of the Hadoop installation
export HADOOP_PREFIX=`dirname "$this"`/..

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
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_PREFIX/$DEFAULT_CONF_DIR}"

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
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
        export HADOOP_SLAVES="${HADOOP_CONF_DIR}/$slavesfile"
    fi
fi


if [ "$HADOOP_HOME_WARN_SUPPRESS" = "" ] && [ "$HADOOP_HOME" != "" ]; then
  echo "Warning: \$HADOOP_HOME is deprecated." 1>&2
  echo 1>&2
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. This interacts badly with the many threads that
# we use in Hadoop. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

export HADOOP_HOME=${HADOOP_PREFIX}
export HADOOP_HOME_WARN_SUPPRESS=1

