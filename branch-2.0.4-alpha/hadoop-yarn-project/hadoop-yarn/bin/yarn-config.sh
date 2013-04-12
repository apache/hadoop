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
bin=`which "$0"`
bin=`dirname "${bin}"`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
if [ -e "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]; then
  . ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh
elif [ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh" ]; then
  . "$HADOOP_COMMON_HOME"/libexec/hadoop-config.sh
elif [ -e "${HADOOP_HOME}/libexec/hadoop-config.sh" ]; then
  . "$HADOOP_HOME"/libexec/hadoop-config.sh
else
  echo "Hadoop common not found."
  exit
fi

# Same glibc bug that discovered in Hadoop.
# Without this you can see very large vmem settings on containers.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      YARN_CONF_DIR=$confdir
    fi
fi
 
# Allow alternate conf dir location.
export YARN_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_YARN_HOME/conf}"

#check to see it is specified whether to use the slaves or the
# masters file
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        slavesfile=$1
        shift
        export YARN_SLAVES="${YARN_CONF_DIR}/$slavesfile"
    fi
fi
