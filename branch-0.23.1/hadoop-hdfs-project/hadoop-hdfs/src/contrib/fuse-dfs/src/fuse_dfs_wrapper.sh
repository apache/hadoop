#!/usr/bin/env bash
#
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
#

export HADOOP_PREFIX=${HADOOP_PREFIX:-/usr/local/share/hadoop}

if [ "$OS_ARCH" = "" ]; then
export OS_ARCH=amd64
fi

if [ "$JAVA_HOME" = "" ]; then
export  JAVA_HOME=/usr/local/java
fi

if [ "$LD_LIBRARY_PATH" = "" ]; then
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$OS_ARCH/server:/usr/local/lib
fi

# If dev build set paths accordingly
if [ -d $HADOOP_PREFIX/build ]; then
  export HADOOP_PREFIX=$HADOOP_PREFIX
  for f in ${HADOOP_PREFIX}/build/*.jar ; do
    export CLASSPATH=$CLASSPATH:$f
  done
  for f in $HADOOP_PREFIX/build/ivy/lib/hadoop-hdfs/common/*.jar ; do
    export CLASSPATH=$CLASSPATH:$f
  done
  export PATH=$HADOOP_PREFIX/build/contrib/fuse-dfs:$PATH
  export LD_LIBRARY_PATH=$HADOOP_PREFIX/build/c++/lib:$JAVA_HOME/jre/lib/$OS_ARCH/server
fi

fuse_dfs $@
