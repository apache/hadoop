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

if [ "$HADOOP_HOME" = "" ]; then
  echo "HADOOP_HOME is empty. Set it to the root directory of Hadoop source code"
  exit 1
fi
export FUSEDFS_PATH="$HADOOP_HOME/hadoop-hdfs-project/hadoop-hdfs-native-client/target/main/native/fuse-dfs"
export LIBHDFS_PATH="$HADOOP_HOME/hadoop-hdfs-project/hadoop-hdfs-native-client/target/usr/local/lib"

if [ "$OS_ARCH" = "" ]; then
export OS_ARCH=amd64
fi

if [ "$JAVA_HOME" = "" ]; then
export  JAVA_HOME=/usr/local/java
fi

if [ "$LD_LIBRARY_PATH" = "" ]; then
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$OS_ARCH/server:/usr/local/lib
fi

while IFS= read -r -d '' file
do
  export CLASSPATH=$CLASSPATH:$file
done < <(find "$HADOOP_HOME/hadoop-client" -name "*.jar" -print0)

while IFS= read -r -d '' file
do
  export CLASSPATH=$CLASSPATH:$file
done < <(find "$HADOOP_HOME/hhadoop-hdfs-project" -name "*.jar" -print0)

export CLASSPATH=$HADOOP_CONF_DIR:$CLASSPATH
export PATH=$FUSEDFS_PATH:$PATH
export LD_LIBRARY_PATH=$LIBHDFS_PATH:$JAVA_HOME/jre/lib/$OS_ARCH/server

fuse_dfs "$@"
