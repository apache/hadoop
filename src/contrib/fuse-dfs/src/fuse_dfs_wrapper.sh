#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
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
 HADOOP_HOME=/usr/local/share/hadoop
fi

for f in ls $HADOOP_HOME/lib/*.jar $HADOOP_HOME/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

if [ "$OS_ARCH" = "" ]; then
 OS_ARCH=amd64
fi

if [ "$JAVA_HOME" = "" ]; then
  JAVA_HOME=/usr/local/java
fi

if [ "$LD_LIBRARY_PATH" = "" ]; then
 LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$OS_ARCH/server:/usr/local/share/hdfs/libhdfs/:/usr/local/lib
fi

./fuse_dfs $@  -o-o allow_other
