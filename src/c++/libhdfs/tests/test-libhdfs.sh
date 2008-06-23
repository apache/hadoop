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

#
# Note: This script depends on 5 environment variables to function correctly:
# a) CLASSPATH
# b) HADOOP_HOME
# c) HADOOP_CONF_DIR 
# d) HADOOP_LOG_DIR 
# e) LIBHDFS_BUILD_DIR
# All these are passed by build.xml.
#

HDFS_TEST=hdfs_test
HADOOP_LIB_DIR=$HADOOP_HOME/lib
HADOOP_BIN_DIR=$HADOOP_HOME/bin

# Manipulate HADOOP_CONF_DIR so as to include 
# HADOOP_HOME/conf/hadoop-default.xml too
# which is necessary to circumvent bin/hadoop
HADOOP_CONF_DIR=$HADOOP_CONF_DIR:$HADOOP_HOME/conf

# set pid file dir so they are not written to /tmp
export HADOOP_PID_DIR=$HADOOP_LOG_DIR

# CLASSPATH initially contains $HADOOP_CONF_DIR
CLASSPATH="${HADOOP_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# for developers, add Hadoop classes to CLASSPATH
if [ -d "$HADOOP_HOME/build/classes" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/build/classes
fi
if [ -d "$HADOOP_HOME/build/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/build
fi
if [ -d "$HADOOP_HOME/build/test/classes" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/build/test/classes
fi

# so that filenames w/ spaces are handled correctly in loops below
IFS=

# add libs to CLASSPATH
for f in $HADOOP_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in $HADOOP_HOME/lib/jsp-2.0/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# restore ordinary behaviour
unset IFS

# Put delays to ensure hdfs is up and running and also shuts down 
# after the tests are complete
cd $HADOOP_HOME
echo Y | $HADOOP_BIN_DIR/hadoop namenode -format &&
$HADOOP_BIN_DIR/hadoop-daemon.sh start namenode && sleep 2 && 
$HADOOP_BIN_DIR/hadoop-daemon.sh start datanode && sleep 2 && 
echo CLASSPATH=$HADOOP_CONF_DIR:$CLASSPATH LD_PRELOAD="$LIBHDFS_BUILD_DIR/libhdfs.so" $LIBHDFS_BUILD_DIR/$HDFS_TEST && 
CLASSPATH=$HADOOP_CONF_DIR:$CLASSPATH LD_PRELOAD="$LIBHDFS_BUILD_DIR/libhdfs.so" $LIBHDFS_BUILD_DIR/$HDFS_TEST
BUILD_STATUS=$?
sleep 3
$HADOOP_BIN_DIR/hadoop-daemon.sh stop datanode && sleep 2 && 
$HADOOP_BIN_DIR/hadoop-daemon.sh stop namenode && sleep 2 

echo exiting with $BUILD_STATUS
exit $BUILD_STATUS
