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

#
# Note: This script depends on 5 environment variables to function correctly:
# a) HADOOP_HOME - must be set
# b) HDFS_TEST_CONF_DIR - optional; the directory to read and write
# core-site.xml to. Defaults to /tmp
# c) LIBHDFS_BUILD_DIR - optional; the location of the hdfs_test
# executable. Defaults to the parent directory.
# d) OS_NAME - used to choose how to locate libjvm.so
# e) CLOVER_JAR - optional; the location of the Clover code coverage tool's jar.
#

if [ "x$HADOOP_HOME" == "x" ]; then
  echo "HADOOP_HOME is unset!"
  exit 1
fi

if [ "x$LIBHDFS_BUILD_DIR" == "x" ]; then
  LIBHDFS_BUILD_DIR=`pwd`/../
fi

if [ "x$HDFS_TEST_CONF_DIR" == "x" ]; then
  HDFS_TEST_CONF_DIR=/tmp
fi

# LIBHDFS_INSTALL_DIR is the directory containing libhdfs.so
LIBHDFS_INSTALL_DIR=$HADOOP_HOME/lib/native/
HDFS_TEST=hdfs_test

HDFS_TEST_JAR=`find $HADOOP_HOME/share/hadoop/hdfs/ \
-name "hadoop-hdfs-*-tests.jar" | head -n 1`

if [ "x$HDFS_TEST_JAR" == "x" ]; then
  echo "HDFS test jar not found! Tried looking in all subdirectories \
of $HADOOP_HOME/share/hadoop/hdfs/"
  exit 1
fi

echo "Found HDFS test jar at $HDFS_TEST_JAR"

# CLASSPATH initially contains $HDFS_TEST_CONF_DIR
CLASSPATH="${HDFS_TEST_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# add Clover jar file needed for code coverage runs
CLASSPATH=${CLASSPATH}:${CLOVER_JAR};

# so that filenames w/ spaces are handled correctly in loops below
IFS=$'\n'

JAR_DIRS="$HADOOP_HOME/share/hadoop/common/lib/
$HADOOP_HOME/share/hadoop/common/
$HADOOP_HOME/share/hadoop/hdfs
$HADOOP_HOME/share/hadoop/hdfs/lib/"

for d in $JAR_DIRS; do 
  for j in $d/*.jar; do
    CLASSPATH=${CLASSPATH}:$j
  done;
done;

# restore ordinary behaviour
unset IFS

findlibjvm () {
javabasedir=$JAVA_HOME
case $OS_NAME in
    mingw* | pw23* )
    lib_jvm_dir=`find $javabasedir -follow \( \
        \( -name client -type d -prune \) -o \
        \( -name "jvm.dll" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    ;;
    aix*)
    lib_jvm_dir=`find $javabasedir \( \
        \( -name client -type d -prune \) -o \
        \( -name "libjvm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    if test -z "$lib_jvm_dir"; then
       lib_jvm_dir=`find $javabasedir \( \
       \( -name client -type d -prune \) -o \
       \( -name "libkaffevm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    fi
    ;;
    *)
    lib_jvm_dir=`find $javabasedir -follow \( \
       \( -name client -type d -prune \) -o \
       \( -name "libjvm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    if test -z "$lib_jvm_dir"; then
       lib_jvm_dir=`find $javabasedir -follow \( \
       \( -name client -type d -prune \) -o \
       \( -name "libkaffevm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    fi
    ;;
  esac
  echo $lib_jvm_dir
}
LIB_JVM_DIR=`findlibjvm`
echo  "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo  LIB_JVM_DIR = $LIB_JVM_DIR
echo  "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
# Put delays to ensure hdfs is up and running and also shuts down 
# after the tests are complete
rm $HDFS_TEST_CONF_DIR/core-site.xml

$HADOOP_HOME/bin/hadoop jar $HDFS_TEST_JAR \
    org.apache.hadoop.test.MiniDFSClusterManager \
    -format -nnport 20300 -writeConfig $HDFS_TEST_CONF_DIR/core-site.xml \
    > /tmp/libhdfs-test-cluster.out 2>&1 & 

MINI_CLUSTER_PID=$!
for i in {1..15}; do
  echo "Waiting for DFS cluster, attempt $i of 15"
  [ -f $HDFS_TEST_CONF_DIR/core-site.xml ] && break;
  sleep 2
done

if [ ! -f $HDFS_TEST_CONF_DIR/core-site.xml ]; then
  echo "Cluster did not come up in 30s"
  kill -9 $MINI_CLUSTER_PID
  exit 1
fi

echo "Cluster up, running tests"
# Disable error checking to make sure we get to cluster cleanup
set +e

CLASSPATH=$CLASSPATH \
LD_PRELOAD="$LIB_JVM_DIR/libjvm.so:$LIBHDFS_INSTALL_DIR/libhdfs.so:" \
$LIBHDFS_BUILD_DIR/$HDFS_TEST

BUILD_STATUS=$?

echo "Tearing cluster down"
kill -9 $MINI_CLUSTER_PID
echo "Exiting with $BUILD_STATUS"
exit $BUILD_STATUS
