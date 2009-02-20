#!/bin/bash


#leave all jars in build
HADOOP_DIR=hadoop-0.18.0-mac01
TARFILE=hadoop18.tar
rm -r hadoop-0.18.0-mac01
mkdir $HADOOP_DIR
mkdir $HADOOP_DIR/bin $HADOOP_DIR/conf $HADOOP_DIR/lib $HADOOP_DIR/chukwa
cp ../build/chukwa-hadoop-0.0.1-client.jar $HADOOP_DIR/lib
cp log4j.properties.templ $HADOOP_DIR/conf/log4j.properties
tar xf $TARFILE $HADOOP_DIR/bin/hadoop
patch $HADOOP_DIR/bin/hadoop < patchhadoop.patch
svn export ../bin $HADOOP_DIR/chukwa/bin
cp ../bin/VERSION $HADOOP_DIR/chukwa/bin
svn export ../conf $HADOOP_DIR/chukwa/conf
svn export ../lib $HADOOP_DIR/chukwa/lib
cp ../hadoopjars/hadoop-0.18.0-core.jar $HADOOP_DIR/chukwa/lib
cp ../build/*.jar $HADOOP_DIR/chukwa
mkdir $HADOOP_DIR/chukwa/var; mkdir $HADOOP_DIR/chukwa/var/run
cp new-chukwa-conf/* $HADOOP_DIR/chukwa/conf
# Do something with chukwa-conf 
tar uvf $TARFILE $HADOOP_DIR