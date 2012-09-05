@echo off
setlocal

set path=%PATH%;%HADOOP_BIN_PATH%

:StartHadoop
start "Apache Hadoop Distribution" hadoop datanode
start "Apache Hadoop Distribution" hadoop tasktracker
