@echo off
setlocal

set path=%PATH%;%HADOOP_BIN_PATH%

:StartHadoop
start "Apache Hadoop Distribution" hadoop namenode
start "Apache Hadoop Distribution" hadoop jobtracker
