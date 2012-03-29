@echo off
setlocal
set path=%HADOOP_BIN_PATH%;%windir%\system32;%windir%

:StartHadoop
start "Apache Hadoop Distribution" hadoop datanode
start "Apache Hadoop Distribution" hadoop tasktracker