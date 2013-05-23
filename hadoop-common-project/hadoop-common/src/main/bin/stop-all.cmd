@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

setlocal enabledelayedexpansion

@rem Stop all hadoop daemons.  Run this on master node.

echo This script is Deprecated. Instead use stop-dfs.cmd and stop-yarn.cmd

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

set DEFAULT_LIBEXEC_DIR=%HADOOP_BIN_PATH%\..\libexec
if not defined HADOOP_LIBEXEC_DIR (
  set HADOOP_LIBEXEC_DIR=%DEFAULT_LIBEXEC_DIR%
)

call %HADOOP_LIBEXEC_DIR%\hadoop-config.cmd %*
if "%1" == "--config" (
  shift
  shift
)

@rem stop hdfs daemons if hdfs is present
if exist %HADOOP_HDFS_HOME%\sbin\stop-dfs.cmd (
  call %HADOOP_HDFS_HOME%\sbin\stop-dfs.cmd --config %HADOOP_CONF_DIR%
)

@rem stop yarn daemons if yarn is present
if exist %HADOOP_YARN_HOME%\sbin\stop-yarn.cmd (
  call %HADOOP_YARN_HOME%\sbin\stop-yarn.cmd --config %HADOOP_CONF_DIR%
)

endlocal
