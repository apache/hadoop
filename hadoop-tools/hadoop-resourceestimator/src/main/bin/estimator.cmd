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
@rem

setlocal enabledelayedexpansion

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

:main
  @rem CLASSPATH initially contains HADOOP_CONF_DIR
    if not defined HADOOP_CONF_DIR (
      echo No HADOOP_CONF_DIR set.
      echo Please specify it.
      goto :eof
    )

  set CLASSPATH=%HADOOP_CONF_DIR%;%CLASSPATH%
goto :eof

:classpath
  set CLASS=org.apache.hadoop.util.Classpath
  goto :eof

:resourceestimator
  set CLASS=org.apache.hadoop.resourceestimator.service.ResourceEstimatorServer
  goto :eof

endlocal
