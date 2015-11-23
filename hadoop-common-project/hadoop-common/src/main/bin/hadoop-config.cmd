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

@rem included in all the hadoop scripts with source command
@rem should not be executable directly
@rem also should not be passed any arguments, since we need original %*

if not defined HADOOP_COMMON_DIR (
  set HADOOP_COMMON_DIR=share\hadoop\common
)
if not defined HADOOP_COMMON_LIB_JARS_DIR (
  set HADOOP_COMMON_LIB_JARS_DIR=share\hadoop\common\lib
)
if not defined HADOOP_COMMON_LIB_NATIVE_DIR (
  set HADOOP_COMMON_LIB_NATIVE_DIR=lib\native
)
if not defined HDFS_DIR (
  set HDFS_DIR=share\hadoop\hdfs
)
if not defined HDFS_LIB_JARS_DIR (
  set HDFS_LIB_JARS_DIR=share\hadoop\hdfs\lib
)
if not defined YARN_DIR (
  set YARN_DIR=share\hadoop\yarn
)
if not defined YARN_LIB_JARS_DIR (
  set YARN_LIB_JARS_DIR=share\hadoop\yarn\lib
)
if not defined MAPRED_DIR (
  set MAPRED_DIR=share\hadoop\mapreduce
)
if not defined MAPRED_LIB_JARS_DIR (
  set MAPRED_LIB_JARS_DIR=share\hadoop\mapreduce\lib
)

@rem the root of the Hadoop installation
set HADOOP_HOME=%~dp0
for %%i in (%HADOOP_HOME%.) do (
  set HADOOP_HOME=%%~dpi
)
if "%HADOOP_HOME:~-1%" == "\" (
  set HADOOP_HOME=%HADOOP_HOME:~0,-1%
)

if not exist %HADOOP_HOME%\share\hadoop\common\hadoop-common-*.jar (
    @echo +================================================================+
    @echo ^|      Error: HADOOP_HOME is not set correctly                   ^|
    @echo +----------------------------------------------------------------+
    @echo ^| Please set your HADOOP_HOME variable to the absolute path of   ^|
    @echo ^| the directory that contains the hadoop distribution            ^|
    @echo +================================================================+
    exit /b 1
)

if not defined HADOOP_CONF_DIR (
  set HADOOP_CONF_DIR=%HADOOP_HOME%\etc\hadoop
)

@rem
@rem Allow alternate conf dir location.
@rem

if "%1" == "--config" (
  set HADOOP_CONF_DIR=%2
  shift
  shift
)

@rem
@rem Set log level. Default to INFO.
@rem

if "%1" == "--loglevel" (
  set HADOOP_LOGLEVEL=%2
  shift
  shift
)

@rem
@rem check to see it is specified whether to use the slaves or the
@rem masters file
@rem

if "%1" == "--hosts" (
  set HADOOP_SLAVES=%HADOOP_CONF_DIR%\%2
  shift
  shift
)

if exist %HADOOP_CONF_DIR%\hadoop-env.cmd (
  call %HADOOP_CONF_DIR%\hadoop-env.cmd
)

@rem
@rem setup java environment variables
@rem

if not defined JAVA_HOME (
  echo Error: JAVA_HOME is not set.
  goto :eof
)

if not exist %JAVA_HOME%\bin\java.exe (
  echo Error: JAVA_HOME is incorrectly set.
  echo        Please update %HADOOP_CONF_DIR%\hadoop-env.cmd
  goto :eof
)

set JAVA=%JAVA_HOME%\bin\java
@rem some Java parameters
set JAVA_HEAP_MAX=-Xmx1000m

@rem
@rem check envvars which might override default args
@rem

if defined HADOOP_HEAPSIZE (
  set JAVA_HEAP_MAX=-Xmx%HADOOP_HEAPSIZE%m
)

@rem
@rem CLASSPATH initially contains %HADOOP_CONF_DIR%
@rem

set CLASSPATH=%HADOOP_CONF_DIR%

if not defined HADOOP_COMMON_HOME (
  if exist %HADOOP_HOME%\share\hadoop\common (
    set HADOOP_COMMON_HOME=%HADOOP_HOME%
  )
)

@rem
@rem for releases, add core hadoop jar & webapps to CLASSPATH
@rem

if exist %HADOOP_COMMON_HOME%\%HADOOP_COMMON_DIR%\webapps (
  set CLASSPATH=!CLASSPATH!;%HADOOP_COMMON_HOME%\%HADOOP_COMMON_DIR%
)

if exist %HADOOP_COMMON_HOME%\%HADOOP_COMMON_LIB_JARS_DIR% (
  set CLASSPATH=!CLASSPATH!;%HADOOP_COMMON_HOME%\%HADOOP_COMMON_LIB_JARS_DIR%\*
)

set CLASSPATH=!CLASSPATH!;%HADOOP_COMMON_HOME%\%HADOOP_COMMON_DIR%\*

@rem
@rem default log directory % file
@rem

if not defined HADOOP_LOG_DIR (
  set HADOOP_LOG_DIR=%HADOOP_HOME%\logs
)

if not defined HADOOP_LOGFILE (
  set HADOOP_LOGFILE=hadoop.log
)

if not defined HADOOP_LOGLEVEL (
  set HADOOP_LOGLEVEL=INFO
)

if not defined HADOOP_ROOT_LOGGER (
  set HADOOP_ROOT_LOGGER=%HADOOP_LOGLEVEL%,console
)

@rem
@rem default policy file for service-level authorization
@rem

if not defined HADOOP_POLICYFILE (
  set HADOOP_POLICYFILE=hadoop-policy.xml
)

@rem
@rem Determine the JAVA_PLATFORM
@rem

for /f "delims=" %%A in ('%JAVA% -Xmx32m %HADOOP_JAVA_PLATFORM_OPTS% -classpath "%CLASSPATH%" org.apache.hadoop.util.PlatformName') do set JAVA_PLATFORM=%%A
@rem replace space with underscore
set JAVA_PLATFORM=%JAVA_PLATFORM: =_%

@rem
@rem setup 'java.library.path' for native hadoop code if necessary
@rem

@rem Check if we're running hadoop directly from the build
if exist %HADOOP_COMMON_HOME%\target\bin (
  if defined JAVA_LIBRARY_PATH (
    set JAVA_LIBRARY_PATH=%JAVA_LIBRARY_PATH%;%HADOOP_COMMON_HOME%\target\bin
   ) else (
    set JAVA_LIBRARY_PATH=%HADOOP_COMMON_HOME%\target\bin
   )
)

@rem For the distro case, check the bin folder
if exist %HADOOP_COMMON_HOME%\bin (
  if defined JAVA_LIBRARY_PATH (
    set JAVA_LIBRARY_PATH=%JAVA_LIBRARY_PATH%;%HADOOP_COMMON_HOME%\bin
  ) else (
    set JAVA_LIBRARY_PATH=%HADOOP_COMMON_HOME%\bin
  )
)

@rem
@rem setup a default TOOL_PATH
@rem
set TOOL_PATH=%HADOOP_HOME%\share\hadoop\tools\lib\*

set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.dir=%HADOOP_LOG_DIR%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.file=%HADOOP_LOGFILE%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.home.dir=%HADOOP_HOME%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.id.str=%HADOOP_IDENT_STRING%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.root.logger=%HADOOP_ROOT_LOGGER%

if defined JAVA_LIBRARY_PATH (
  set HADOOP_OPTS=%HADOOP_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
)
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.policy.file=%HADOOP_POLICYFILE%

@rem
@rem Disable ipv6 as it can cause issues
@rem

set HADOOP_OPTS=%HADOOP_OPTS% -Djava.net.preferIPv4Stack=true

@rem
@rem put hdfs in classpath if present
@rem

if not defined HADOOP_HDFS_HOME (
  if exist %HADOOP_HOME%\%HDFS_DIR% (
    set HADOOP_HDFS_HOME=%HADOOP_HOME%
  )
)

if exist %HADOOP_HDFS_HOME%\%HDFS_DIR%\webapps (
  set CLASSPATH=!CLASSPATH!;%HADOOP_HDFS_HOME%\%HDFS_DIR%
)

if exist %HADOOP_HDFS_HOME%\%HDFS_LIB_JARS_DIR% (
  set CLASSPATH=!CLASSPATH!;%HADOOP_HDFS_HOME%\%HDFS_LIB_JARS_DIR%\*
)

set CLASSPATH=!CLASSPATH!;%HADOOP_HDFS_HOME%\%HDFS_DIR%\*

@rem
@rem put yarn in classpath if present
@rem

if not defined HADOOP_YARN_HOME (
  if exist %HADOOP_HOME%\%YARN_DIR% (
    set HADOOP_YARN_HOME=%HADOOP_HOME%
  )
)

if exist %HADOOP_YARN_HOME%\%YARN_DIR%\webapps (
  set CLASSPATH=!CLASSPATH!;%HADOOP_YARN_HOME%\%YARN_DIR%
)

if exist %HADOOP_YARN_HOME%\%YARN_LIB_JARS_DIR% (
  set CLASSPATH=!CLASSPATH!;%HADOOP_YARN_HOME%\%YARN_LIB_JARS_DIR%\*
)

set CLASSPATH=!CLASSPATH!;%HADOOP_YARN_HOME%\%YARN_DIR%\*

@rem
@rem put mapred in classpath if present AND different from YARN
@rem

if not defined HADOOP_MAPRED_HOME (
  if exist %HADOOP_HOME%\%MAPRED_DIR% (
    set HADOOP_MAPRED_HOME=%HADOOP_HOME%
  )
)

if not "%HADOOP_MAPRED_HOME%\%MAPRED_DIR%" == "%HADOOP_YARN_HOME%\%YARN_DIR%" (

  if exist %HADOOP_MAPRED_HOME%\%MAPRED_DIR%\webapps (
    set CLASSPATH=!CLASSPATH!;%HADOOP_MAPRED_HOME%\%MAPRED_DIR%
  )

  if exist %HADOOP_MAPRED_HOME%\%MAPRED_LIB_JARS_DIR% (
    set CLASSPATH=!CLASSPATH!;%HADOOP_MAPRED_HOME%\%MAPRED_LIB_JARS_DIR%\*
  )

  set CLASSPATH=!CLASSPATH!;%HADOOP_MAPRED_HOME%\%MAPRED_DIR%\*
)

@rem
@rem add user-specified CLASSPATH last
@rem

if defined HADOOP_CLASSPATH (
  if not defined HADOOP_USE_CLIENT_CLASSLOADER (
    if defined HADOOP_USER_CLASSPATH_FIRST (
      set CLASSPATH=%HADOOP_CLASSPATH%;%CLASSPATH%;
    ) else (
      set CLASSPATH=%CLASSPATH%;%HADOOP_CLASSPATH%;
    )
  )
)

:eof
