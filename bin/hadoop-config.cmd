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

set HADOOP_HOME=%~dp0
for %%i in (%HADOOP_HOME%.) do (
  set HADOOP_HOME=%%~dpi
)
if "%HADOOP_HOME:~-1%" == "\" (
  set HADOOP_HOME=%HADOOP_HOME:~0,-1%
)

if not exist %HADOOP_HOME%\hadoop-core-*.jar (
  if not exist %HADOOP_HOME%\build\hadoop-core-*.jar (
    @echo +================================================================+
    @echo ^|      Error: HADOOP_HOME is not set correctly                   ^|
    @echo +----------------------------------------------------------------+
    @echo ^| Please set your HADOOP_HOME variable to the absolute path of   ^|
    @echo ^| the directory that contains hadoop-core-VERSION.jar            ^|
    @echo +================================================================+
    exit /b 1
  )
)

set HADOOP_CORE_HOME=%HADOOP_HOME%
set HADOOP_CONF_DIR=%HADOOP_HOME%\conf

@rem
@rem Allow alternate conf dir location.
@rem

if "%1" == "--config" (
  set HADOOP_CONF_DIR=%2
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
  echo        Please update %HADOOP_HOME%\conf\hadoop-env.cmd
  goto :eof
)

set JAVA=%JAVA_HOME%\bin\java
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
set CLASSPATH=%CLASSPATH%;%JAVA_HOME%\lib\tools.jar

@rem
@rem for developers, add Hadoop classes to CLASSPATH
@rem

if exist %HADOOP_CORE_HOME%\build\classes (
  set CLASSPATH=%CLASSPATH%;%HADOOP_CORE_HOME%\build\classes
)

if exist %HADOOP_CORE_HOME%\build\webapps (
  set CLASSPATH=%CLASSPATH%;%HADOOP_CORE_HOME%\build
)

if exist %HADOOP_CORE_HOME%\build\test\classes (
  set CLASSPATH=%CLASSPATH%;%HADOOP_CORE_HOME%\build\test\classes
)

if exist %HADOOP_CORE_HOME%\build\test\core\classes (
  set CLASSPATH=%CLASSPATH%;%HADOOP_CORE_HOME%\build\test\core\classes
)

for %%i in (%HADOOP_CORE_HOME%\build\*.jar) do (
  set CLASSPATH=!CLASSPATH!;%%i
)
for %%i in (%HADOOP_CORE_HOME%\build\ivy\lib\Hadoop\common\*.jar) do (
  set CLASSPATH=!CLASSPATH!;%%i
)

@rem
@rem for releases, add core hadoop jar & webapps to CLASSPATH
@rem

if exist %HADOOP_CORE_HOME%\webapps (
  set CLASSPATH=%CLASSPATH%;%HADOOP_CORE_HOME%
)

for %%i in (%HADOOP_CORE_HOME%\*.jar) do (
  set CLASSPATH=!CLASSPATH!;%%i
)

@rem
@rem add libs to CLASSPATH
@rem

set CLASSPATH=!CLASSPATH!;%HADOOP_CORE_HOME%\lib\*;%HADOOP_CORE_HOME%\lib\jsp-2.1\*

if not defined HADOOP_LOG_DIR (
  set HADOOP_LOG_DIR=%HADOOP_HOME%\logs
)

if not defined HADOOP_LOGFILE (
  set HADOOP_LOGFILE=hadoop.log
)

if not defined HADOOP_ROOT_LOGGER (
  set HADOOP_ROOT_LOGGER=INFO,console,DRFA
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
set JAVA_LIBRARY_PATH=
if exist %HADOOP_CORE_HOME%\build\native (
  set JAVA_LIBRARY_PATH=%HADOOP_CORE_HOME%\build\native\%JAVA_PLATFORM%\lib
)

@rem For the disto case, check the lib\native folder
if exist %HADOOP_CORE_HOME%\lib\native (
  set JAVA_LIBRARY_PATH=%JAVA_LIBRARY_PATH%;%HADOOP_CORE_HOME%\lib\native\%JAVA_PLATFORM%;%HADOOP_CORE_HOME%\lib\native
)

set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.dir=%HADOOP_LOG_DIR%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.log.file=%HADOOP_LOGFILE%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.home.dir=%HADOOP_CORE_HOME%
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.root.logger=%HADOOP_ROOT_LOGGER%

if defined JAVA_LIBRARY_PATH (
  set HADOOP_OPTS=%HADOOP_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
)
set HADOOP_OPTS=%HADOOP_OPTS% -Dhadoop.policy.file=%HADOOP_POLICYFILE%

@rem
@rem add user-specified CLASSPATH last
@rem

if defined HADOOP_CLASSPATH (
  if defined HADOOP_USER_CLASSPATH_FIRST (
    set CLASSPATH=%HADOOP_CLASSPATH%;%CLASSPATH%;
  )
  if not defined HADOOP_USER_CLASSPATH_FIRST (
    set CLASSPATH=%CLASSPATH%;%HADOOP_CLASSPATH%;
  )
)

:eof
