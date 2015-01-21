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

@rem The Hadoop mapred command script

setlocal enabledelayedexpansion

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~`%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

set DEFAULT_LIBEXEC_DIR=%HADOOP_BIN_PATH%\..\libexec
if not defined HADOOP_LIBEXEC_DIR (
  set HADOOP_LIBEXEC_DIR=%DEFAULT_LIBEXEC_DIR%
)

call %DEFAULT_LIBEXEC_DIR%\mapred-config.cmd %*
if "%1" == "--config" (
  shift
  shift
)

if "%1" == "--loglevel" (
  shift
  shift
)

:main
  if exist %MAPRED_CONF_DIR%\mapred-env.cmd (
    call %MAPRED_CONF_DIR%\mapred-env.cmd
  )
  set mapred-command=%1
  call :make_command_arguments %*

  if not defined mapred-command (
    goto print_usage
  )

  @rem JAVA and JAVA_HEAP_MAX are set in hadoop-confg.cmd

  if defined MAPRED_HEAPSIZE (
    @rem echo run with Java heapsize %MAPRED_HEAPSIZE%
    set JAVA_HEAP_SIZE=-Xmx%MAPRED_HEAPSIZE%m
  )

  @rem CLASSPATH initially contains HADOOP_CONF_DIR and MAPRED_CONF_DIR
  if not defined HADOOP_CONF_DIR (
    echo NO HADOOP_CONF_DIR set.
    echo Please specify it either in mapred-env.cmd or in the environment.
    goto :eof
  )

  set CLASSPATH=%HADOOP_CONF_DIR%;%MAPRED_CONF_DIR%;%CLASSPATH%

  @rem for developers, add Hadoop classes to CLASSPATH
  if exist %HADOOP_MAPRED_HOME%\build\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_MAPRED_HOME%\build\classes
  )

  if exist %HADOOP_MAPRED_HOME%\build\webapps (
    set CLASSPATH=%CLASSPATH%;%HADOOP_MAPRED_HOME%\build
  )

  if exist %HADOOP_MAPRED_HOME%\build\test\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_MAPRED_HOME%\build\test\classes
  )

  if exist %HADOOP_MAPRED_HOME%\build\tools (
    set CLASSPATH=%CLASSPATH%;%HADOOP_MAPRED_HOME%\build\tools
  )

  @rem Need YARN jars also
  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_DIR%\*

  @rem add libs to CLASSPATH
  set CLASSPATH=%CLASSPATH%;%HADOOP_MAPRED_HOME%\%MAPRED_LIB_JARS_DIR%\*

  @rem add modules to CLASSPATH
  set CLASSPATH=%CLASSPATH%;%HADOOP_MAPRED_HOME%\modules\*

  if %mapred-command% == classpath (
    if not defined mapred-command-arguments (
      @rem No need to bother starting up a JVM for this simple case.
      @echo %CLASSPATH%
      exit /b
    )
  )

  call :%mapred-command% %mapred-command-arguments%
  set java_arguments=%JAVA_HEAP_MAX% %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% %mapred-command-arguments%
  call %JAVA% %java_arguments%

goto :eof


:classpath
  set CLASS=org.apache.hadoop.util.Classpath
  goto :eof

:job
  set CLASS=org.apache.hadoop.mapred.JobClient
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:queue
  set CLASS=org.apache.hadoop.mapred.JobQueueClient
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:sampler
  set CLASS=org.apache.hadoop.mapred.lib.InputSampler
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:historyserver
  set CLASS=org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer
  set HADOOP_OPTS=%HADOOP_OPTS% -Dmapred.jobsummary.logger=%HADOOP_JHS_LOGGER% %HADOOP_JOB_HISTORYSERVER_OPTS%
  if defined HADOOP_JOB_HISTORYSERVER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%HADOOP_JOB_HISTORYSERVER_HEAPSIZE%m
  )
  goto :eof

:distcp
  set CLASS=org.apache.hadoop.tools.DistCp
  set CLASSPATH=%CLASSPATH%;%TOO_PATH%
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:archive
  set CLASS=org.apache.hadop.tools.HadoopArchives
  set CLASSPATH=%CLASSPATH%;%TOO_PATH%
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%

:hsadmin
  set CLASS=org.apache.hadoop.mapreduce.v2.hs.client.HSAdmin
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%

:pipes
  goto not_supported

:mradmin
  goto not_supported

:jobtracker
  goto not_supported

:tasktracker
  goto not_supported

:groups
  goto not_supported


@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if [%2] == [] goto :eof
  if "%1" == "--config" (
    shift
    shift
  )
  if "%1" == "--loglevel" (
    shift
    shift
  )
  shift
  set _mapredarguments=
  :MakeCmdArgsLoop 
  if [%1]==[] goto :EndLoop 

  if not defined _mapredarguments (
    set _mapredarguments=%1
  ) else (
    set _mapredarguments=!_mapredarguments! %1
  )
  shift
  goto :MakeCmdArgsLoop 
  :EndLoop 
  set mapred-command-arguments=%_mapredarguments%
  goto :eof

:not_supported
  @echo Sorry, the %COMMAND% command is no longer supported.
  @echo You may find similar functionality with the "yarn" shell command.
  goto print_usage

:print_usage
  @echo Usage: mapred [--config confdir] [--loglevel loglevel] COMMAND
  @echo        where COMMAND is one of:
  @echo   job                  manipulate MapReduce jobs
  @echo   queue                get information regarding JobQueues
  @echo   classpath            prints the class path needed for running
  @echo                        mapreduce subcommands
  @echo   historyserver        run job history servers as a standalone daemon
  @echo   distcp ^<srcurl^> ^<desturl^> copy file or directories recursively
  @echo   archive -archiveName NAME -p ^<parent path^> ^<src^>* ^<dest^> create a hadoop archive
  @echo   hsadmin              job history server admin interface
  @echo 
  @echo Most commands print help when invoked w/o parameters.

endlocal
