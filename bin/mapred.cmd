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

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

@rem if we're being called by --service we need to use %2 otherwise use %1 
@rem for the command line so we log to the right file 
if "%2" == "" (
  set HADOOP_LOGFILE=hadoop-%1-%computername%.log
) else (
  set HADOOP_LOGFILE=hadoop-%2-%computername%.log
)

set mapred-config-script=%HADOOP_BIN_PATH%\hadoop-config.cmd
call %mapred-config-script% %*

:main

  if "%1" == "--config" (
    set config_override=true
    shift
    set HADOOP_CONF_DIR=%2
    shift

    if exist %HADOOP_CONF_DIR%\hadoop-env.cmd (
      call %HADOOP_CONF_DIR%\hadoop-env.cmd
    )
  )

  if "%1" == "--service" (
     set service_entry=true
	 set HADOOP_ROOT_LOGGER=INFO,DRFA
	 shift
  )

  set mapred-command=%1
  call :make_command_arguments %*

  if not defined mapred-command (
      goto print_usage
  )

  call :%mapred-command% %mapred-command-arguments%
  set java_arguments=%HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% %mapred-command-arguments%
  if defined service_entry (
    call :makeServiceXml %java_arguments%
  ) else (
    call %JAVA% %java_arguments%
  )

goto :eof

:historyserver
  set CLASS=org.apache.hadoop.mapred.JobHistoryServer
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_JOB_HISTORYSERVER_OPTS%
  goto :eof

:mradmin
  set CLASS=org.apache.hadoop.mapred.tools.MRAdmin
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:jobtracker
  set CLASS=org.apache.hadoop.mapred.JobTracker
  set HADOOP_OPTS=-server -Xmx4096m %HADOOP_OPTS% %HADOOP_JOBTRACKER_OPTS%
  goto :eof

:tasktracker
  set CLASS=org.apache.hadoop.mapred.TaskTracker
  set HADOOP_OPTS=-Xmx512m %HADOOP_OPTS% %HADOOP_TASKTRACKER_OPTS%
  goto :eof

:job
  set CLASS=org.apache.hadoop.mapred.JobClient
  goto :eof

:queue
  set CLASS=org.apache.hadoop.mapred.JobQueueClient
  goto :eof

:pipe
  set CLASS=org.apache.hadoop.mapred.pipes.Submitter
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:sampler
  set CLASS=org.apache.hadoop.mapred.lib.InputSampler
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:makeServiceXml
  set arguments=%*
  @echo ^<service^>
  @echo   ^<id^>%mapred-command%^</id^>
  @echo   ^<name^>%mapred-command%^</name^>
  @echo   ^<description^>This service runs Isotope %mapred-command%^</description^>
  @echo   ^<executable^>%JAVA%^</executable^>
  @echo   ^<arguments^>%arguments%^</arguments^>
  @echo ^</service^>
  goto :eof

@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if [%2] == [] goto :eof
  set _count=0
  set _mapredarguments=
  if defined service_entry (set _shift=2) else (set _shift=1)
  if defined config_override (set /a _shift=!_shift! + 2)
  :SHIFTLOOP
  set /a _count=!_count!+1
  if !_count! GTR %_shift% goto :MakeCmdArgsLoop
  shift
  goto :SHIFTLOOP

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

:print_usage
  @echo Usage: mapred [--config confdir] COMMAND
  @echo        where COMMAND is one of:
  @echo   mradmin              run a Map-Reduce admin client
  @echo   jobtracker           run the MapReduce job Tracker node
  @echo   tasktracker          run a MapReduce task Tracker node
  @echo   pipes                run a Pipes job
  @echo   job                  manipulate MapReduce jobs
  @echo   queue                get information regarding JobQueues
  @echo   historyserver        run job history servers as a standalone daemon
  @echo.
  @echo Most commands print help when invoked w/o parameters.

endlocal
