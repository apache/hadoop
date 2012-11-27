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


@rem The Hadoop command script
@rem
@rem Environment Variables
@rem
@rem   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   HADOOP_CLASSPATH Extra Java CLASSPATH entries.
@rem
@rem   HADOOP_USER_CLASSPATH_FIRST      When defined, the HADOOP_CLASSPATH is
@rem                                    added in the beginning of the global
@rem                                    classpath. Can be defined, for example,
@rem                                    by doing
@rem                                    export HADOOP_USER_CLASSPATH_FIRST=true
@rem
@rem   HADOOP_HEAPSIZE  The maximum amount of heap to use, in MB.
@rem                    Default is 1000.
@rem
@rem   HADOOP_OPTS      Extra Java runtime options.
@rem
@rem   HADOOP_NAMENODE_OPTS       These options are added to HADOOP_OPTS
@rem   HADOOP_CLIENT_OPTS         when the respective command is run.
@rem   HADOOP_{COMMAND}_OPTS etc  HADOOP_JT_OPTS applies to JobTracker
@rem                              for e.g.  HADOOP_CLIENT_OPTS applies to
@rem                              more than one command (fs, dfs, fsck,
@rem                              dfsadmin etc)
@rem
@rem   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
@rem
@rem   HADOOP_ROOT_LOGGER The root appender. Default is INFO,console
@rem

if not defined HADOOP_BIN_PATH ( 
  set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
  set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

call :updatepath %HADOOP_BIN_PATH%

:main
  setlocal enabledelayedexpansion

  set hadoop-config-script=%HADOOP_BIN_PATH%\hadoop-config.cmd
  set hadoop-hdfs-script=%HADOOP_BIN_PATH%\hdfs.cmd
  set hadoop-mapred-script=%HADOOP_BIN_PATH%\mapred.cmd

  call %hadoop-config-script% %*
  if "%1" == "--config" (
    shift
    shift
  )

  set hadoop-command=%1
  if not defined hadoop-command (
      goto print_usage
  )

  call :make_command_arguments %*

  set hdfscommands=namenode secondarynamenode datanode fs dfs dfsadmin fsck balancer fetchdt
  for %%i in ( %hdfscommands% ) do (
    if %hadoop-command% == %%i set hdfscommand=true
  )
  if defined hdfscommand (
    if exist %hadoop-hdfs-script% (
      call %hadoop-hdfs-script% %hadoop-command% %hadoop-command-arguments%
      goto :eof
    )
  )

  set mapredcommands=mradmin jobtracker tasktracker pipes job queue historyserver
  for %%i in ( %mapredcommands% ) do (
    if %hadoop-command% == %%i set mapredcommand=true  
  )
  if defined mapredcommand (
    if exist %hadoop-mapred-script% (
      call %hadoop-mapred-script% %hadoop-command% %hadoop-command-arguments%
      goto :eof
    )
  )

  if %hadoop-command% == classpath (
    @echo %CLASSPATH%
    goto :eof
  )
  
  set corecommands=fs version jar distcp daemonlog archive
  for %%i in ( %corecommands% ) do (
    if %hadoop-command% == %%i set corecommand=true  
  )
  if defined corecommand (
    call :%hadoop-command%
  ) else (
    set CLASSPATH=%CLASSPATH%;%CD%
    set CLASS=%hadoop-command%
  )

  set path=%PATH%;%HADOOP_BIN_PATH%
  
  call %JAVA% %JAVA_HEAP_MAX% %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% %hadoop-command-arguments%

  goto :eof

:version 
  set CLASS=org.apache.hadoop.util.VersionInfo
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:jar
  set CLASS=org.apache.hadoop.util.RunJar
  goto :eof

:distcp
  set CLASS=org.apache.hadoop.tools.DistCp
  set CLASSPATH=%CLASSPATH%;%TOOL_PATH%
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:daemonlog
  set CLASS=org.apache.hadoop.log.LogLevel
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:archive
  set CLASS=org.apache.hadoop.tools.HadoopArchives
  set CLASSPATH=%CLASSPATH%;%TOOL_PATH%
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:updatepath
  set path_to_add=%*
  set current_path_comparable=%path%
  set current_path_comparable=%current_path_comparable: =_%
  set current_path_comparable=%current_path_comparable:(=_%
  set current_path_comparable=%current_path_comparable:)=_%
  set path_to_add_comparable=%path_to_add%
  set path_to_add_comparable=%path_to_add_comparable: =_%
  set path_to_add_comparable=%path_to_add_comparable:(=_%
  set path_to_add_comparable=%path_to_add_comparable:)=_%

  for %%i in ( %current_path_comparable% ) do (
    if /i "%%i" == "%path_to_add_comparable%" (
      set path_to_add_exist=true
    )
  )
  set system_path_comparable=
  set path_to_add_comparable=
  if not defined path_to_add_exist path=%path_to_add%;%path%
  set path_to_add=
  goto :eof

@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if "%1" == "--config" (
    shift
    shift
  )
  if [%2] == [] goto :eof
  shift
  set _arguments=
  :MakeCmdArgsLoop 
  if [%1]==[] goto :EndLoop 

  if not defined _arguments (
    set _arguments=%1
  ) else (
    set _arguments=!_arguments! %1
  )
  shift
  goto :MakeCmdArgsLoop 
  :EndLoop 
  set hadoop-command-arguments=%_arguments%
  goto :eof


:print_usage
  @echo Usage: hadoop [--config confdir] COMMAND
  @echo where COMMAND is one of:
  @echo   namenode -format     format the DFS filesystem
  @echo   secondarynamenode    run the DFS secondary namenode
  @echo   namenode             run the DFS namenode
  @echo   datanode             run a DFS datanode
  @echo   dfsadmin             run a DFS admin client
  @echo   mradmin              run a Map-Reduce admin client
  @echo   fsck                 run a DFS filesystem checking utility
  @echo   fs                   run a generic filesystem user client
  @echo   balancer             run a cluster balancing utility
  @echo   fetchdt              fetch a delegation token from the NameNode
  @echo   jobtracker           run the MapReduce job Tracker node
  @echo   pipes                run a Pipes job
  @echo   tasktracker          run a MapReduce task Tracker node
  @echo   historyserver        run job history servers as a standalone daemon
  @echo   job                  manipulate MapReduce jobs
  @echo   queue                get information regarding JobQueues
  @echo   version              print the version
  @echo   jar ^<jar^>            run a jar file
  @echo.
  @echo   distcp ^<srcurl^> ^<desturl^> copy file or directories recursively
  @echo   archive -archiveName NAME ^<src^>* ^<dest^> create a hadoop archive
  @echo   daemonlog            get/set the log level for each daemon
  @echo  or
  @echo   CLASSNAME            run the class named CLASSNAME
  @echo Most commands print help when invoked w/o parameters.

endlocal
