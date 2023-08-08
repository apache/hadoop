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
@rem   JAVA_HOME            The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   YARN_CLASSPATH       Extra Java CLASSPATH entries.
@rem
@rem   YARN_HEAPSIZE        The maximum amount of heap to use, in MB.
@rem                        Default is 1000.
@rem
@rem   YARN_{COMMAND}_HEAPSIZE overrides YARN_HEAPSIZE for a given command
@rem                           eg YARN_NODEMANAGER_HEAPSIZE sets the heap
@rem                           size for the NodeManager.  If you set the
@rem                           heap size in YARN_{COMMAND}_OPTS or YARN_OPTS
@rem                           they take precedence.
@rem
@rem   YARN_OPTS            Extra Java runtime options.
@rem
@rem   YARN_CLIENT_OPTS     when the respective command is run.
@rem   YARN_{COMMAND}_OPTS etc  YARN_NODEMANAGER_OPTS applies to NodeManager
@rem                              for e.g.  YARN_CLIENT_OPTS applies to
@rem                              more than one command (fs, dfs, fsck,
@rem                              dfsadmin etc)
@rem
@rem   YARN_CONF_DIR        Alternate conf dir. Default is ${HADOOP_YARN_HOME}/conf.
@rem
@rem   YARN_ROOT_LOGGER     The root appender. Default is INFO,console
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

call %DEFAULT_LIBEXEC_DIR%\yarn-config.cmd %*
if "%1" == "--config" (
  shift
  shift
)
if "%1" == "--loglevel" (
  shift
  shift
)

:main
  if exist %YARN_CONF_DIR%\yarn-env.cmd (
    call %YARN_CONF_DIR%\yarn-env.cmd
  )

  set yarn-command=%1
  call :make_command_arguments %*

  if not defined yarn-command (
      goto print_usage
  )

  @rem JAVA and JAVA_HEAP_MAX and set in hadoop-config.cmd

  if defined YARN_HEAPSIZE (
    @rem echo run with Java heapsize %YARN_HEAPSIZE%
    set JAVA_HEAP_MAX=-Xmx%YARN_HEAPSIZE%m
  )

  @rem CLASSPATH initially contains HADOOP_CONF_DIR & YARN_CONF_DIR
  if not defined HADOOP_CONF_DIR (
    echo No HADOOP_CONF_DIR set. 
    echo Please specify it either in yarn-env.cmd or in the environment.
    goto :eof
  )

  set CLASSPATH=%HADOOP_CONF_DIR%;%YARN_CONF_DIR%;%CLASSPATH%

  @rem for developers, add Hadoop classes to CLASSPATH
  if exist %HADOOP_YARN_HOME%\yarn-api\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-api\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-common\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-common\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-mapreduce\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-mapreduce\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-master-worker\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-master-worker\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-nodemanager\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-nodemanager\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-common\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-common\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-resourcemanager\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-resourcemanager\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-applicationhistoryservice\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-applicationhistoryservice\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-router\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-router\target\classes
  )

  if exist %HADOOP_YARN_HOME%\yarn-server\yarn-server-globalpolicygenerator\target\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\yarn-server\yarn-server-globalpolicygenerator\target\classes
  )

  if exist %HADOOP_YARN_HOME%\build\test\classes (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\build\test\classes
  )

  if exist %HADOOP_YARN_HOME%\build\tools (
    set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\build\tools
  )

  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_DIR%\*
  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_LIB_JARS_DIR%\*

  if %yarn-command% == classpath (
    if not defined yarn-command-arguments (
      @rem No need to bother starting up a JVM for this simple case.
      @echo %CLASSPATH%
      exit /b
    )
  )

  set yarncommands=resourcemanager nodemanager proxyserver rmadmin version jar ^
     application applicationattempt container node queue logs daemonlog historyserver ^
     timelineserver timelinereader router globalpolicygenerator classpath
  for %%i in ( %yarncommands% ) do (
    if %yarn-command% == %%i set yarncommand=true
  )
  if defined yarncommand (
    call :%yarn-command%
  ) else (
    set CLASSPATH=%CLASSPATH%;%CD%
    set CLASS=%yarn-command%
  )

  if defined JAVA_LIBRARY_PATH (
    set YARN_OPTS=%YARN_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
  )

  set java_arguments=%JAVA_HEAP_MAX% %YARN_OPTS% -classpath %CLASSPATH% %CLASS% %yarn-command-arguments%
  call %JAVA% %java_arguments%

goto :eof

:classpath
  set CLASS=org.apache.hadoop.util.Classpath
  goto :eof

:rmadmin
  set CLASS=org.apache.hadoop.yarn.client.cli.RMAdminCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:application
  set CLASS=org.apache.hadoop.yarn.client.cli.ApplicationCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  set yarn-command-arguments=%yarn-command% %yarn-command-arguments%
  goto :eof

:applicationattempt
  set CLASS=org.apache.hadoop.yarn.client.cli.ApplicationCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  set yarn-command-arguments=%yarn-command% %yarn-command-arguments%
  goto :eof

:cluster
  set CLASS=org.apache.hadoop.yarn.client.cli.ClusterCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:container
  set CLASS=org.apache.hadoop.yarn.client.cli.ApplicationCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  set yarn-command-arguments=%yarn-command% %yarn-command-arguments%
  goto :eof  

:node
  set CLASS=org.apache.hadoop.yarn.client.cli.NodeCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:queue
  set CLASS=org.apache.hadoop.yarn.client.cli.QueueCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:resourcemanager
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\rm-config\log4j.properties
  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_DIR%\timelineservice\*
  set CLASSPATH=%HADOOP_YARN_HOME%\%YARN_DIR%\timelineservice\lib\*;%CLASSPATH%
  set CLASS=org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
  set YARN_OPTS=%YARN_OPTS% %YARN_RESOURCEMANAGER_OPTS%
  if defined YARN_RESOURCEMANAGER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_RESOURCEMANAGER_HEAPSIZE%m
  )
  goto :eof

:historyserver
  @echo DEPRECATED: Use of this command to start the timeline server is deprecated. 1>&2
  @echo Instead use the timelineserver command for it. 1>&2
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\ahs-config\log4j.properties
  set CLASS=org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer
  set YARN_OPTS=%YARN_OPTS% %HADOOP_HISTORYSERVER_OPTS%
  if defined YARN_HISTORYSERVER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_HISTORYSERVER_HEAPSIZE%m
  )
  goto :eof

:timelineserver
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\timelineserver-config\log4j.properties
  set CLASS=org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer
  set YARN_OPTS=%YARN_OPTS% %HADOOP_TIMELINESERVER_OPTS%
  if defined YARN_TIMELINESERVER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_TIMELINESERVER_HEAPSIZE%m
  )
  goto :eof

:timelinereader
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\timelineserver-config\log4j.properties
  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_DIR%\timelineservice\*
  set CLASSPATH=%HADOOP_YARN_HOME%\%YARN_DIR%\timelineservice\lib\*;%CLASSPATH%
  set CLASS=org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderServer
  set YARN_OPTS=%YARN_OPTS% %YARN_TIMELINEREADER_OPTS%
  goto :eof

:router
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\router-config\log4j.properties
  set CLASS=org.apache.hadoop.yarn.server.router.Router
  set YARN_OPTS=%YARN_OPTS% %HADOOP_ROUTER_OPTS%
  if defined YARN_ROUTER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_ROUTER_HEAPSIZE%m
  )
  goto :eof

:globalpolicygenerator
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\globalpolicygenerator-config\log4j.properties
  set CLASS=org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator
  set YARN_OPTS=%YARN_OPTS% %YARN_GLOBALPOLICYGENERATOR_OPTS%
  goto :eof

:routeradmin
  set CLASS=org.apache.hadoop.yarn.client.cli.RouterCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:nodemanager
  set CLASSPATH=%CLASSPATH%;%YARN_CONF_DIR%\nm-config\log4j.properties
  set CLASSPATH=%CLASSPATH%;%HADOOP_YARN_HOME%\%YARN_DIR%\timelineservice\*
  set CLASSPATH=%HADOOP_YARN_HOME%\%YARN_DIR%\timelineservice\lib\*;%CLASSPATH%
  set CLASS=org.apache.hadoop.yarn.server.nodemanager.NodeManager
  set YARN_OPTS=%YARN_OPTS% -server %HADOOP_NODEMANAGER_OPTS%
  if defined YARN_NODEMANAGER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_NODEMANAGER_HEAPSIZE%m
  )
  goto :eof

:proxyserver
  set CLASS=org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer
  set YARN_OPTS=%YARN_OPTS% %HADOOP_PROXYSERVER_OPTS%
  if defined YARN_PROXYSERVER_HEAPSIZE (
    set JAVA_HEAP_MAX=-Xmx%YARN_PROXYSERVER_HEAPSIZE%m
  )
  goto :eof

:version
  set CLASS=org.apache.hadoop.util.VersionInfo
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:jar
  set CLASS=org.apache.hadoop.util.RunJar
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:logs
  set CLASS=org.apache.hadoop.yarn.client.cli.LogsCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:daemonlog
  set CLASS=org.apache.hadoop.log.LogLevel
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

:schedulerconf
  set CLASS=org.apache.hadoop.yarn.client.cli.SchedConfCLI
  set YARN_OPTS=%YARN_OPTS% %YARN_CLIENT_OPTS%
  goto :eof

@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if "%1" == "--config" (
    shift
    shift
  )
  if "%1" == "--loglevel" (
    shift
    shift
  )
  if [%2] == [] goto :eof
  shift
  set _yarnarguments=
  :MakeCmdArgsLoop 
  if [%1]==[] goto :EndLoop 

  if not defined _yarnarguments (
    set _yarnarguments=%1
  ) else (
    set _yarnarguments=!_yarnarguments! %1
  )
  shift
  goto :MakeCmdArgsLoop 
  :EndLoop 
  set yarn-command-arguments=%_yarnarguments%
  goto :eof

:print_usage
  @echo Usage: yarn [--config confdir] [--loglevel loglevel] COMMAND
  @echo        where COMMAND is one of:
  @echo   resourcemanager      run the ResourceManager
  @echo   nodemanager          run a nodemanager on each slave
  @echo   router               run the Router daemon
  @echo   globalpolicygenerator  run the Global Policy Generator
  @echo   routeradmin          router admin tools
  @echo   timelineserver       run the timeline server
  @echo   timelinereader       run the timeline reader server
  @echo   rmadmin              admin tools
  @echo   version              print the version
  @echo   jar ^<jar^>          run a jar file
  @echo   application          prints application(s) report/kill application
  @echo   applicationattempt   prints applicationattempt(s) report
  @echo   cluster              prints cluster information
  @echo   container            prints container(s) report
  @echo   node                 prints node report(s)
  @echo   queue                prints queue information
  @echo   logs                 dump container logs
  @echo   schedulerconf        updates scheduler configuration
  @echo   classpath            prints the class path needed to get the
  @echo                        Hadoop jar and the required libraries
  @echo   daemonlog            get/set the log level for each daemon
  @echo   or
  @echo   CLASSNAME            run the class named CLASSNAME
  @echo Most commands print help when invoked w/o parameters.

endlocal
