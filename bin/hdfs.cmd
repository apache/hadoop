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

set HADOOP_LOGFILE=hadoop-%1-%computername%.log
set hdfs-config-script=%HADOOP_BIN_PATH%\hadoop-config.cmd
call %hdfs-config-script% %*

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

  set hdfs-command=%1
  call :make_command_arguments %*

  if not defined hdfs-command (
      goto print_usage
  )

  call :%hdfs-command% %hdfs-command-arguments%
  set java_arguments=-server %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% %hdfs-command-arguments%
  if defined service_entry (
    call :makeServiceXml %java_arguments%
  ) else (
    call %JAVA% %java_arguments%
  )

goto :eof

:namenode
  set CLASS=org.apache.hadoop.hdfs.server.namenode.NameNode
  set HADOOP_OPTS=-Xmx4096m %HADOOP_OPTS% %HADOOP_NAMENODE_OPTS%
  goto :eof

:secondarynamenode
  set CLASS=org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode
  set HADOOP_OPTS=-Xmx1024m %HADOOP_OPTS% %HADOOP_SECONDARYNAMENODE_OPTS%
  goto :eof

:datanode
  set CLASS=org.apache.hadoop.hdfs.server.datanode.DataNode
  set HADOOP_OPTS=-Xms1024m -Xmx2048m %HADOOP_OPTS% %HADOOP_DATANODE_OPTS%
  goto :eof

:fs
:dfs
  set CLASS=org.apache.hadoop.fs.FsShell
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:dfsadmin
  set CLASS=org.apache.hadoop.hdfs.tools.DFSAdmin
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:fsck
  set CLASS=org.apache.hadoop.hdfs.tools.DFSck
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:balancer
  set CLASS=org.apache.hadoop.hdfs.server.balancer.Balancer
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_BALANCER_OPTS%
  goto :eof

:jmxget
  set CLASS=org.apache.hadoop.hdfs.tools.JMXGet
  goto :eof

:oiv
  set CLASS=org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer
  goto :eof

:fetchdt
  set CLASS=org.apache.hadoop.hdfs.tools.DelegationTokenFetcher
  goto :eof

:makeServiceXml
  set arguments=%*
  @echo ^<service^>
  @echo   ^<id^>%hdfs-command%^</id^>
  @echo   ^<name^>%hdfs-command%^</name^>
  @echo   ^<description^>This service runs Isotope %hdfs-command%^</description^>
  @echo   ^<executable^>%JAVA%^</executable^>
  @echo   ^<arguments^>%arguments%^</arguments^>
  @echo ^</service^>
  goto :eof

@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if "%2" == "" goto :eof
  set _count=0
  set _hdfsarguments=
  if defined service_entry (set _shift=2) else (set _shift=1)
  if defined config_override (set /a _shift=!_shift! + 2)
  :SHIFTLOOP
  set /a _count=!_count!+1
  if !_count! GTR %_shift% goto :MakeCmdArgsLoop
  shift
  goto :SHIFTLOOP

  :MakeCmdArgsLoop
  if [%1]==[] goto :EndLoop 

  if not defined _hdfsarguments (
    set _hdfsarguments=%1
  ) else (
    set _hdfsarguments=!_hdfsarguments! %1
  )
  shift
  goto :MakeCmdArgsLoop 
  :EndLoop 

  set hdfs-command-arguments=%_hdfsarguments%
  goto :eof

:print_usage
  @echo Usage: hdfs [--config confdir] COMMAND
  @echo        where COMMAND is one of:
  @echo   namenode -format     format the DFS filesystem
  @echo   secondarynamenode    run the DFS secondary namenode
  @echo   namenode             run the DFS namenode
  @echo   datanode             run a DFS datanode
  @echo   dfsadmin             run a DFS admin client
  @echo   fsck                 run a DFS filesystem checking utility
  @echo   balancer             run a cluster balancing utility
  @echo   jmxget               get JMX exported values from NameNode or DataNode.
  @echo   oiv                  apply the offline fsimage viewer to an fsimage
  @echo   fetchdt              fetch a delegation token from the NameNode
  @echo                                                 Use -help to see options
  @echo.
  @echo Most commands print help when invoked w/o parameters.

endlocal
