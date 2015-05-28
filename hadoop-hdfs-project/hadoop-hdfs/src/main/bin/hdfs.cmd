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

call %HADOOP_LIBEXEC_DIR%\hdfs-config.cmd %*
if "%1" == "--config" (
  shift
  shift
)

:main
  if exist %HADOOP_CONF_DIR%\hadoop-env.cmd (
    call %HADOOP_CONF_DIR%\hadoop-env.cmd
  )

  set hdfs-command=%1
  call :make_command_arguments %*

  if not defined hdfs-command (
      goto print_usage
  )

  set hdfscommands=dfs namenode secondarynamenode journalnode zkfc datanode dfsadmin haadmin fsck balancer jmxget oiv oev fetchdt getconf groups snapshotDiff lsSnapshottableDir cacheadmin mover storagepolicies crypto
  for %%i in ( %hdfscommands% ) do (
    if %hdfs-command% == %%i set hdfscommand=true
  )
  if defined hdfscommand (
    call :%hdfs-command%
  ) else (
    set CLASSPATH=%CLASSPATH%;%CD%
    set CLASS=%hdfs-command%
  )

  set java_arguments=%JAVA_HEAP_MAX% %HADOOP_OPTS% -classpath %CLASSPATH% %CLASS% %hdfs-command-arguments%
  call %JAVA% %java_arguments%

goto :eof

:namenode
  set CLASS=org.apache.hadoop.hdfs.server.namenode.NameNode
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_NAMENODE_OPTS%
  goto :eof

:journalnode
  set CLASS=org.apache.hadoop.hdfs.qjournal.server.JournalNode
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_JOURNALNODE_OPTS%
  goto :eof

:zkfc
  set CLASS=org.apache.hadoop.hdfs.tools.DFSZKFailoverController
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_ZKFC_OPTS%
  goto :eof

:secondarynamenode
  set CLASS=org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_SECONDARYNAMENODE_OPTS%
  goto :eof

:datanode
  set CLASS=org.apache.hadoop.hdfs.server.datanode.DataNode
  set HADOOP_OPTS=%HADOOP_OPTS% -server %HADOOP_DATANODE_OPTS%
  goto :eof

:dfs
  set CLASS=org.apache.hadoop.fs.FsShell
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:dfsadmin
  set CLASS=org.apache.hadoop.hdfs.tools.DFSAdmin
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_CLIENT_OPTS%
  goto :eof

:haadmin
  set CLASS=org.apache.hadoop.hdfs.tools.DFSHAAdmin
  set CLASSPATH=%CLASSPATH%;%TOOL_PATH%
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
  set CLASS=org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewerPB
  goto :eof

:oev
  set CLASS=org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsViewer
  goto :eof

:fetchdt
  set CLASS=org.apache.hadoop.hdfs.tools.DelegationTokenFetcher
  goto :eof

:getconf
  set CLASS=org.apache.hadoop.hdfs.tools.GetConf
  goto :eof

:groups
  set CLASS=org.apache.hadoop.hdfs.tools.GetGroups
  goto :eof

:snapshotDiff
  set CLASS=org.apache.hadoop.hdfs.tools.snapshot.SnapshotDiff
  goto :eof

:lsSnapshottableDir
  set CLASS=org.apache.hadoop.hdfs.tools.snapshot.LsSnapshottableDir
  goto :eof

:cacheadmin
  set CLASS=org.apache.hadoop.hdfs.tools.CacheAdmin
  goto :eof

:mover
  set CLASS=org.apache.hadoop.hdfs.server.mover.Mover
  set HADOOP_OPTS=%HADOOP_OPTS% %HADOOP_MOVER_OPTS%
  goto :eof

:storagepolicies
  set CLASS=org.apache.hadoop.hdfs.tools.GetStoragePolicies
  goto :eof

:crypto
  set CLASS=org.apache.hadoop.hdfs.tools.CryptoAdmin
  goto :eof

@rem This changes %1, %2 etc. Hence those cannot be used after calling this.
:make_command_arguments
  if "%1" == "--config" (
    shift
    shift
  )
  if [%2] == [] goto :eof
  shift
  set _hdfsarguments=
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
  @echo   dfs                  run a filesystem command on the file systems supported in Hadoop.
  @echo   namenode -format     format the DFS filesystem
  @echo   secondarynamenode    run the DFS secondary namenode
  @echo   namenode             run the DFS namenode
  @echo   journalnode          run the DFS journalnode
  @echo   zkfc                 run the ZK Failover Controller daemon
  @echo   datanode             run a DFS datanode
  @echo   dfsadmin             run a DFS admin client
  @echo   haadmin              run a DFS HA admin client
  @echo   fsck                 run a DFS filesystem checking utility
  @echo   balancer             run a cluster balancing utility
  @echo   jmxget               get JMX exported values from NameNode or DataNode.
  @echo   oiv                  apply the offline fsimage viewer to an fsimage
  @echo   oev                  apply the offline edits viewer to an edits file
  @echo   fetchdt              fetch a delegation token from the NameNode
  @echo   getconf              get config values from configuration
  @echo   groups               get the groups which users belong to
  @echo   snapshotDiff         diff two snapshots of a directory or diff the
  @echo                        current directory contents with a snapshot
  @echo   lsSnapshottableDir   list all snapshottable dirs owned by the current user
  @echo 						Use -help to see options
  @echo   cacheadmin           configure the HDFS cache
  @echo   crypto               configure HDFS encryption zones
  @echo   mover                run a utility to move block replicas across storage types
  @echo   storagepolicies      get all the existing block storage policies
  @echo.
  @echo Most commands print help when invoked w/o parameters.

endlocal
