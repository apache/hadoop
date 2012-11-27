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


@rem Runs a Hadoop command as a daemon.
@rem
@rem Environment Variables
@rem
@rem   HADOOP_CONF_DIR  Alternate conf dir. Default is %HADOOP_HOME%/conf.
@rem   HADOOP_LOG_DIR   Where log files are stored.  PWD by default.
@rem   HADOOP_MASTER    host:path where hadoop code should be copied from
@rem   HADOOP_IDENT_STRING   A string representing this instance of hadoop. %USERNAME% by default
@rem

setlocal
set usage= "Usage: Pass arguments in the given order : hadoop-daemon.cmd [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <hadoop-command> <args...>"

@rem if no args specified, show usage
if [%2] == [] (
    echo %usage%
    goto :eof
)

if not defined HADOOP_BIN_PATH ( 
    set HADOOP_BIN_PATH=%~dp0
)

if "%HADOOP_BIN_PATH:~-1%" == "\" (
    set HADOOP_BIN_PATH=%HADOOP_BIN_PATH:~0,-1%
)

call :updatepath %HADOOP_BIN_PATH%
set hadoop-config-script=%HADOOP_BIN_PATH%\hadoop-config.cmd
call %hadoop-config-script% %*
if "%1" == "--config" (
    shift
    shift
)

if "%1" == "--hosts" (
    shift
    shift
)

@rem get remaining arguments

set startStop=%1
shift
set command=%1
shift

if exist %HADOOP_CONF_DIR%\hadoop-env.cmd (
    call %HADOOP_CONF_DIR%\hadoop-env.cmd
)

set hdfscommands=namenode secondarynamenode datanode
for %%i in ( %hdfscommands% ) do (
    if %command% == %%i set hdfscommand=true
  )

set mapredcommands=jobtracker tasktracker historyserver
  for %%i in ( %mapredcommands% ) do (
    if %command% == %%i set mapredcommand=true  
  )
@rem if an alternate config directory is specified generate new service xml 
if defined %HADOOP_CONF_DIR% (
	move /Y %command%.xml %command%.backup.xml
	if defined hdfscommand (
		%HADOOP_BIN_PATH%\hdfs.cmd --conf %HADOOP_CONF_DIR% makeServiceXml > %command%.xml
	) else (
		if defined mapredcommand (
			%HADOOP_BIN_PATH%\mapred.cmd --conf %HADOOP_CONF_DIR% makeServiceXml > %command%.xml
		) else (
			@echo %usage%
			exit /b 1
		)
	)
	if not exist %command%.xml (
		@echo Generating new conf failed, copying back old conf
		move /Y %command%.backup.xml %command%.xml 
		exit /b 2
    )
)  

if %startStop% == start (
    @echo Stopping any existing services before starting
    sc stop %command%
    @echo starting %command%
    set errorlevel=
    sc start %command%
	if %errorlevel% NEQ 0  (
		@echo Failed to start %command% service
		exit /b 2
	)
	goto :end    
) else (
    if %startStop% == stop (
        @echo stopping %command%
        set errorlevel=
        sc stop %command%
		if %errorlevel% NEQ 0  (
			@echo Failed to stop %command% service
			exit /b 2
		)
        goto :end
    ) else (
        @echo %usage%
        exit /b 1
    )
)
@rem Adds the path sent as argument to path variable 
:updatepath
  set path_to_add=%*
  set current_path_comparable=%path:(x86)=%
  set current_path_comparable=%current_path_comparable: =_%
  set path_to_add_comparable=%path_to_add:(x86)=%
  set path_to_add_comparable=%path_to_add_comparable: =_%
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

:end

endlocal