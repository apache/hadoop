@ECHO OFF
@REM Licensed to the Apache Software Foundation (ASF) under one or more
@REM contributor license agreements.  See the NOTICE file distributed with
@REM this work for additional information regarding copyright ownership.
@REM The ASF licenses this file to You under the Apache License, Version 2.0
@REM (the "License"); you may not use this file except in compliance with
@REM the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.

@WHERE devenv
IF %ERRORLEVEL% NEQ 0 (
  @ECHO "devenv command was not found. Verify your compiler installation level."
  EXIT /b 1
)

@REM Need to save output to a file because for loop will just
@REM loop forever... :(

SET srcdir=%1
SET workdir=%2

IF EXIST %srcdir%\Backup (
  @ECHO "Solution files already upgraded."
  EXIT /b 0
)

CD %srcdir%
DIR /B *.sln > %workdir%\HADOOP-SLN-UPGRADE.TXT

FOR /F %%f IN (%workdir%\HADOOP-SLN-UPGRADE.TXT) DO (
  devenv %%f /upgrade
)
