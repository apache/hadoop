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

SETLOCAL ENABLEDELAYEDEXPANSION 
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd


set I=1
:loop1
  echo Iteration: !I!
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\monsterQuery\monster_query.small.cmd"  ^> monster_query.small.!i!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_SMALL_JOBS_PER_CLASS%  goto loop1


set I=1
:loop2
  echo Iteration: !I!
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\monsterQuery\monster_query.medium.cmd"  ^> monster_query.medium.!i!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_MEDIUM_JOBS_PER_CLASS%  goto loop2

set I=1
:loop3
  echo Iteration: !I!
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\monsterQuery\monster_query.large.cmd"  ^> monster_query.large.!i!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_LARGE_JOBS_PER_CLASS%  goto loop3

CALL "%GRID_MIX_HOME%\submissionScripts\WaitAllGridmix.cmd"