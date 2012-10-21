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

REM Environment configuration
REM Hadoop installation
REM set var only if it has not already been set externally
if not defined HADOOP_HOME (
  SET HADOOP_HOME=
)

REM *-expansion for jar references doesn't work when quoted.
REM so we use a version variable.
if not defined HADOOP_VERSION (
  SET HADOOP_VERSION=1.1.0-SNAPSHOT
)

REM Base directory for gridmix install
REM set var only if it has not already been set externally
if not defined GRID_MIX_HOME (
  set GRID_MIX_HOME="%GRID_DIR%"
)

REM Hadoop example jar
REM set var only if it has not already been set externally
if not defined EXAMPLE_JAR (
  set EXAMPLE_JAR="%HADOOP_HOME%\hadoop-examples-%HADOOP_VERSION%.jar"
)

REM Hadoop test jar
REM # set var only if it has not already been set externally
if not defined APP_JAR (
  set APP_JAR="%HADOOP_HOME%\hadoop-test-%HADOOP_VERSION%.jar"
)

REM Hadoop streaming jar
REM set var only if it has not already been set externally
if not defined STREAM_JAR (
  set STREAM_JAR="%HADOOP_HOME%\hadoop-streaming-%HADOOP_VERSION%.jar"
)

REM Location on default filesystem for writing gridmix data (usually HDFS)
REM Default: /gridmix/data
REM set var only if it has not already been set externally
if not defined GRID_MIX_DATA (
  set GRID_MIX_DATA=/gridmix/data
)

REM Location of executables in default filesystem (usually HDFS)
REM Default: /gridmix/programs
REM set var only if it has not already been set externally
if not defined GRID_MIX_PROG (
  set GRID_MIX_PROG=/gridmix/programs
)

REM Data sources
REM Variable length key, value compressed SequenceFile
set VARCOMPSEQ=%GRID_MIX_DATA%/WebSimulationBlockCompressed
REM Fixed length key, value compressed SequenceFile
set FIXCOMPSEQ=%GRID_MIX_DATA%/MonsterQueryBlockCompressed
REM Variable length key, value uncompressed Text File
set VARINFLTEXT=%GRID_MIX_DATA%/SortUncompressed
REM Fixed length key, value compressed Text File
set FIXCOMPTEXT=%GRID_MIX_DATA%/EntropySimulationCompressed

REM Job sizing
set NUM_OF_LARGE_JOBS_FOR_ENTROPY_CLASS=5
set NUM_OF_LARGE_JOBS_PER_CLASS=3
set NUM_OF_MEDIUM_JOBS_PER_CLASS=20
set NUM_OF_SMALL_JOBS_PER_CLASS=40

set NUM_OF_REDUCERS_FOR_LARGE_JOB=370
set NUM_OF_REDUCERS_FOR_MEDIUM_JOB=170
set NUM_OF_REDUCERS_FOR_SMALL_JOB=15

REM Throttling
set INTERVAL_BETWEEN_SUBMITION=20

REM Hod
REM export HOD_OPTIONS=""

set CLUSTER_DIR_BASE="%GRID_MIX_HOME%\CLUSTER_DIR_BASE"
set HOD_CONFIG=
set ALL_HOD_OPTIONS="-c %HOD_CONFIG% %HOD_OPTIONS%"
set SMALL_JOB_HOD_OPTIONS="%ALL_HOD_OPTIONS% -n 5"
set MEDIUM_JOB_HOD_OPTIONS="%ALL_HOD_OPTIONS% -n 50"
set LARGE_JOB_HOD_OPTIONS="%ALL_HOD_OPTIONS% -n 100"


echo HADOOP_HOME=%HADOOP_HOME%
echo HADOOP_VERSION=%HADOOP_VERSION%
echo GRID_MIX_HOME=%GRID_MIX_HOME%
echo VARCOMPSEQ=%VARCOMPSEQ%
echo CLUSTER_DIR_BASE=%CLUSTER_DIR_BASE%
echo ALL_HOD_OPTIONS=%ALL_HOD_OPTIONS%