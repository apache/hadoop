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

SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set NUM_OF_REDUCERS=1
set INDIR=%VARCOMPSEQ%
FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)

set OUTDIR=perf-out/webdata-scan-out-dir-large_%Date%
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%

CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 0.2 -keepred 5 ^
    -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ^
    -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text -indir %INDIR% -outdir %OUTDIR% -r %NUM_OF_REDUCERS%
