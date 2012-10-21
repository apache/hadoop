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
SET GRID_MIX=%~dp0
CD %GRID_MIX%
CALL "%GRID_MIX%gridmix-env-2.cmd"

FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)

ECHO %Date% > %1_start.out


SET HADOOP_CLASSPATH=%APP_JAR%:%EXAMPLE_JAR%:%STREAMING_JAR%
SET LIBJARS=%APP_JAR%,%EXAMPLE_JAR%,%STREAMING_JAR%

CALL %HADOOP_HOME%\bin\hadoop jar gridmix.jar org.apache.hadoop.mapred.GridMixRunner -libjars %LIBJARS%

FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)

ECHO %Date% > %1_end.out

	