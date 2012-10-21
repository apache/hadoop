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

if not defined HADOOP_VERSION (
  SET HADOOP_VERSION=1.1.0-SNAPSHOT
)

SET HADOOP_TEST_JAR=hadoop-examples-%HADOOP_VERSION%.jar
SET HADOOP_EXAMPLE_JAR=hadoop-examples-%HADOOP_VERSION%.jar
SET HADOOP_STREAMING_JAR=hadoop-streaming-%HADOOP_VERSION%.jar

SET HADOOP_HOME=%HADOOP_HOME%
SET HADOOP_CONF_DIR=
SET USE_REAL_DATASET=TRUE

SET APP_JAR=%HADOOP_HOME%\%HADOOP_TEST_JAR%
SET EXAMPLE_JAR=%HADOOP_HOME%\%HADOOP_EXAMPLE_JAR%
SET STREAMING_JAR=%HADOOP_HOME%\%HADOOP_STREAMING_JAR%
