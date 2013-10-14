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

@rem User for YARN daemons
if not defined HADOOP_YARN_USER (
  set HADOOP_YARN_USER=%yarn%
)

if not defined YARN_CONF_DIR (
  set YARN_CONF_DIR=%HADOOP_YARN_HOME%\conf
)

if defined YARN_HEAPSIZE (
  @rem echo run with Java heapsize %YARN_HEAPSIZE%
  set JAVA_HEAP_MAX=-Xmx%YARN_HEAPSIZE%m
)

if not defined YARN_LOG_DIR (
  set YARN_LOG_DIR=%HADOOP_YARN_HOME%\logs
)

if not defined YARN_LOGFILE (
  set YARN_LOGFILE=yarn.log
)

@rem default policy file for service-level authorization
if not defined YARN_POLICYFILE (
  set YARN_POLICYFILE=hadoop-policy.xml
)

if not defined YARN_ROOT_LOGGER (
  set YARN_ROOT_LOGGER=INFO,console
)

set YARN_OPTS=%YARN_OPTS% -Dhadoop.log.dir=%YARN_LOG_DIR%
set YARN_OPTS=%YARN_OPTS% -Dyarn.log.dir=%YARN_LOG_DIR%
set YARN_OPTS=%YARN_OPTS% -Dhadoop.log.file=%YARN_LOGFILE%
set YARN_OPTS=%YARN_OPTS% -Dyarn.log.file=%YARN_LOGFILE%
set YARN_OPTS=%YARN_OPTS% -Dyarn.home.dir=%HADOOP_YARN_HOME%
set YARN_OPTS=%YARN_OPTS% -Dyarn.id.str=%YARN_IDENT_STRING%
set YARN_OPTS=%YARN_OPTS% -Dhadoop.home.dir=%HADOOP_YARN_HOME%
set YARN_OPTS=%YARN_OPTS% -Dhadoop.root.logger=%YARN_ROOT_LOGGER%
set YARN_OPTS=%YARN_OPTS% -Dyarn.root.logger=%YARN_ROOT_LOGGER%
if defined JAVA_LIBRARY_PATH (
  set YARN_OPTS=%YARN_OPTS% -Djava.library.path=%JAVA_LIBRARY_PATH%
)
set YARN_OPTS=%YARN_OPTS% -Dyarn.policy.file=%YARN_POLICYFILE%