:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements. See the NOTICE file distributed with this
:: work for additional information regarding copyright ownership. The ASF
:: licenses this file to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
:: 
:: http://www.apache.org/licenses/LICENSE-2.0
:: 
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
:: WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
:: License for the specific language governing permissions and limitations under
:: the License.
::
@echo off
echo Test chown operations
setlocal
set WINUTILS="%HADOOP_HOME%\bin\winutils.exe"
set TESTDIR=winutils-test

:: Setup test directory
::
if not exist %TESTDIR% md %TESTDIR%
pushd %TESTDIR%

:: Get username
::
for /F "tokens=1" %%A in ('call whoami') do set USER=%%A

:: Test cases
::
echo Test case 1:
if exist a goto Failure
type NUL>a
%WINUTILS% chown %USER%:Administrators a
if not %ERRORLEVEL% == 0 goto Failure
call:CmpOwn "a" "%USER%" "BUILTIN\Administrators"
if not %ERRORLEVEL% == 0 goto Failure
del a
if not %ERRORLEVEL% == 0 goto Failure
echo passed.

echo Test case 2:
if exist a goto Failure
type NUL>a
%WINUTILS% chown %USER% a
if not %ERRORLEVEL% == 0 goto Failure
%WINUTILS% chown :Administrators a
if not %ERRORLEVEL% == 0 goto Failure
call:CmpOwn "a" "%USER%" "BUILTIN\Administrators"
if not %ERRORLEVEL% == 0 goto Failure
del a
if not %ERRORLEVEL% == 0 goto Failure
echo passed.


:: Cleanup
::
popd
rd %TESTDIR%
goto Success

:: -----------------------------------------------------------------------------
:: -- Function section starts below here
:: -----------------------------------------------------------------------------

::  CmpOwn
::  - Compare file ownership against expected owner
::  - Use 'ls' to get the ownership
::  - Exit with errorlevel > 0 on failure
::
:CmpOwn :: [file] [expected user owner] [expected group owner]
for /F "tokens=3" %%A in ('call %WINUTILS% ls %~1') do set OWNER=%%A
if not %ERRORLEVEL% == 0 exit /B 1
for /F "tokens=4" %%A in ('call %WINUTILS% ls %~1') do set GROUP=%%A
if not %ERRORLEVEL% == 0 exit /B 1
if /I not %OWNER%==%~2 exit /B 1
if /I not %GROUP%==%~3 exit /B 1
exit /B 0 


::  Failure
::  - Report test failure
::
:Failure
echo Test failed.
exit /B 1

::  Success
::  - Report test success
::
:Success
echo Test succeeded.
exit /B 0
