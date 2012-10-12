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
echo Test most basic security settings are working
setlocal
set WINUTILS="%HADOOP_HOME%\bin\winutils.exe"
set TESTDIR=winutils-test

:: Setup test directory
::
if not exist %TESTDIR% md %TESTDIR%
pushd %TESTDIR%

:: Test cases
::
echo Test case 1:
::  - Create a file.
::  - Change mode to 377 so owner does not have read permission.
::  - Verify the owner truly does not have the permissions to read.
if exist a goto Failure
type NUL>a
%WINUTILS% chmod 377 a
if not %ERRORLEVEL% == 0 goto Failure
type a
if %ERRORLEVEL% == 0 goto Failure
del a
if not %ERRORLEVEL% == 0 goto Failure
echo passed.

echo Test case 2:
::  - Create a file.
::  - Change mode to 577 so owner does not have write permission.
::  - Verify the owner truly does not have the permissions to write.
if exist a goto Failure
type NUL>a
%WINUTILS% chmod 577 a
if not %ERRORLEVEL% == 0 goto Failure
cmd /C "echo a>>a"
if %ERRORLEVEL% == 0 goto Failure
del a
if not %ERRORLEVEL% == 0 goto Failure
echo passed.

echo Test case 3:
::  - Copy WINUTILS to a new executable file, a.exe.
::  - Change mode to 677 so owner does not have execute permission.
::  - Verify the owner truly does not have the permissions to execute the file.
if exist a.exe goto Failure
copy %WINUTILS% a.exe >NUL
%WINUTILS% chmod 677 a.exe
if not %ERRORLEVEL% == 0 goto Failure
.\a.exe ls
if %ERRORLEVEL% == 0 goto Failure
del a.exe
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
