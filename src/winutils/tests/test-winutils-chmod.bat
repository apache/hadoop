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
echo Test various chmod operations
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
call:TestChmod "7" "------rwx"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 2:
call:TestChmod "70" "---rwx---"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 3:
call:TestChmod "u-x,g+r,o=g" "rw-r--r--"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 4:
call:TestChmod "u-x,g+rw" "rw-rw----"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 5:
call:TestChmod "u-x,g+rwx-x,o=u" "rw-rw-rw-"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 6:
call:TestChmodR "755" "rwxr-xr-x" "rwxr-xr-x"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 7:
call:TestChmodR "u-x,g+r,o=g" "rw-r--r--" "rw-r--r--"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 8:
call:TestChmodR "u-x,g+rw" "rw-rw----" "rw-rw----"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 9:
call:TestChmodR "u-x,g+rwx-x,o=u" "rw-rw-rw-" "rw-rw-rw-"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 10:
call:TestChmodR "a+rX" "rw-r--r--" "rwxr-xr-x"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

echo Test case 11:
call:TestChmod "+" "rwx------"
if %ERRORLEVEL% neq 0 goto Failure
echo passed.

:: Cleanup
::
popd
rd %TESTDIR%
goto Success


:: -----------------------------------------------------------------------------
:: -- Function section starts below here
:: -----------------------------------------------------------------------------


::  TestChmod
::  - Test 'chmod' mode or octal mode string of a single file
::  - The new permission will be compared with the expected result
::
:TestChmod :: [mode|octal mode] [expected permission]
if exist a exit /B 1
type NUL>a
%WINUTILS% chmod 700 a
%WINUTILS% chmod %~1 a
call:CmpPerm "a" %~2
if not %ERRORLEVEL% == 0 exit /B 1
del a
exit /B 0


::  TestChmodR
::  - Test 'chmod' mode or octal mode string with recursive option
::  - The new permission will be compared with the expected results
::  - The permissions could be different due to 'X' in mode string
::
:TestChmodR :: [mode|octal mode] [expected permission] [expected permission x]
if exist a exit /B 1
md a
%WINUTILS% chmod 700 a
type NUL>a\a
%WINUTILS% chmod 600 a\a
md a\b
%WINUTILS% chmod 700 a\b
md a\b\a
%WINUTILS% chmod 700 a\b\a
type NUL>a\b\b
%WINUTILS% chmod 600 a\b\b
type NUL>a\b\x
%WINUTILS% chmod u+x a\b\x

%WINUTILS% chmod -R %~1 a

call:CmpPerm "a" %~3
if not %ERRORLEVEL% == 0 exit /B 1
call:CmpPerm "a\a" %~2
if not %ERRORLEVEL% == 0 exit /B 1
call:CmpPerm "a\b" %~3
if not %ERRORLEVEL% == 0 exit /B 1
call:CmpPerm "a\b\a" %~3
if not %ERRORLEVEL% == 0 exit /B 1
call:CmpPerm "a\b\b" %~2
if not %ERRORLEVEL% == 0 exit /B 1
call:CmpPerm "a\b\x" %~3
if not %ERRORLEVEL% == 0 exit /B 1

rd /S /Q a
exit /B 0


::  CmpPerm
::  - Compare file permission against given permission
::  - Use 'ls' to get the new permission
::  - Exit with errorlevel > 0 on failure
::
:CmpPerm :: [file] [perm]
for /F "tokens=1" %%A in ('call %WINUTILS% ls %~1') do set PERM=%%A
if not %ERRORLEVEL% == 0 exit /B 1
if not %PERM:~-9%==%~2 exit /B 1
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
