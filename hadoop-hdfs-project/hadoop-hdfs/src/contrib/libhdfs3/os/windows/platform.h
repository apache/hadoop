/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIBHDFS3_WINDOWS_PLATFORM_H
#define LIBHDFS3_WINDOWS_PLATFORM_H

#include "build.h"

#define THREAD_LOCAL __declspec(thread)
#define ATTRIBUTE_NORETURN __attribute__ ((noreturn))
#define ATTRIBUTE_NOINLINE __attribute__ ((noinline))

#define STACK_LENGTH 64

/*
 * Get rid of extra stuff.
 */
#define VC_EXTRALEAN
#define WIN32_LEAN_AND_MEAN

/*
 * Windows does not understand __attribute__, which is gcc-specific.
 * Hence make this key work invalid
 */
#define __attribute__(A) /* do nothing */

/*
 * Include Windows specific headers in one place.
 */
#include <WinSock2.h>
#include <Windows.h>
#include <io.h>
#include <IPHlpApi.h>
#pragma comment(lib, "IPHLPAPI.lib")
#include <process.h>
#include <Rpc.h>
#include <RpcDce.h> /* for UidCreate */
#include <stdio.h>
#include <ws2ipdef.h>
#include <Ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")

/*
 * Account for the lack of inttypes.h in Visual Studio 2010
 */
#define PRId32 "ld"
#define _PFX_64 "ll"
#define PRId64 _PFX_64 "d"

/*
 * Define some macros for equivalent functions in Windows.
 * Most of the time it is a rename. Sometimes, we have to redefine
 * function signature as well.
 */

/*
 * File access.
 */
#define R_OK 4 /* Read only */
#define STDERR_FILENO 2
#define access _access
#define lseek _lseek

/*
 * String related.
 */
#define snprintf(str, size, format, ...) \
	_snprintf_s((str), (size), _TRUNCATE, (format), __VA_ARGS__)
#define strcasecmp _stricmp
#define strerror_r(errnum, buf, buflen) strerror_s((buf), (buflen), (errnum))
#define strtoll _strtoi64
#define strtok_r strtok_s
#define vsnprintf(str, size, format, ...) \
	vsnprintf_s((str), (size), _TRUNCATE, (format), __VA_ARGS__)

// Others.
#define getpid _getpid
#define pthread_self GetCurrentThreadId
#define setenv(name, value, overwrite) _putenv_s((name), (value))

/*
 * ERROR macro conflicts with ERROR def in some protobuf header.
 * Thus, undef it here.
 */
#undef ERROR

/*
 * Define path separator macro.
 */
#define PATH_SEPRATOR '\\'

/*
 * Support for signals in Windows is limited.
 */
typedef unsigned long sigset_t;

#endif // LIBHDFS3_WINDOWS_PLATFORM_H
