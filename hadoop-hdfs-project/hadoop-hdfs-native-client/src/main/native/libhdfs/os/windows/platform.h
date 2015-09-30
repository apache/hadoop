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

#ifndef LIBHDFS_PLATFORM_H
#define LIBHDFS_PLATFORM_H

#include <stdio.h>
#include <windows.h>
#include <winsock.h>

/*
 * O_ACCMODE defined to match Linux definition.
 */
#ifndef O_ACCMODE
#define O_ACCMODE 0x0003
#endif

/*
 * Windows has a different name for its maximum path length constant.
 */
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif

/*
 * Windows does not define EDQUOT and ESTALE in errno.h.  The closest equivalents
 * are these constants from winsock.h.
 */
#ifndef EDQUOT
#define EDQUOT WSAEDQUOT
#endif

#ifndef ESTALE
#define ESTALE WSAESTALE
#endif

/*
 * gcc-style type-checked format arguments are not supported on Windows, so just
 * stub this macro.
 */
#define TYPE_CHECKED_PRINTF_FORMAT(formatArg, varArgs)

/*
 * Define macros for various string formatting functions not defined on Windows.
 * Where possible, we reroute to one of the secure CRT variants.  On Windows,
 * the preprocessor does support variadic macros, even though they weren't
 * defined until C99.
 */
#define snprintf(str, size, format, ...) \
  _snprintf_s((str), (size), _TRUNCATE, (format), __VA_ARGS__)
#define strncpy(dest, src, n) \
  strncpy_s((dest), (n), (src), _TRUNCATE)
#define strtok_r(str, delim, saveptr) \
  strtok_s((str), (delim), (saveptr))
#define vsnprintf(str, size, format, ...) \
  vsnprintf_s((str), (size), _TRUNCATE, (format), __VA_ARGS__)

/*
 * Mutex data type defined as Windows CRITICAL_SECTION.   A critical section (not
 * Windows mutex) is used, because libhdfs only needs synchronization of multiple
 * threads within a single process, not synchronization across process
 * boundaries.
 */
typedef CRITICAL_SECTION mutex;

/*
 * Thread data type defined as HANDLE to a Windows thread.
 */
typedef HANDLE threadId;

#endif
