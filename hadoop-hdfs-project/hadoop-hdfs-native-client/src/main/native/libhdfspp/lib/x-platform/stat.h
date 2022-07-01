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

#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_STAT
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_STAT

#if defined(_WIN32)

// Windows.
// These macros are derived from POSIX sys/stat.h. Windows defines some of
// these macros, but not all. Thus, we align with the bits defined by POSIX for
// all of them to be consistent.
#define S_IRUSR 0400
#define S_IWUSR 0200
#define S_IXUSR 0100
#define S_IRGRP (S_IRUSR >> 3)
#define S_IWGRP (S_IWUSR >> 3)
#define S_IXGRP (S_IXUSR >> 3)
#define S_IROTH (S_IRGRP >> 3)
#define S_IWOTH (S_IWGRP >> 3)
#define S_IXOTH (S_IXGRP >> 3)

#else

// Linux (or other non-Windows OS).
#include <sys/stat.h>

#endif

#endif
