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

#ifndef LIBHDFS3_POSIX_PLATFORM_H
#define LIBHDFS3_POSIX_PLATFORM_H

#include "build.h"

#define THREAD_LOCAL __thread
#define ATTRIBUTE_NORETURN __attribute__ ((noreturn))
#define ATTRIBUTE_NOINLINE __attribute__ ((noinline))

#define GCC_VERSION (__GNUC__ * 10000 \
                     + __GNUC_MINOR__ * 100 \
                     + __GNUC_PATCHLEVEL__)

// defined by gcc
#if defined(__ELF__) && defined(OS_LINUX)
# define HAVE_SYMBOLIZE
#elif defined(OS_MACOSX) && defined(HAVE_DLADDR)
// Use dladdr to symbolize.
# define HAVE_SYMBOLIZE
#endif

#define STACK_LENGTH 64
#define PATH_SEPRATOR '/'

#endif // LIBHDFS3_POSIX_PLATFORM_H
