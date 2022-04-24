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

#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_C_API_TYPES
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_C_API_TYPES

#if _WIN32 || _WIN64
// Windows system.

#if _WIN64
// Windows 64-bit.
typedef long int x_platform_ssize_t;
#else
// Windows 32-bit.
typedef int x_platform_ssize_t;
#endif

#else
// ssize_t is correctly defined by taking bit-ness into account on non-Windows
// systems.
#include <sys/types.h>

typedef ssize_t x_platform_ssize_t;
#endif

#endif
