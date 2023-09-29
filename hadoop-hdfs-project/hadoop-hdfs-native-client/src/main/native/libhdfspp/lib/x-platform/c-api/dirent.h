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

#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_C_API_DIRENT_H
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_C_API_DIRENT_H

#if !(defined(WIN32) || defined(USE_X_PLATFORM_DIRENT))

/*
 * For non-Windows environments, we use the dirent.h header itself.
 */
#include <dirent.h>

#else

/*
 * If it's a Windows environment or if the macro USE_X_PLATFORM_DIRENT is
 * defined, we switch to using dirent from the XPlatform library.
 */
#include "x-platform/c-api/extern/dirent.h"

#endif

#endif
