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

/*
 * We will use XPlatform's dirent on Windows or when the macro
 * USE_X_PLATFORM_DIRENT is defined.
 */
#if defined(WIN32) || defined(USE_X_PLATFORM_DIRENT)

/*
 * We will use extern "C" only on Windows.
 */
#if defined(WIN32) && defined(__cplusplus)
extern "C" {
#endif

/**
 * DIR struct holds the pointer to XPlatform::Dirent instance. Since this will
 * be used in C, we can't hold the pointer to XPlatform::Dirent. We're working
 * around this by using a void pointer and casting it to XPlatform::Dirent when
 * needed in C++.
 */
typedef struct DIR {
  void *x_platform_dirent_ptr;
} DIR;

/**
 * dirent struct contains the name of the file/folder while iterating through
 * the directory's children.
 */
struct dirent {
  char d_name[256];
};

/**
 * Opens a directory for iteration. Internally, it instantiates DIR struct for
 * the given path. closedir must be called on the returned pointer to DIR struct
 * when done.
 *
 * @param dir_path The path to the directory to iterate through.
 * @return A pointer to the DIR struct.
 */
DIR *opendir(const char *dir_path);

/**
 * For iterating through the children of the directory pointed to by the DIR
 * struct pointer.
 *
 * @param dir The pointer to the DIR struct.
 * @return A pointer to dirent struct containing the name of the current child
 * file/folder.
 */
struct dirent *readdir(DIR *dir);

/**
 * De-allocates the XPlatform::Dirent instance pointed to by the DIR pointer.
 *
 * @param dir The pointer to DIR struct to close.
 * @return 0 if successful.
 */
int closedir(DIR *dir);

#if defined(WIN32) && defined(__cplusplus)
}
#endif

#else
/*
 * For non-Windows environments, we use the dirent.h header itself.
 */
#include <dirent.h>

#endif

#endif