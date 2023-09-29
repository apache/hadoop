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
#ifndef UTILS_FILE_UTILS_H
#define UTILS_FILE_UTILS_H

#include <stdbool.h>

/**
 * Read the contents of the specified file into an allocated buffer and return
 * the contents as a NUL-terminated string. NOTE: The file contents must not
 * contain a NUL character or the result will appear to be truncated.
 *
 * Returns a pointer to the allocated, NUL-terminated string or NULL on error.
 */
char* read_file_to_string(const char* filename);

/**
 * Read a file to a string as the YARN nodemanager user and returns the
 * result as a string. See read_file_to_string for more details.
 *
 * Returns a pointer to the allocated, NUL-terminated string or NULL on error.
 */
char* read_file_to_string_as_nm_user(const char* filename);

/**
 * Write a sequence of bytes to a new file as the YARN nodemanager user.
 *
 * Returns true on success or false on error.
 */
bool write_file_as_nm(const char* path, const void* data, size_t count);

#endif /* UTILS_FILE_UTILS_H */
