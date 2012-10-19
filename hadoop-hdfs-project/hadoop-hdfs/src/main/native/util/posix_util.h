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

#ifndef __POSIX_UTIL_H__
#define __POSIX_UTIL_H__

/**
 * Recursively delete the contents of a directory
 *
 * @param path      directory whose contents we should delete
 *
 * @return          0 on success; error code otherwise
 */
int recursiveDeleteContents(const char *path);

/**
 * Recursively delete a local path, using unlink or rmdir as appropriate.
 *
 * @param path      path to delete
 *
 * @return          0 on success; error code otherwise
 */
int recursiveDelete(const char *path);

/**
 * Get a temporary directory 
 *
 * @param tempDir   (out param) path to the temporary directory
 * @param nameMax   Length of the tempDir buffer
 * @param mode      Mode to create with
 *
 * @return          0 on success; error code otherwise
 */
int createTempDir(char *tempDir, int nameMax, int mode);

/**
 * Sleep without using signals
 *
 * @param sec       Number of seconds to sleep
 */
void sleepNoSig(int sec);

#endif
