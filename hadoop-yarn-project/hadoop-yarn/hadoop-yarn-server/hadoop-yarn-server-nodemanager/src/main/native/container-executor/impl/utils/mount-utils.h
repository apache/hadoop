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

#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_MOUNT_UTIL_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_MOUNT_UTIL_H__

typedef struct mount_options_struct {
  char **opts;
  unsigned int num_opts;
  unsigned int rw; // 0 for read, 1 for write
} mount_options;

typedef struct mount_struct {
    char *src;
    char *dest;
    mount_options *options;
} mount;

void free_mount_options(mount_options *options);

void free_mounts(mount *mounts, const unsigned int num_mounts);

int validate_mounts(char **permitted_ro_mounts, char **permitted_rw_mounts, mount *mounts, unsigned int num_mounts);

#endif
