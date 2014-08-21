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

#ifndef HADOOP_CORE_NDFS_PERMISSION_H
#define HADOOP_CORE_NDFS_PERMISSION_H

#include <stdint.h>

struct hadoop_err;

/**
 * Parse a Hadoop permission string into a numeric value.
 *
 * @param str       The string to parse.
 * @param perm      (out param) the numeric value.
 *
 * @return          NULL on success; the error otherwise. 
 */
struct hadoop_err *parse_permission(const char *str, uint32_t *perm);

#endif

// vim: ts=4:sw=4:tw=79:et
