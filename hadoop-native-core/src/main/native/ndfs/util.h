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

#ifndef HADOOP_CORE_NDFS_UTIL_H
#define HADOOP_CORE_NDFS_UTIL_H

struct hadoop_err;
struct hrpc_proxy;
struct native_fs;

/**
 * Initialize a namenode proxy.
 *
 * @param fs            The native filesystem.
 * @param proxy         (out param) The proxy to initialize.
 */
void ndfs_nn_proxy_init(struct native_fs *fs, struct hrpc_proxy *proxy);

/**
 * Construct a canonical path from a URI.
 *
 * @param fs        The filesystem.
 * @param uri       The URI.
 * @param out       (out param) the canonical path.
 *
 * @return          NULL on success; the error otherwise.
 */
struct hadoop_err *build_path(struct native_fs *fs, const char *uri_str,
                                     char **out);

#endif

// vim: ts=4:sw=4:tw=79:et
