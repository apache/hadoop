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

#ifndef HADOOP_CORE_COMMON_HADOOP_ERR
#define HADOOP_CORE_COMMON_HADOOP_ERR

#include <uv.h> /* for uv_loop_t */

/**
 * A Hadoop error.  This is the libhadoop-core version of an IOException.
 */
struct hadoop_err;

/**
 * Allocate a new local error.
 *
 * @param code          Error code.
 * @param fmt           printf-style format.
 * @param ...           printf-style arguments.
 *
 * @return              A new error message.  This will never be NULL.
 */
struct hadoop_err *hadoop_lerr_alloc(int code, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

/**
 * Allocate a new error object based on a libuv error.
 *
 * @param loop          The libuv loop to check.
 * @param fmt           printf-style format.
 * @param ...           printf-style arguments.
 *
 * @return              A new error message.  This will never be NULL.
 */
struct hadoop_err *hadoop_uverr_alloc(int cod, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

/**
 * Given a hadoop error, get the error code.
 *
 * @param err       The hadoop error.
 *
 * @return          The error code.
 */
int hadoop_err_code(const struct hadoop_err *err);

/**
 * Given a hadoop error, get the error message.
 *
 * @param err       The hadoop error.
 *
 * @return          The error message.  Valid until the hadoop_err
 *                  object is freed.
 */
const char *hadoop_err_msg(const struct hadoop_err *err);

/**
 * Free a hadoop error.
 *
 * @param err       The hadoop error.
 */
void hadoop_err_free(struct hadoop_err *err);

#endif

// vim: ts=4:sw=4:tw=79:et
