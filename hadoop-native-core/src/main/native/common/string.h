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

#ifndef HADOOP_CORE_COMMON_STRING
#define HADOOP_CORE_COMMON_STRING

#include <stdio.h>
#include <stdint.h>

/**
 * Print out a buffer in hexadecimal.
 *
 * @param fp        The FILE* to print to.
 * @param buf       The buffer to print.
 * @param buf_len   The length of the buffer.
 * @param fmt       Printf-style format.
 * @param ...       Printf-style arguments.
 */
void hex_buf_print(FILE *fp, const void *buf, int32_t buf_len,
        const char *fmt, ...) __attribute__((format(printf, 4, 5)));

#endif

// vim: ts=4:sw=4:tw=79:et
