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

#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_C_API_SYSCALL_H
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_C_API_SYSCALL_H

/**
 * C APIs for accessing XPlatform
 */

int x_platform_syscall_write_to_stdout(const char* msg);
int x_platform_syscall_create_and_open_temp_file(char* pattern,
                                                 size_t pattern_len);
int x_platform_syscall_close_file(int fd);

#endif  // NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_C_API_SYSCALL_H
