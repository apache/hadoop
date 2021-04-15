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

#include "x-platform/syscall.h"

#include <algorithm>
#include <vector>

extern "C" {
int x_platform_syscall_write_to_stdout(const char* msg) {
  return XPlatform::Syscall::WriteToStdout(msg) ? 1 : 0;
}

int x_platform_syscall_create_and_open_temp_file(char* pattern,
                                                 const size_t pattern_len) {
  std::vector<char> pattern_vec(pattern, pattern + pattern_len);

  const auto fd = XPlatform::Syscall::CreateAndOpenTempFile(pattern_vec);
  if (fd != -1) {
    std::copy_n(pattern_vec.begin(), pattern_len, pattern);
  }
  return fd;
}

int x_platform_syscall_close_file(const int fd) {
  return XPlatform::Syscall::CloseFile(fd);
}
}
