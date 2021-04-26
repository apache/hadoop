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

#include <Shlwapi.h>
#include <WinBase.h>
#include <Windows.h>

#include <cstring>

#include "syscall.h"

#pragma comment(lib, "Shlwapi.lib")

bool XPlatform::Syscall::WriteToStdout(const std::string& message) {
  return WriteToStdoutImpl(message.c_str());
}

int XPlatform::Syscall::WriteToStdout(const char* message) {
  return WriteToStdoutImpl(message) ? 1 : 0;
}

bool XPlatform::Syscall::FnMatch(const std::string& pattern,
                                 const std::string& str) {
  return PathMatchSpecA(static_cast<LPCSTR>(str.c_str()),
                        static_cast<LPCSTR>(pattern.c_str())) == TRUE;
}

bool XPlatform::Syscall::WriteToStdoutImpl(const char* message) {
  auto* const stdout_handle = GetStdHandle(STD_OUTPUT_HANDLE);
  if (stdout_handle == INVALID_HANDLE_VALUE || stdout_handle == nullptr) {
    return false;
  }

  unsigned long bytes_written = 0;
  const auto message_len = lstrlen(message);
  const auto result =
      WriteFile(stdout_handle, message, message_len, &bytes_written, nullptr);
  return result && static_cast<unsigned long>(message_len) == bytes_written;
}

void XPlatform::Syscall::ClearBufferSafely(void* buffer,
                                           const size_t sz_bytes) {
  if (buffer != nullptr) {
    SecureZeroMemory(buffer, sz_bytes);
  }
}

bool XPlatform::Syscall::StringCompareIgnoreCase(const std::string& a,
                                                 const std::string& b) {
  return _stricmp(a.c_str(), b.c_str()) == 0;
}
