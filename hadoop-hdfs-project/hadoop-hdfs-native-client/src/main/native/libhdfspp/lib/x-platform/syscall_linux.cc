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

#include <unistd.h>

#include <cstring>

#include "syscall.h"

bool XPlatform::Syscall::WriteToStdout(const std::string& message) {
  return WriteToStdoutImpl(message.c_str());
}

int XPlatform::Syscall::WriteToStdout(const char* message) {
  return WriteToStdoutImpl(message) ? 1 : 0;
}

bool XPlatform::Syscall::WriteToStdoutImpl(const char* message) {
  const auto message_len = strlen(message);
  const auto result = write(1, message, message_len);
  return result == static_cast<ssize_t>(message_len);
}
