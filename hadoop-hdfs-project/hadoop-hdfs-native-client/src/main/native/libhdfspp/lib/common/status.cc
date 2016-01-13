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

#include "hdfspp/status.h"

#include <cassert>
#include <cstring>

namespace hdfs {

Status::Status(int code, const char *msg1)
    : state_(ConstructState(code, msg1, nullptr)) {}

Status::Status(int code, const char *msg1, const char *msg2)
    : state_(ConstructState(code, msg1, msg2)) {}

const char *Status::ConstructState(int code, const char *msg1,
                                   const char *msg2) {
  assert(code != kOk);
  const uint32_t len1 = strlen(msg1);
  const uint32_t len2 = msg2 ? strlen(msg2) : 0;
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char *result = new char[size + 8 + 2];
  *reinterpret_cast<uint32_t *>(result) = size;
  *reinterpret_cast<uint32_t *>(result + 4) = code;
  memcpy(result + 8, msg1, len1);
  if (len2) {
    result[8 + len1] = ':';
    result[9 + len1] = ' ';
    memcpy(result + 10 + len1, msg2, len2);
  }
  return result;
}

std::string Status::ToString() const {
  if (!state_) {
    return "OK";
  } else {
    uint32_t length = *reinterpret_cast<const uint32_t *>(state_);
    return std::string(state_ + 8, length);
  }
}

const char *Status::CopyState(const char *state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char *result = new char[size + 8];
  memcpy(result, state, size + 8);
  return result;
}
}
