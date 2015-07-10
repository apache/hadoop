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

#include "util.h"

#include <array>
#include <functional>
#include <algorithm>

namespace hdfs {

std::string Base64Encode(const std::string &src) {
  static const char kDictionary[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    "abcdefghijklmnopqrstuvwxyz"
                                    "0123456789+/";

  int encoded_size = (src.size() + 2) / 3 * 4;
  std::string dst;
  dst.reserve(encoded_size);

  size_t i = 0;
  while (i + 3 < src.length()) {
    const char *s = &src[i];
    const int r[4] = {s[0] >> 2, ((s[0] << 4) | (s[1] >> 4)) & 0x3f,
                      ((s[1] << 2) | (s[2] >> 6)) & 0x3f, s[2] & 0x3f};

    std::transform(r, r + sizeof(r) / sizeof(int), std::back_inserter(dst),
                   [&r](unsigned char v) { return kDictionary[v]; });
    i += 3;
  }

  size_t remained = src.length() - i;
  const char *s = &src[i];

  switch (remained) {
  case 0:
    break;
  case 1: {
    char padding[4] = {kDictionary[s[0] >> 2], kDictionary[(s[0] << 4) & 0x3f],
                       '=', '='};
    dst.append(padding, sizeof(padding));
  } break;
  case 2: {
    char padding[4] = {kDictionary[src[i] >> 2],
                       kDictionary[((s[0] << 4) | (s[1] >> 4)) & 0x3f],
                       kDictionary[(s[1] << 2) & 0x3f], '='};
    dst.append(padding, sizeof(padding));
  } break;
  default:
    assert("Unreachable");
    break;
  }
  return dst;
}

}
