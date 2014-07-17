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

#ifndef CHECKSUM_H_
#define CHECKSUM_H_

#include <stdint.h>
#include <sys/types.h>

namespace NativeTask {

extern uint32_t crc32_sb8(uint32_t, const uint8_t *, size_t);
extern uint32_t crc32c_sb8(uint32_t, const uint8_t *, size_t);

enum ChecksumType {
  CHECKSUM_NONE,
  CHECKSUM_CRC32,
  CHECKSUM_CRC32C,
};

class Checksum {
public:
  static uint32_t init(ChecksumType type) {
    switch (type) {
    case CHECKSUM_NONE:
      return 0;
    case CHECKSUM_CRC32:
      return 0xffffffff;
    case CHECKSUM_CRC32C:
      return 0xffffffff;
    }
    return 0;
  }

  static void update(ChecksumType type, uint32_t & value, const void * buff, uint32_t length) {
    switch (type) {
    case CHECKSUM_NONE:
      return;
    case CHECKSUM_CRC32:
      value = crc32_sb8(value, (const uint8_t *)buff, length);
      return;
    case CHECKSUM_CRC32C:
      value = crc32c_sb8(value, (const uint8_t *)buff, length);
      return;
    }
    return;
  }

  static uint32_t getValue(ChecksumType type, uint32_t value) {
    switch (type) {
    case CHECKSUM_NONE:
      return 0;
    case CHECKSUM_CRC32:
    case CHECKSUM_CRC32C:
      return ~value;
    }
    return 0;
  }
};

} // namespace NativeTask

#endif /* CHECKSUM_H_ */
