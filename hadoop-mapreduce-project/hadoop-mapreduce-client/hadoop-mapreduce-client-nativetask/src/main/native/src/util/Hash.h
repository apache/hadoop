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

#ifndef HASH_H_
#define HASH_H_

#include <stdint.h>
#include <stdlib.h>

// Hash function for a byte array.
extern uint64_t CityHash64(const char *buf, size_t len);

// Hash function for a byte array.  For convenience, a 64-bit seed is also
// hashed into the result.
extern uint64_t CityHash64WithSeed(const char *buf, size_t len, uint64_t seed);

namespace NativeTask {

class Hash {
public:
  /**
   * Compatible with hadoop Text & BytesWritable hash
   */
  inline static int32_t BytesHash(const char * bytes, uint32_t length) {
    int32_t hash = 1;
    for (uint32_t i = 0; i < length; i++)
      hash = (31 * hash) + (int32_t)bytes[i];
    return hash;
  }

  /**
   * Unsigned version of BytesHash
   */
  inline static uint32_t BytesHashU(const char * bytes, uint32_t length) {
    uint32_t hash = 1;
    for (uint32_t i = 0; i < length; i++)
      hash = (31U * hash) + (uint32_t)bytes[i];
    return hash;
  }

  /**
   * City hash, faster for longer input
   */
  inline static uint64_t CityHash(const char * bytes, uint32_t length) {
    return CityHash64(bytes, length);
  }

  /**
   * City hash, faster for longer input
   */
  inline static uint64_t CityHashWithSeed(const char * bytes, uint32_t length, uint64_t seed) {
    return CityHash64WithSeed(bytes, length, seed);
  }
};

} // namespace NativeTask

#endif /* HASH_H_ */
