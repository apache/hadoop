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

#include <iostream>
#include "NativeTask.h"

using namespace NativeTask;

namespace Custom {
using namespace std;

inline uint32_t bswap(uint32_t val) {
  __asm__("bswap %0" : "=r" (val) : "0" (val));
  return val;
}

int32_t ReadInt(const char * src) {
  return (int32_t) bswap(*(uint32_t*) src);
}

inline uint64_t bswap64(uint64_t val) {
#ifdef __X64
  __asm__("bswapq %0" : "=r" (val) : "0" (val));
#else

  uint64_t lower = val & 0xffffffffU;
  uint32_t higher = (val >> 32) & 0xffffffffU;

  lower = bswap(lower);
  higher = bswap(higher);

  return (lower << 32) + higher;

#endif
  return val;
}

int64_t ReadLong(const char * src) {
  return (int64_t) bswap64(*(uint64_t*) src);
}

int CustomComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int32_t src_IDa = ReadInt(src);
  int64_t src_IDb = ReadLong(src+4);
  int32_t dest_IDa = ReadInt(dest);
  int64_t dest_IDb = ReadLong(dest+4);
  if(src_IDa > dest_IDa){
    return 1;
  }
  if(src_IDa < dest_IDa){
    return -1;
  }
  if(src_IDb > dest_IDb){
    return 1;
  }
  if(src_IDb < dest_IDb){
    return -1;
  }
  return 0;
};

DEFINE_NATIVE_LIBRARY(Custom) {
  REGISTER_FUNCTION(CustomComparator,Custom);
}

}






