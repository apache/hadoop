/*
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

/**
 * High performance primitive functions
 *
 **/

#ifndef PRIMITIVES_H_
#define PRIMITIVES_H_

#include <stddef.h>
#include <stdint.h>
#include <assert.h>
#include <string>

#ifdef __GNUC__
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

//#define SIMPLE_MEMCPY

#if !defined(SIMPLE_MEMCPY)
#define simple_memcpy memcpy
#define simple_memcpy2 memcpy
#else

/**
 * This memcpy assumes src & dest are not overlapped,
 * and len are normally very small(<64)
 * This function is primarily optimized for x86-64 processors,
 * on which unaligned 64-bit loads and stores are cheap
 *
 * @param dest: dest buffer
 * @param src:  src buffer
 * @param len: src buffer size, must be >0
 */
inline void simple_memcpy(void * dest, const void * src, size_t len) {
  const uint8_t * src8 = (const uint8_t*)src;
  uint8_t * dest8 = (uint8_t*)dest;
  switch (len) {
    case 0:
    return;
    case 1:
    dest8[0]=src8[0];
    return;
    case 2:
    *(uint16_t*)dest8=*(const uint16_t*)src8;
    return;
    case 3:
    *(uint16_t*)dest8 = *(const uint16_t*)src8;
    dest8[2]=src8[2];
    return;
    case 4:
    *(uint32_t*)dest8 = *(const uint32_t*)src8;
    return;
  }
  if (len<8) {
    *(uint32_t*)dest8 = *(const uint32_t*)src8;
    *(uint32_t*)(dest8+len-4) = *(const uint32_t*)(src8+len-4);
    return;
  }
  if (len<128) {
    int64_t cur = (int64_t)len - 8;
    while (cur>0) {
      *(uint64_t*)(dest8+cur) = *(const uint64_t*)(src8+cur);
      cur -= 8;
    }
    *(uint64_t*)(dest8) = *(const uint64_t*)(src8);
    return;
  }
  ::memcpy(dest, src, len);
}

#endif

/**
 * little-endian to big-endian or vice versa
 */
inline uint32_t bswap(uint32_t val) {
  __asm__("bswap %0" : "=r" (val) : "0" (val));
  return val;
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

/**
 * Fast memcmp
 */
inline int64_t fmemcmp(const char * src, const char * dest, uint32_t len) {

#ifdef BUILDIN_MEMCMP
  return memcmp(src, dest, len);
#else

  const uint8_t * src8 = (const uint8_t*)src;
  const uint8_t * dest8 = (const uint8_t*)dest;
  switch (len) {
  case 0:
    return 0;
  case 1:
    return (int64_t)src8[0] - (int64_t)dest8[0];
  case 2: {
    int64_t ret = ((int64_t)src8[0] - (int64_t)dest8[0]);
    if (ret)
      return ret;
    return ((int64_t)src8[1] - (int64_t)dest8[1]);
  }
  case 3: {
    int64_t ret = ((int64_t)src8[0] - (int64_t)dest8[0]);
    if (ret)
      return ret;
    ret = ((int64_t)src8[1] - (int64_t)dest8[1]);
    if (ret)
      return ret;
    return ((int64_t)src8[2] - (int64_t)dest8[2]);
  }
  case 4: {
    return (int64_t)bswap(*(uint32_t*)src) - (int64_t)bswap(*(uint32_t*)dest);
  }
  }
  if (len < 8) {
    int64_t ret = ((int64_t)bswap(*(uint32_t*)src) - (int64_t)bswap(*(uint32_t*)dest));
    if (ret) {
      return ret;
    }
    return ((int64_t)bswap(*(uint32_t*)(src + len - 4))
        - (int64_t)bswap(*(uint32_t*)(dest + len - 4)));
  }
  uint32_t cur = 0;
  uint32_t end = len & (0xffffffffU << 3);
  while (cur < end) {
    uint64_t l = *(uint64_t*)(src8 + cur);
    uint64_t r = *(uint64_t*)(dest8 + cur);
    if (l != r) {
      l = bswap64(l);
      r = bswap64(r);
      return l > r ? 1 : -1;
    }
    cur += 8;
  }
  uint64_t l = *(uint64_t*)(src8 + len - 8);
  uint64_t r = *(uint64_t*)(dest8 + len - 8);
  if (l != r) {
    l = bswap64(l);
    r = bswap64(r);
    return l > r ? 1 : -1;
  }
  return 0;
#endif
}

inline int64_t fmemcmp(const char * src, const char * dest, uint32_t srcLen, uint32_t destLen) {
  uint32_t minlen = srcLen < destLen ? srcLen : destLen;
  int64_t ret = fmemcmp(src, dest, minlen);
  if (ret) {
    return ret;
  }
  return (int64_t)srcLen - (int64_t)destLen;
}

/**
 * Fast memory equal
 */
inline bool fmemeq(const char * src, const char * dest, uint32_t len) {
#ifdef BUILDIN_MEMCMP
  return 0 == memcmp(src, dest, len);
#else

  const uint8_t * src8 = (const uint8_t*)src;
  const uint8_t * dest8 = (const uint8_t*)dest;
  switch (len) {
  case 0:
    return true;
  case 1:
    return src8[0] == dest8[0];
  case 2:
    return *(uint16_t*)src8 == *(uint16_t*)dest8;
  case 3:
    return (*(uint16_t*)src8 == *(uint16_t*)dest8) && (src8[2] == dest8[2]);
  case 4:
    return *(uint32_t*)src8 == *(uint32_t*)dest8;
  }
  if (len < 8) {
    return (*(uint32_t*)src8 == *(uint32_t*)dest8)
        && (*(uint32_t*)(src8 + len - 4) == *(uint32_t*)(dest8 + len - 4));
  }
  uint32_t cur = 0;
  uint32_t end = len & (0xffffffff << 3);
  while (cur < end) {
    uint64_t l = *(uint64_t*)(src8 + cur);
    uint64_t r = *(uint64_t*)(dest8 + cur);
    if (l != r) {
      return false;
    }
    cur += 8;
  }
  uint64_t l = *(uint64_t*)(src8 + len - 8);
  uint64_t r = *(uint64_t*)(dest8 + len - 8);
  if (l != r) {
    return false;
  }
  return true;
#endif
}

inline bool fmemeq(const char * src, uint32_t srcLen, const char * dest, uint32_t destLen) {
  if (srcLen != destLen) {
    return false;
  }
  return fmemeq(src, dest, std::min(srcLen, destLen));
}

/**
 * Fast memory equal, reverse order
 */
inline bool frmemeq(const char * src, const char * dest, uint32_t len) {
  const uint8_t * src8 = (const uint8_t*)src;
  const uint8_t * dest8 = (const uint8_t*)dest;
  switch (len) {
  case 0:
    return true;
  case 1:
    return src8[0] == dest8[0];
  case 2:
    return *(uint16_t*)src8 == *(uint16_t*)dest8;
  case 3:
    return (src8[2] == dest8[2]) && (*(uint16_t*)src8 == *(uint16_t*)dest8);
  case 4:
    return *(uint32_t*)src8 == *(uint32_t*)dest8;
  }
  if (len < 8) {
    return (*(uint32_t*)(src8 + len - 4) == *(uint32_t*)(dest8 + len - 4))
        && (*(uint32_t*)src8 == *(uint32_t*)dest8);
  }
  int32_t cur = (int32_t)len - 8;
  while (cur > 0) {
    if (*(uint64_t*)(src8 + cur) != *(uint64_t*)(dest8 + cur)) {
      return false;
    }
    cur -= 8;
  }
  return *(uint64_t*)(src8) == *(uint64_t*)(dest8);
}

inline bool frmemeq(const char * src, const char * dest, uint32_t srcLen, uint32_t destLen) {
  if (srcLen != destLen) {
    return false;
  }
  return frmemeq(src, dest, std::min(srcLen, destLen));
}

#endif /* PRIMITIVES_H_ */
