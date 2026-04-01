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

#include <assert.h>
#include <stddef.h>  // for size_t
#include <stdio.h>
#include <string.h>

#include "bulk_crc32.h"
#include "gcc_optimizations.h"

/**
 * Hardware-accelerated CRC32 calculation using RISC-V Zbc extension.
 * Uses carry-less multiply instructions (clmul/clmulh) for CRC32 (zlib
 * polynomial).
 */

typedef void (*crc_pipelined_func_t)(uint32_t *, uint32_t *, uint32_t *,
                                     const uint8_t *, size_t, int);
extern crc_pipelined_func_t pipelined_crc32_zlib_func;

#if defined(__riscv) && (__riscv_xlen == 64)

/**
 * Precomputed constants for CRC32 (zlib polynomial) reduction using
 * carry-less multiplication. These constants are derived from the
 * polynomial 0xEDB88320 (reflected) and are used for Barrett reduction
 * or similar folding algorithms.
 */
#define RV_CRC32_CONST_R3 0x01751997d0ULL
#define RV_CRC32_CONST_R4 0x00ccaa009eULL
#define RV_CRC32_CONST_R5 0x0163cd6124ULL
#define RV_CRC32_MASK32 0x00000000FFFFFFFFULL
#define RV_CRC32_POLY_TRUE_LE_FULL 0x01DB710641ULL
#define RV_CRC32_CONST_RU 0x01F7011641ULL

/**
 * Performs a 64-bit carry-less multiplication (clmul) of two values.
 * This instruction is part of the RISC-V Zbc extension.
 */
static inline uint64_t rv_clmul(uint64_t a, uint64_t b) {
  uint64_t r;
  __asm__ volatile(
      ".option push\n\t"
      ".option arch, +zbc\n\t"
      "clmul %0, %1, %2\n\t"
      ".option pop\n\t"
      : "=r"(r)
      : "r"(a), "r"(b));
  return r;
}

/**
 * Performs the high 64 bits of a 64-bit carry-less multiplication (clmulh).
 * This instruction is part of the RISC-V Zbc extension.
 */
static inline uint64_t rv_clmulh(uint64_t a, uint64_t b) {
  uint64_t r;
  __asm__ volatile(
      ".option push\n\t"
      ".option arch, +zbc\n\t"
      "clmulh %0, %1, %2\n\t"
      ".option pop\n\t"
      : "=r"(r)
      : "r"(a), "r"(b));
  return r;
}

/**
 * Fallback bitwise implementation of CRC32 (zlib) for small data chunks
 * or to handle misaligned data at the beginning/end of a buffer.
 */
static inline uint32_t rv_crc32_zlib_bitwise(uint32_t crc, const uint8_t *buf,
                                             size_t len) {
  uint32_t c = crc;
  for (size_t i = 0; i < len; ++i) {
    c ^= buf[i];
    for (int k = 0; k < 8; ++k) {
      uint32_t mask = -(int32_t)(c & 1);
      c = (c >> 1) ^ (0xEDB88320U & mask);  // reflected polynomial
    }
  }
  return c;
}

/**
 * Hardware-accelerated CRC32 (zlib) calculation using RISC-V Zbc
 * carry-less multiplication instructions.
 */
static uint32_t rv_crc32_zlib_clmul(uint32_t crc, const uint8_t *buf,
                                    size_t len) {
  const uint8_t *p = buf;
  size_t n = len;

  if (n < 32) {
    return rv_crc32_zlib_bitwise(crc, p, n);
  }

  // Handle misaligned data at the start. This is considered unlikely
  // in typical Hadoop usage but necessary for correctness.
  uintptr_t mis = (uintptr_t)p & 0xF;
  if (unlikely(mis)) {
    size_t pre = 16 - mis;
    if (pre > n) pre = n;
    crc = rv_crc32_zlib_bitwise(crc, p, pre);
    p += pre;
    n -= pre;
  }

  uint64_t x0 = *(const uint64_t *)(const void *)(p + 0);
  uint64_t x1 = *(const uint64_t *)(const void *)(p + 8);
  x0 ^= (uint64_t)crc;
  p += 16;
  n -= 16;

  const uint64_t C1 = RV_CRC32_CONST_R3;
  const uint64_t C2 = RV_CRC32_CONST_R4;

  // Main loop: process 16 bytes of aligned data per iteration using
  // carry-less multiplication for high-performance folding.
  while (likely(n >= 16)) {
    uint64_t tL = rv_clmul(C2, x1);
    uint64_t tH = rv_clmulh(C2, x1);
    uint64_t yL = rv_clmul(C1, x0);
    uint64_t yH = rv_clmulh(C1, x0);
    x0 = yL ^ tL;
    x1 = yH ^ tH;

    uint64_t d0 = *(const uint64_t *)(const void *)(p + 0);
    uint64_t d1 = *(const uint64_t *)(const void *)(p + 8);
    x0 ^= d0;
    x1 ^= d1;
    p += 16;
    n -= 16;
  }

  // Final reduction and folding of the remaining 16 bytes in the pipeline.
  {
    uint64_t tH = rv_clmulh(x0, C2);
    uint64_t tL = rv_clmul(x0, C2);
    x0 = x1 ^ tL;
    x1 = tH;
  }

  uint64_t hi = x1;
  uint64_t lo = x0;
  uint64_t t2 = (lo >> 32) | (hi << 32);
  lo &= RV_CRC32_MASK32;

  lo = rv_clmul(RV_CRC32_CONST_R5, lo) ^ t2;
  uint64_t tmp = lo;
  lo &= RV_CRC32_MASK32;
  lo = rv_clmul(lo, RV_CRC32_CONST_RU);
  lo &= RV_CRC32_MASK32;
  lo = rv_clmul(lo, RV_CRC32_POLY_TRUE_LE_FULL) ^ tmp;

  uint32_t c = (uint32_t)(lo >> 32);

  // Handle any remaining bytes (less than 16) using bitwise fallback.
  if (n) {
    c = rv_crc32_zlib_bitwise(c, p, n);
  }
  return c;
}

/**
 * Pipelined version of hardware-accelerated CRC32 calculation using
 * RISC-V Zbc carry-less multiply instructions.
 *
 *   crc1, crc2, crc3 : Store initial checksum for each block before
 *           calling. When it returns, updated checksums are stored.
 *   p_buf : The base address of the data buffer. The buffer should be
 *           at least as big as block_size * num_blocks.
 *   block_size : The size of each block in bytes.
 *   num_blocks : The number of blocks to work on. Valid values are 1, 2, or 3.
 *                A value of 0 is treated as a no-op. Any other value will
 *                trigger an assertion in debug builds.
 */
static void pipelined_crc32_zlib(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3,
                                 const uint8_t *p_buf, size_t block_size,
                                 int num_blocks) {
  const uint8_t *p1 = p_buf;
  const uint8_t *p2 = p_buf + block_size;
  const uint8_t *p3 = p_buf + 2 * block_size;

  switch (num_blocks) {
    case 3:
      *crc3 = rv_crc32_zlib_clmul(*crc3, p3, block_size);
      // fall through
    case 2:
      *crc2 = rv_crc32_zlib_clmul(*crc2, p2, block_size);
      // fall through
    case 1:
      *crc1 = rv_crc32_zlib_clmul(*crc1, p1, block_size);
      break;
    case 0:
      return;
    default:
      assert(0 && "BUG: Invalid number of checksum blocks");
  }
}

#endif  // __riscv && __riscv_xlen==64

/**
 * On library load, determine what sort of crc we are going to do
 * and set crc function pointers appropriately.
 */
void __attribute__((constructor)) init_cpu_support_flag(void) {
#if defined(__riscv) && (__riscv_xlen == 64)
  // check if CPU supports Zbc.
  // parse /proc/cpuinfo 'isa' line for substring "zbc".
  FILE *f = fopen("/proc/cpuinfo", "r");
  if (f) {
    char line[256];
    int has_zbc = 0;
    while (fgets(line, sizeof(line), f)) {
      if ((strstr(line, "isa") || strstr(line, "extensions")) &&
          strstr(line, "zbc")) {
        has_zbc = 1;
        break;
      }
    }
    fclose(f);
    if (has_zbc) {
      pipelined_crc32_zlib_func = pipelined_crc32_zlib;
    }
  }
#endif
}
