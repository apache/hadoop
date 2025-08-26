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
#include <setjmp.h>
#include <stddef.h>  // for size_t
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#ifdef __linux__
#include <signal.h>   // struct sigaction
#include <strings.h>  // strcasecmp
#include <sys/auxv.h>
#include <sys/types.h>
#include <unistd.h>
#endif
#include <stdlib.h>  // getenv

#include "bulk_crc32.h"
#include "gcc_optimizations.h"

// Forward declare the tables to avoid including the headers that cause symbol
// conflicts
extern const uint32_t CRC32_T8_0[256];
extern const uint32_t CRC32C_T8_0[256];

///////////////////////////////////////////////////////////////////////////
// Begin code for RISC-V Zbc (CLMUL) hardware support of CRC32/CRC32C
//
// Requirements / hints:
//   - GCC >= 10 recommended (RISC-V Zbc inline asm support)
//   - Suggested ISA: -march=rv64gc_zba_zbb_zbc
//   - Suggested flags: -O3 -march=rv64gc_zba_zbb_zbc -mtune=native
//   - Runtime opt-in: set HADOOP_ENABLE_RISCV_CRC=1 to enable HW path
///////////////////////////////////////////////////////////////////////////

// ------------------------------
// CLMUL constants used by the Barrett-reduction style folding
// (for little-endian reflected polynomials)
#define CLMUL_MIN_LEN 32
#define CLMUL_CHUNK_LEN 16
#define CONSTANT_R3 0x1751997d0ULL  // 128->64 folding multiplier
#define CONSTANT_R4 0x0ccaa009eULL  // 128->64 folding multiplier
#define CONSTANT_R5 0x163cd6124ULL  // Barrett reduction helper
#define MASK32 0xFFFFFFFFULL
#define CRCPOLY_TRUE_LE_FULL 0x1DB710641ULL
#define CONSTANT_RU 0x1F7011641ULL

// CRC32C (Castagnoli) polynomial constants
#define CRC32C_CONSTANT_R3 0x740eef02ULL
#define CRC32C_CONSTANT_R4 0x9e4addf8ULL
#define CRC32C_CONSTANT_R5 0xba4fc28eULL
#define CRC32C_CRCPOLY_TRUE_LE_FULL 0x105ec76f0ULL
#define CRC32C_CONSTANT_RU 0x1f7011641ULL  // shared with zlib variant

// Safe 64-bit load: avoid potential faults/penalties on unaligned access
static inline uint64_t load64_aligned_or_copy(const void *p) {
  uint64_t v;
  if (((uintptr_t)p & 7) == 0) {
    return *(const uint64_t *)p;
  }
  memcpy(&v, p, sizeof(v));
  return v;
}

// Zbc clmul helper, avoid fixed register names in inline asm
static inline uint64_t clmul64_lo(uint64_t a, uint64_t b) {
  uint64_t r;
  __asm__ __volatile__("clmul %0, %1, %2" : "=r"(r) : "r"(a), "r"(b));
  return r;
}

/**
 * Generic CRC32 table-lookup implementation (fallback path)
 */
static uint32_t crc32_generic_zlib(uint32_t crc, const uint8_t *buf,
                                   size_t len) {
  while (len--) {
    crc = CRC32_T8_0[(crc ^ *buf++) & 0xFF] ^ (crc >> 8);
  }
  return crc;
}

static uint32_t crc32_generic_castagnoli(uint32_t crc, const uint8_t *buf,
                                         size_t len) {
  while (len--) {
    crc = CRC32C_T8_0[(crc ^ *buf++) & 0xFF] ^ (crc >> 8);
  }
  return crc;
}

/**
 * RISC-V CLMUL-optimized CRC32 (zlib polynomial)
 * Algorithm adapted from riscv-crc32-clmul
 */
static uint32_t crc32_riscv_zlib(uint32_t crc, const uint8_t *buf, size_t len)
    __attribute__((unused));
static uint32_t crc32_riscv_zlib(uint32_t crc, const uint8_t *buf, size_t len) {
  // Small inputs: use SW table fallback
  if (len < CLMUL_MIN_LEN) {
    return crc32_generic_zlib(crc, buf, len);
  }

  // Peel until 16-byte alignment or remaining < 16
  while (len && ((uintptr_t)buf & (CLMUL_CHUNK_LEN - 1))) {
    crc = CRC32_T8_0[(crc ^ *buf++) & 0xFF] ^ (crc >> 8);
    --len;
  }
  if (len < CLMUL_MIN_LEN) {
    return crc32_generic_zlib(crc, buf, len);
  }

  // Invert initial CRC
  crc ^= 0xFFFFFFFFu;

  // Read the first 16 bytes
  uint64_t t0 = load64_aligned_or_copy(buf);
  uint64_t t1 = load64_aligned_or_copy(buf + 8);
  t0 ^= crc;  // fold initial CRC
  buf += 16;
  len -= 16;

  // Constants
  const uint64_t cR3 = CONSTANT_R3;
  const uint64_t cR4 = CONSTANT_R4;

  while (len >= 16) {
    uint64_t d0 = load64_aligned_or_copy(buf);
    uint64_t d1 = load64_aligned_or_copy(buf + 8);
    uint64_t t_high1, t_high0;
    // Fold (t0,t1) with (d0,d1)
    __asm__ __volatile__(
        "clmulh %2, %5, %1\n\t"
        "clmul  %3, %5, %1\n\t"
        "clmulh %1, %4, %0\n\t"
        "clmul  %0, %4, %0\n\t"
        "xor    %0, %0, %3\n\t"
        "xor    %1, %1, %2\n\t"
        "xor    %0, %0, %6\n\t"
        "xor    %1, %1, %7\n\t"
        : "+r"(t0), "+r"(t1), "=&r"(t_high1), "=&r"(t_high0)
        : "r"(cR3), "r"(cR4), "r"(d0), "r"(d1)
        : "memory");
    buf += 16;
    len -= 16;
  }

  // Remainder (<16B): optionally fold once more, then finish in SW
  if (len) {
    // Optional: one more fold, then Barrett; tail via table lookup
  }

  // Final 64-bit fold
  {
    uint64_t t0_orig = t0;
    uint64_t t2;
    __asm__ __volatile__(
        "clmulh %1, %0, %3\n\t"
        "clmul  %0, %0, %3\n\t"
        "xor    %0, %0, %4\n\t"
        "mv     %2, %1\n\t"
        : "+r"(t0), "=&r"(t2), "+r"(t1)
        : "r"(cR4), "r"(t0_orig)
        : "memory");
  }

  // Barrett reduction
  uint32_t crc32_result;
  {
    const uint64_t cR5 = CONSTANT_R5;
    const uint64_t cRU = CONSTANT_RU;
    const uint64_t cPoly = CRCPOLY_TRUE_LE_FULL;

    uint64_t tmp = (t0 >> 32) | (t1 << 32);
    uint64_t lo = clmul64_lo(t0 & MASK32, cR5) ^ tmp;
    uint64_t saved = lo;
    lo = clmul64_lo(lo & MASK32, cRU);
    lo &= MASK32;
    lo = clmul64_lo(lo, cPoly) ^ saved;
    crc32_result = (uint32_t)(lo >> 32);
  }
  crc32_result ^= 0xFFFFFFFFu;

  // Process remaining tail (<16B)
  if (len) {
    return crc32_generic_zlib(crc32_result, buf, len);
  }
  return crc32_result;
}

/**
 * RISC-V CLMUL-optimized CRC32C (Castagnoli polynomial)
 * Algorithm adapted from riscv-crc32-clmul
 */
static uint32_t crc32_riscv_castagnoli(uint32_t crc, const uint8_t *buf,
                                       size_t len) __attribute__((unused));
static uint32_t crc32_riscv_castagnoli(uint32_t crc, const uint8_t *buf,
                                       size_t len) {
  if (len < CLMUL_MIN_LEN) {
    return crc32_generic_castagnoli(crc, buf, len);
  }
  while (len && ((uintptr_t)buf & (CLMUL_CHUNK_LEN - 1))) {
    crc = CRC32C_T8_0[(crc ^ *buf++) & 0xFF] ^ (crc >> 8);
    --len;
  }
  if (len < CLMUL_MIN_LEN) {
    return crc32_generic_castagnoli(crc, buf, len);
  }
  crc ^= 0xFFFFFFFFu;
  uint64_t t0 = load64_aligned_or_copy(buf);
  uint64_t t1 = load64_aligned_or_copy(buf + 8);
  t0 ^= crc;
  buf += 16;
  len -= 16;
  const uint64_t cR3 = CRC32C_CONSTANT_R3;
  const uint64_t cR4 = CRC32C_CONSTANT_R4;
  while (len >= 16) {
    uint64_t d0 = load64_aligned_or_copy(buf);
    uint64_t d1 = load64_aligned_or_copy(buf + 8);
    uint64_t th1, th0;
    __asm__ __volatile__(
        "clmulh %2, %5, %1\n\t"
        "clmul  %3, %5, %1\n\t"
        "clmulh %1, %4, %0\n\t"
        "clmul  %0, %4, %0\n\t"
        "xor    %0, %0, %3\n\t"
        "xor    %1, %1, %2\n\t"
        "xor    %0, %0, %6\n\t"
        "xor    %1, %1, %7\n\t"
        : "+r"(t0), "+r"(t1), "=&r"(th1), "=&r"(th0)
        : "r"(cR3), "r"(cR4), "r"(d0), "r"(d1)
        : "memory");
    buf += 16;
    len -= 16;
  }
  {
    uint64_t t0_orig = t0, t2;
    __asm__ __volatile__(
        "clmulh %1, %0, %3\n\t"
        "clmul  %0, %0, %3\n\t"
        "xor    %0, %0, %4\n\t"  // t0 ^= t0_orig   <-- 修正：从 %2 改为 %4
        "mv     %2, %1\n\t"
        : "+r"(t0), "=&r"(t2), "+r"(t1)
        : "r"(cR4), "r"(t0_orig)
        : "memory");
  }
  // Barrett reduction (Castagnoli polynomial)
  uint32_t crc32_result;
  {
    const uint64_t cR5 = CRC32C_CONSTANT_R5;
    const uint64_t cRU = CRC32C_CONSTANT_RU;
    const uint64_t cPoly = CRC32C_CRCPOLY_TRUE_LE_FULL;

    uint64_t tmp = (t0 >> 32) | (t1 << 32);
    uint64_t lo = clmul64_lo(t0 & MASK32, cR5) ^ tmp;
    uint64_t saved = lo;
    lo = clmul64_lo(lo & MASK32, cRU);
    lo &= MASK32;
    lo = clmul64_lo(lo, cPoly) ^ saved;
    crc32_result = (uint32_t)(lo >> 32);
  }
  crc32_result ^= 0xFFFFFFFFu;
  if (len) {
    return crc32_generic_castagnoli(crc32_result, buf, len);
  }
  return crc32_result;
}

// ----------------------------------------------------------------------
// Inline helpers for interleaved (pipelined) multi-block processing
// ----------------------------------------------------------------------
static inline void fold_block16_zlib(uint64_t *t0, uint64_t *t1, uint64_t d0,
                                     uint64_t d1, uint64_t cR3, uint64_t cR4) {
  uint64_t th1, th0;
  __asm__ __volatile__(
      "clmulh %2, %5, %1\n\t"
      "clmul  %3, %5, %1\n\t"
      "clmulh %1, %4, %0\n\t"
      "clmul  %0, %4, %0\n\t"
      "xor    %0, %0, %3\n\t"
      "xor    %1, %1, %2\n\t"
      "xor    %0, %0, %6\n\t"
      "xor    %1, %1, %7\n\t"
      : "+r"(*t0), "+r"(*t1), "=&r"(th1), "=&r"(th0)
      : "r"(cR3), "r"(cR4), "r"(d0), "r"(d1)
      : "memory");
}

static inline uint32_t finalize_crc_zlib(uint64_t t0, uint64_t t1,
                                         uint64_t cR4) {
  // Final 64-bit fold
  uint64_t t0_orig = t0, t2;
  __asm__ __volatile__(
      "clmulh %1, %0, %3\n\t"
      "clmul  %0, %0, %3\n\t"
      "xor    %0, %0, %4\n\t"
      "mv     %2, %1\n\t"
      : "+r"(t0), "=&r"(t2), "+r"(t1)
      : "r"(cR4), "r"(t0_orig)
      : "memory");

  // Barrett reduction
  const uint64_t cR5 = CONSTANT_R5;
  const uint64_t cRU = CONSTANT_RU;
  const uint64_t cPoly = CRCPOLY_TRUE_LE_FULL;
  uint64_t tmp = (t0 >> 32) | (t1 << 32);
  uint64_t lo = clmul64_lo(t0 & MASK32, cR5) ^ tmp;
  uint64_t saved = lo;
  lo = clmul64_lo(lo & MASK32, cRU);
  lo &= MASK32;
  lo = clmul64_lo(lo, cPoly) ^ saved;
  return (uint32_t)(lo >> 32) ^ 0xFFFFFFFFu;
}

static inline void fold_block16_c(uint64_t *t0, uint64_t *t1, uint64_t d0,
                                  uint64_t d1, uint64_t cR3, uint64_t cR4) {
  uint64_t th1, th0;
  __asm__ __volatile__(
      "clmulh %2, %5, %1\n\t"
      "clmul  %3, %5, %1\n\t"
      "clmulh %1, %4, %0\n\t"
      "clmul  %0, %4, %0\n\t"
      "xor    %0, %0, %3\n\t"
      "xor    %1, %1, %2\n\t"
      "xor    %0, %0, %6\n\t"
      "xor    %1, %1, %7\n\t"
      : "+r"(*t0), "+r"(*t1), "=&r"(th1), "=&r"(th0)
      : "r"(cR3), "r"(cR4), "r"(d0), "r"(d1)
      : "memory");
}

static inline uint32_t finalize_crc_c(uint64_t t0, uint64_t t1, uint64_t cR4) {
  // Final 64-bit fold
  uint64_t t0_orig = t0, t2;
  __asm__ __volatile__(
      "clmulh %1, %0, %3\n\t"
      "clmul  %0, %0, %3\n\t"
      "xor    %0, %0, %4\n\t"
      "mv     %2, %1\n\t"
      : "+r"(t0), "=&r"(t2), "+r"(t1)
      : "r"(cR4), "r"(t0_orig)
      : "memory");

  // Barrett reduction (CRC32C)
  const uint64_t cR5 = CRC32C_CONSTANT_R5;
  const uint64_t cRU = CRC32C_CONSTANT_RU;
  const uint64_t cPoly = CRC32C_CRCPOLY_TRUE_LE_FULL;
  uint64_t tmp = (t0 >> 32) | (t1 << 32);
  uint64_t lo = clmul64_lo(t0 & MASK32, cR5) ^ tmp;
  uint64_t saved = lo;
  lo = clmul64_lo(lo & MASK32, cRU);
  lo &= MASK32;
  lo = clmul64_lo(lo, cPoly) ^ saved;
  return (uint32_t)(lo >> 32) ^ 0xFFFFFFFFu;
}

/**
 * Pipelined CRC32C using RISC-V Zbc CLMUL (process 1-3 blocks)
 *
 *   - crc1, crc2, crc3: per-block initial/output checksums
 *   - p_buf: data buffer
 *   - block_size: bytes per block
 *   - num_blocks: number of blocks (1..3)
 */
static void pipelined_crc32c_riscv(uint32_t *crc1, uint32_t *crc2,
                                   uint32_t *crc3, const uint8_t *p_buf,
                                   size_t block_size, int num_blocks) {
  assert(num_blocks >= 1 && num_blocks <= 3 && "invalid num_blocks");
  // Small blocks: use single-block fallback for simplicity
  if (block_size < CLMUL_MIN_LEN) {
    *crc1 = crc32_riscv_castagnoli(*crc1, p_buf, block_size);
    if (num_blocks >= 2) {
      *crc2 = crc32_riscv_castagnoli(*crc2, p_buf + block_size, block_size);
    }
    if (num_blocks >= 3) {
      *crc3 = crc32_riscv_castagnoli(*crc3, p_buf + 2 * block_size, block_size);
    }
    return;
  }

  // Per-block state
  const uint64_t cR3 = CRC32C_CONSTANT_R3;
  const uint64_t cR4 = CRC32C_CONSTANT_R4;

  const uint8_t *b0 = p_buf;
  const uint8_t *b1 = p_buf + (num_blocks >= 2 ? block_size : 0);
  const uint8_t *b2 = p_buf + (num_blocks >= 3 ? 2 * block_size : 0);

  size_t r0 = block_size, r1 = (num_blocks >= 2 ? block_size : 0),
         r2 = (num_blocks >= 3 ? block_size : 0);

  uint64_t t0_0 = 0, t1_0 = 0, t0_1 = 0, t1_1 = 0, t0_2 = 0, t1_2 = 0;

  // Preload head and fold initial CRC for each active block
  uint32_t c0 = *crc1 ^ 0xFFFFFFFFu;
  t0_0 = load64_aligned_or_copy(b0) ^ c0;
  t1_0 = load64_aligned_or_copy(b0 + 8);
  b0 += 16;
  r0 -= 16;

  uint32_t c1v = 0, c2v = 0;
  if (num_blocks >= 2) {
    c1v = *crc2 ^ 0xFFFFFFFFu;
    t0_1 = load64_aligned_or_copy(b1) ^ c1v;
    t1_1 = load64_aligned_or_copy(b1 + 8);
    b1 += 16;
    r1 -= 16;
  }
  if (num_blocks >= 3) {
    c2v = *crc3 ^ 0xFFFFFFFFu;
    t0_2 = load64_aligned_or_copy(b2) ^ c2v;
    t1_2 = load64_aligned_or_copy(b2 + 8);
    b2 += 16;
    r2 -= 16;
  }

  // Interleave 16B folds across blocks (RR) to increase ILP
  while (r0 >= 16 || r1 >= 16 || r2 >= 16) {
    if (r0 >= 16) {
      uint64_t d0 = load64_aligned_or_copy(b0);
      uint64_t d1 = load64_aligned_or_copy(b0 + 8);
      fold_block16_c(&t0_0, &t1_0, d0, d1, cR3, cR4);
      b0 += 16;
      r0 -= 16;
    }
    if (r1 >= 16) {
      uint64_t d0 = load64_aligned_or_copy(b1);
      uint64_t d1 = load64_aligned_or_copy(b1 + 8);
      fold_block16_c(&t0_1, &t1_1, d0, d1, cR3, cR4);
      b1 += 16;
      r1 -= 16;
    }
    if (r2 >= 16) {
      uint64_t d0 = load64_aligned_or_copy(b2);
      uint64_t d1 = load64_aligned_or_copy(b2 + 8);
      fold_block16_c(&t0_2, &t1_2, d0, d1, cR3, cR4);
      b2 += 16;
      r2 -= 16;
    }
  }

  // Finalize + tail per block
  uint32_t out0 = finalize_crc_c(t0_0, t1_0, cR4);
  if (r0) out0 = crc32_generic_castagnoli(out0, b0, r0);
  *crc1 = out0;

  if (num_blocks >= 2) {
    uint32_t out1 = finalize_crc_c(t0_1, t1_1, cR4);
    if (r1) out1 = crc32_generic_castagnoli(out1, b1, r1);
    *crc2 = out1;
  }
  if (num_blocks >= 3) {
    uint32_t out2 = finalize_crc_c(t0_2, t1_2, cR4);
    if (r2) out2 = crc32_generic_castagnoli(out2, b2, r2);
    *crc3 = out2;
  }
}

/**
 * Pipelined CRC32 (zlib polynomial) using RISC-V Zbc CLMUL (1-3 blocks)
 *
 *   - crc1, crc2, crc3: per-block initial/output checksums
 *   - p_buf: data buffer
 *   - block_size: bytes per block
 *   - num_blocks: number of blocks (1..3)
 */
static void pipelined_crc32_zlib_riscv(uint32_t *crc1, uint32_t *crc2,
                                       uint32_t *crc3, const uint8_t *p_buf,
                                       size_t block_size, int num_blocks) {
  assert(num_blocks >= 1 && num_blocks <= 3 && "invalid num_blocks");
  // Small blocks: use single-block fallback
  if (block_size < CLMUL_MIN_LEN) {
    *crc1 = crc32_riscv_zlib(*crc1, p_buf, block_size);
    if (num_blocks >= 2) {
      *crc2 = crc32_riscv_zlib(*crc2, p_buf + block_size, block_size);
    }
    if (num_blocks >= 3) {
      *crc3 = crc32_riscv_zlib(*crc3, p_buf + 2 * block_size, block_size);
    }
    return;
  }

  // Per-block state
  const uint64_t cR3 = CONSTANT_R3;
  const uint64_t cR4 = CONSTANT_R4;

  const uint8_t *b0 = p_buf;
  const uint8_t *b1 = p_buf + (num_blocks >= 2 ? block_size : 0);
  const uint8_t *b2 = p_buf + (num_blocks >= 3 ? 2 * block_size : 0);

  size_t r0 = block_size, r1 = (num_blocks >= 2 ? block_size : 0),
         r2 = (num_blocks >= 3 ? block_size : 0);

  uint64_t t0_0 = 0, t1_0 = 0, t0_1 = 0, t1_1 = 0, t0_2 = 0, t1_2 = 0;

  // Preload head and fold initial CRC for each active block
  uint32_t c0 = *crc1 ^ 0xFFFFFFFFu;
  t0_0 = load64_aligned_or_copy(b0) ^ c0;
  t1_0 = load64_aligned_or_copy(b0 + 8);
  b0 += 16;
  r0 -= 16;

  uint32_t c1v = 0, c2v = 0;
  if (num_blocks >= 2) {
    c1v = *crc2 ^ 0xFFFFFFFFu;
    t0_1 = load64_aligned_or_copy(b1) ^ c1v;
    t1_1 = load64_aligned_or_copy(b1 + 8);
    b1 += 16;
    r1 -= 16;
  }
  if (num_blocks >= 3) {
    c2v = *crc3 ^ 0xFFFFFFFFu;
    t0_2 = load64_aligned_or_copy(b2) ^ c2v;
    t1_2 = load64_aligned_or_copy(b2 + 8);
    b2 += 16;
    r2 -= 16;
  }

  // Interleave 16B folds across blocks (RR)
  while (r0 >= 16 || r1 >= 16 || r2 >= 16) {
    if (r0 >= 16) {
      uint64_t d0 = load64_aligned_or_copy(b0);
      uint64_t d1 = load64_aligned_or_copy(b0 + 8);
      fold_block16_zlib(&t0_0, &t1_0, d0, d1, cR3, cR4);
      b0 += 16;
      r0 -= 16;
    }
    if (r1 >= 16) {
      uint64_t d0 = load64_aligned_or_copy(b1);
      uint64_t d1 = load64_aligned_or_copy(b1 + 8);
      fold_block16_zlib(&t0_1, &t1_1, d0, d1, cR3, cR4);
      b1 += 16;
      r1 -= 16;
    }
    if (r2 >= 16) {
      uint64_t d0 = load64_aligned_or_copy(b2);
      uint64_t d1 = load64_aligned_or_copy(b2 + 8);
      fold_block16_zlib(&t0_2, &t1_2, d0, d1, cR3, cR4);
      b2 += 16;
      r2 -= 16;
    }
  }

  // Finalize + tail per block
  uint32_t out0 = finalize_crc_zlib(t0_0, t1_0, cR4);
  if (r0) out0 = crc32_generic_zlib(out0, b0, r0);
  *crc1 = out0;

  if (num_blocks >= 2) {
    uint32_t out1 = finalize_crc_zlib(t0_1, t1_1, cR4);
    if (r1) out1 = crc32_generic_zlib(out1, b1, r1);
    *crc2 = out1;
  }
  if (num_blocks >= 3) {
    uint32_t out2 = finalize_crc_zlib(t0_2, t1_2, cR4);
    if (r2) out2 = crc32_generic_zlib(out2, b2, r2);
    *crc3 = out2;
  }
}

// Function pointer types for pipelined CRC functions
typedef void (*crc_pipelined_func_t)(uint32_t *, uint32_t *, uint32_t *,
                                     const uint8_t *, size_t, int);

// External function pointers from bulk_crc32.c
extern crc_pipelined_func_t pipelined_crc32c_func;
extern crc_pipelined_func_t pipelined_crc32_zlib_func;

///////////////////////////////////////////////////////////////////////////
// HW detection and initialization
///////////////////////////////////////////////////////////////////////////

/**
 * Check /proc/cpuinfo for RISC-V Zbc support (Linux only)
 */
static int check_cpuinfo_for_zbc(void) __attribute__((unused));
static int check_cpuinfo_for_zbc(void) {
#ifdef __linux__
  FILE *fp = fopen("/proc/cpuinfo", "r");
  if (fp == NULL) {
    return 0;
  }

  char line[256];
  int found_zbc = 0;

  while (fgets(line, sizeof(line), fp)) {
    // Look for ISA line containing "zbc"
    if (strncmp(line, "isa", 3) == 0) {
      if (strstr(line, "zbc") != NULL) {
        found_zbc = 1;
        break;
      }
    }
  }

  fclose(fp);
  return found_zbc;
#else
  return 0;
#endif
}

// Global jmp env and SIGILL handler
#ifdef __linux__
static sigjmp_buf g_sigill_jmp_env;
static void crc_sigill_handler(int signo) { siglongjmp(g_sigill_jmp_env, 1); }
#endif

/**
 * Probe Zbc availability by executing CLMUL (catch SIGILL)
 * More robust than parsing /proc/cpuinfo alone.
 */
static int test_clmul_instruction(void) __attribute__((unused));
static int test_clmul_instruction(void) {
#ifdef __linux__
  struct sigaction old_action, new_action;
  int clmul_available = 0;

  memset(&new_action, 0, sizeof(new_action));
  new_action.sa_handler = crc_sigill_handler;
  sigemptyset(&new_action.sa_mask);
  new_action.sa_flags = 0;

  if (sigaction(SIGILL, &new_action, &old_action) == 0) {
    if (sigsetjmp(g_sigill_jmp_env, 1) == 0) {
      volatile uint64_t a = 1, b = 1, result = 0;
      __asm__ __volatile__("clmul %0, %1, %2\n\t"
                           : "=r"(result)
                           : "r"(a), "r"(b)
                           : "memory");
      clmul_available = 1;
    } else {
      clmul_available = 0;
    }
    sigaction(SIGILL, &old_action, NULL);
  }
  return clmul_available;
#else
  return 0;
#endif
}

/**
 * Check if RISC-V Zbc (CLMUL) is available at runtime.
 */
static int has_riscv_zbc_support(void) {
#ifdef __linux__
  // Environment gate: off by default; require "1"/"true" to enable
  const char *env = getenv("HADOOP_ENABLE_RISCV_CRC");
  if (!(env && (strcmp(env, "1") == 0 || strcasecmp(env, "true") == 0))) {
    return 0;
  }

  // Require both: cpuinfo reports zbc and CLMUL probe succeeds
  if (check_cpuinfo_for_zbc() && test_clmul_instruction()) {
    return 1;
  }
  return 0;
#else
  return 0;
#endif
}

/**
 * Library constructor: switch to HW-accelerated functions when Zbc is present.
 * Requires HADOOP_ENABLE_RISCV_CRC=1 to opt in.
 */
void __attribute__((constructor)) init_riscv_crc_support(void) {
  if (has_riscv_zbc_support()) {
    __sync_synchronize();
    pipelined_crc32c_func = pipelined_crc32c_riscv;
    pipelined_crc32_zlib_func = pipelined_crc32_zlib_riscv;
    __sync_synchronize();
#ifdef DEBUG_CRC_INIT
    fprintf(stderr,
            "RISC-V Zbc enabled via HADOOP_ENABLE_RISCV_CRC, hardware CRC "
            "acceleration active\n");
#endif
  } else {
#ifdef DEBUG_CRC_INIT
    fprintf(stderr,
            "RISC-V Zbc hardware CRC disabled (either not supported or "
            "HADOOP_ENABLE_RISCV_CRC not set)\n");
#endif
  }
}