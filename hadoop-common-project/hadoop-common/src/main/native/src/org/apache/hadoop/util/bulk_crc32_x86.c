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
 *
 * Portions of this file are from http://www.evanjones.ca/crc32c.html under
 * the BSD license:
 *   Copyright 2008,2009,2010 Massachusetts Institute of Technology.
 *   All rights reserved. Use of this source code is governed by a
 *   BSD-style license that can be found in the LICENSE file.
 */

#include <assert.h>
#include <stddef.h>    // for size_t

#include  "bulk_crc32.h"
#include "gcc_optimizations.h"
#include "gcc_optimizations.h"

///////////////////////////////////////////////////////////////////////////
// Begin code for SSE4.2 specific hardware support of CRC32C
///////////////////////////////////////////////////////////////////////////

#  define SSE42_FEATURE_BIT (1 << 20)
#  define CPUID_FEATURES 1
/**
 * Call the cpuid instruction to determine CPU feature flags.
 */
static uint32_t cpuid(uint32_t eax_in) {
  uint32_t eax, ebx, ecx, edx;
#  if defined(__PIC__) && !defined(__LP64__)
// 32-bit PIC code uses the ebx register for the base offset --
// have to save and restore it on the stack
  asm("pushl %%ebx\n\t"
      "cpuid\n\t"
      "movl %%ebx, %[ebx]\n\t"
      "popl %%ebx" : "=a" (eax), [ebx] "=r"(ebx),  "=c"(ecx), "=d"(edx) : "a" (eax_in)
      : "cc");
#  else
  asm("cpuid" : "=a" (eax), "=b"(ebx), "=c"(ecx), "=d"(edx) : "a"(eax_in)
      : "cc");
#  endif

  return ecx;
}

//
// Definitions of the SSE4.2 crc32 operations. Using these instead of
// the GCC __builtin_* intrinsics allows this code to compile without
// -msse4.2, since we do dynamic CPU detection at runtime.
//

#  ifdef __LP64__
inline uint64_t _mm_crc32_u64(uint64_t crc, uint64_t value) {
  asm("crc32q %[value], %[crc]\n" : [crc] "+r" (crc) : [value] "rm" (value));
  return crc;
}
#  endif

inline uint32_t _mm_crc32_u32(uint32_t crc, uint32_t value) {
  asm("crc32l %[value], %[crc]\n" : [crc] "+r" (crc) : [value] "rm" (value));
  return crc;
}

inline uint32_t _mm_crc32_u16(uint32_t crc, uint16_t value) {
  asm("crc32w %[value], %[crc]\n" : [crc] "+r" (crc) : [value] "rm" (value));
  return crc;
}

inline uint32_t _mm_crc32_u8(uint32_t crc, uint8_t value) {
  asm("crc32b %[value], %[crc]\n" : [crc] "+r" (crc) : [value] "rm" (value));
  return crc;
}

#  ifdef __LP64__
/**
 * Pipelined version of hardware-accelerated CRC32C calculation using
 * the 64 bit crc32q instruction. 
 * One crc32c instruction takes three cycles, but two more with no data
 * dependency can be in the pipeline to achieve something close to single 
 * instruction/cycle. Here we feed three blocks in RR.
 *
 *   crc1, crc2, crc3 : Store initial checksum for each block before
 *           calling. When it returns, updated checksums are stored.
 *   p_buf : The base address of the data buffer. The buffer should be
 *           at least as big as block_size * num_blocks.
 *   block_size : The size of each block in bytes.
 *   num_blocks : The number of blocks to work on. Min = 1, Max = 3
 */
static void pipelined_crc32c(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3, const uint8_t *p_buf, size_t block_size, int num_blocks) {
  uint64_t c1 = *crc1;
  uint64_t c2 = *crc2;
  uint64_t c3 = *crc3;
  uint64_t *data = (uint64_t*)p_buf;
  int counter = block_size / sizeof(uint64_t);
  int remainder = block_size % sizeof(uint64_t);
  uint8_t *bdata;

  /* We do switch here because the loop has to be tight in order
   * to fill the pipeline. Any other statement inside the loop
   * or inbetween crc32 instruction can slow things down. Calling
   * individual crc32 instructions three times from C also causes
   * gcc to insert other instructions inbetween.
   *
   * Do not rearrange the following code unless you have verified
   * the generated machine code is as efficient as before.
   */
  switch (num_blocks) {
    case 3:
      /* Do three blocks */
      while (likely(counter)) {
        __asm__ __volatile__(
        "crc32q (%7), %0;\n\t"
        "crc32q (%7,%6,1), %1;\n\t"
        "crc32q (%7,%6,2), %2;\n\t"
         : "=r"(c1), "=r"(c2), "=r"(c3)
         : "0"(c1), "1"(c2), "2"(c3), "r"(block_size), "r"(data)
        );
        data++;
        counter--;
      }

      /* Take care of the remainder. They are only up to seven bytes,
       * so performing byte-level crc32 won't take much time.
       */
      bdata = (uint8_t*)data;
      while (likely(remainder)) {
        __asm__ __volatile__(
        "crc32b (%7), %0;\n\t"
        "crc32b (%7,%6,1), %1;\n\t"
        "crc32b (%7,%6,2), %2;\n\t"
         : "=r"(c1), "=r"(c2), "=r"(c3)
         : "0"(c1), "1"(c2), "2"(c3), "r"(block_size), "r"(bdata)
        );
        bdata++;
        remainder--;
      }
      break;
    case 2:
      /* Do two blocks */
      while (likely(counter)) {
        __asm__ __volatile__(
        "crc32q (%5), %0;\n\t"
        "crc32q (%5,%4,1), %1;\n\t"
         : "=r"(c1), "=r"(c2) 
         : "0"(c1), "1"(c2), "r"(block_size), "r"(data)
        );
        data++;
        counter--;
      }

      bdata = (uint8_t*)data;
      while (likely(remainder)) {
        __asm__ __volatile__(
        "crc32b (%5), %0;\n\t"
        "crc32b (%5,%4,1), %1;\n\t"
         : "=r"(c1), "=r"(c2) 
         : "0"(c1), "1"(c2), "r"(block_size), "r"(bdata)
        );
        bdata++;
        remainder--;
      }
      break;
    case 1:
      /* single block */
      while (likely(counter)) {
        __asm__ __volatile__(
        "crc32q (%2), %0;\n\t"
         : "=r"(c1) 
         : "0"(c1), "r"(data)
        );
        data++;
        counter--;
      }
      bdata = (uint8_t*)data;
      while (likely(remainder)) {
        __asm__ __volatile__(
        "crc32b (%2), %0;\n\t"
         : "=r"(c1) 
         : "0"(c1), "r"(bdata)
        );
        bdata++;
        remainder--;
      }
      break;
    case 0:
      return;
    default:
      assert(0 && "BUG: Invalid number of checksum blocks");
  }

  *crc1 = c1;
  *crc2 = c2;
  *crc3 = c3;
  return;
}

# else  // 32-bit

/**
 * Pipelined version of hardware-accelerated CRC32C calculation using
 * the 32 bit crc32l instruction. 
 * One crc32c instruction takes three cycles, but two more with no data
 * dependency can be in the pipeline to achieve something close to single 
 * instruction/cycle. Here we feed three blocks in RR.
 *
 *   crc1, crc2, crc3 : Store initial checksum for each block before
 *                calling. When it returns, updated checksums are stored.
 *   data       : The base address of the data buffer. The buffer should be
 *                at least as big as block_size * num_blocks.
 *   block_size : The size of each block in bytes. 
 *   num_blocks : The number of blocks to work on. Min = 1, Max = 3
 */
static void pipelined_crc32c(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3, const uint8_t *p_buf, size_t block_size, int num_blocks) {
  uint32_t c1 = *crc1;
  uint32_t c2 = *crc2;
  uint32_t c3 = *crc3;
  int counter = block_size / sizeof(uint32_t);
  int remainder = block_size % sizeof(uint32_t);
  uint32_t *data = (uint32_t*)p_buf;
  uint8_t *bdata;

  /* We do switch here because the loop has to be tight in order
   * to fill the pipeline. Any other statement inside the loop
   * or inbetween crc32 instruction can slow things down. Calling
   * individual crc32 instructions three times from C also causes
   * gcc to insert other instructions inbetween.
   *
   * Do not rearrange the following code unless you have verified
   * the generated machine code is as efficient as before.
   */
  switch (num_blocks) {
    case 3:
      /* Do three blocks */
      while (likely(counter)) {
        __asm__ __volatile__(
        "crc32l (%7), %0;\n\t"
        "crc32l (%7,%6,1), %1;\n\t"
        "crc32l (%7,%6,2), %2;\n\t"
         : "=r"(c1), "=r"(c2), "=r"(c3)
         : "r"(c1), "r"(c2), "r"(c3), "r"(block_size), "r"(data)
        );
        data++;
        counter--;
      }
      /* Take care of the remainder. They are only up to three bytes,
       * so performing byte-level crc32 won't take much time.
       */
      bdata = (uint8_t*)data;
      while (likely(remainder)) {
        __asm__ __volatile__(
        "crc32b (%7), %0;\n\t"
        "crc32b (%7,%6,1), %1;\n\t"
        "crc32b (%7,%6,2), %2;\n\t"
         : "=r"(c1), "=r"(c2), "=r"(c3)
         : "r"(c1), "r"(c2), "r"(c3), "r"(block_size), "r"(bdata)
        );
        bdata++;
        remainder--;
      }
      break;
    case 2:
      /* Do two blocks */
      while (likely(counter)) {
        __asm__ __volatile__(
        "crc32l (%5), %0;\n\t"
        "crc32l (%5,%4,1), %1;\n\t"
         : "=r"(c1), "=r"(c2) 
         : "r"(c1), "r"(c2), "r"(block_size), "r"(data)
        );
        data++;
        counter--;
      }

      bdata = (uint8_t*)data;
      while (likely(remainder)) {
        __asm__ __volatile__(
        "crc32b (%5), %0;\n\t"
        "crc32b (%5,%4,1), %1;\n\t"
         : "=r"(c1), "=r"(c2) 
         : "r"(c1), "r"(c2), "r"(block_size), "r"(bdata)
        );
        bdata++;
        remainder--;
      }
      break;
    case 1:
      /* single block */
      while (likely(counter)) {
        __asm__ __volatile__(
        "crc32l (%2), %0;\n\t"
         : "=r"(c1) 
         : "r"(c1), "r"(data)
        );
        data++;
        counter--;
      }
      bdata = (uint8_t*)data;
      while (likely(remainder)) {
        __asm__ __volatile__(
        "crc32b (%2), %0;\n\t"
         : "=r"(c1) 
         : "r"(c1), "r"(bdata)
        );
        bdata++;
        remainder--;
      }
      break;
    case 0:
       return;
    default:
      assert(0 && "BUG: Invalid number of checksum blocks");
  }

  *crc1 = c1;
  *crc2 = c2;
  *crc3 = c3;
  return;
}

# endif // 64-bit vs 32-bit

/**
 * On library load, initiailize the cached function pointer
 * if cpu supports SSE4.2's crc32 instruction.
 */
typedef void (*crc_pipelined_func_t)(uint32_t *, uint32_t *, uint32_t *, const uint8_t *, size_t, int);
extern crc_pipelined_func_t pipelined_crc32c_func;

void __attribute__ ((constructor)) init_cpu_support_flag(void) {
  uint32_t ecx = cpuid(CPUID_FEATURES);
  if (ecx & SSE42_FEATURE_BIT) pipelined_crc32c_func = pipelined_crc32c;
}
