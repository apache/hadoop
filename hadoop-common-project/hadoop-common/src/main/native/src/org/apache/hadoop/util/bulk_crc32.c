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

#include "org_apache_hadoop.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>

#ifdef UNIX
#include <arpa/inet.h>
#include <unistd.h>
#endif // UNIX

#include "crc32_zlib_polynomial_tables.h"
#include "crc32c_tables.h"
#include "bulk_crc32.h"
#include "gcc_optimizations.h"

#if (!defined(__FreeBSD__) && !defined(WINDOWS))
#define USE_PIPELINED
#endif

#define CRC_INITIAL_VAL 0xffffffff

typedef uint32_t (*crc_update_func_t)(uint32_t, const uint8_t *, size_t);
static uint32_t crc_val(uint32_t crc);
static uint32_t crc32_zlib_sb8(uint32_t crc, const uint8_t *buf, size_t length);
static uint32_t crc32c_sb8(uint32_t crc, const uint8_t *buf, size_t length);

#ifdef USE_PIPELINED
static void pipelined_crc32c(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3, const uint8_t *p_buf, size_t block_size, int num_blocks);
#endif
static int cached_cpu_supports_crc32; // initialized by constructor below
static uint32_t crc32c_hardware(uint32_t crc, const uint8_t* data, size_t length);

int bulk_calculate_crc(const uint8_t *data, size_t data_len,
                    uint32_t *sums, int checksum_type,
                    int bytes_per_checksum) {
  uint32_t crc;
  crc_update_func_t crc_update_func;

  switch (checksum_type) {
    case CRC32_ZLIB_POLYNOMIAL:
      crc_update_func = crc32_zlib_sb8;
      break;
    case CRC32C_POLYNOMIAL:
      crc_update_func = crc32c_sb8;
      break;
    default:
      return -EINVAL;
      break;
  }
  while (likely(data_len > 0)) {
    int len = likely(data_len >= bytes_per_checksum) ? bytes_per_checksum : data_len;
    crc = CRC_INITIAL_VAL;
    crc = crc_update_func(crc, data, len);
    *sums = ntohl(crc_val(crc));
    data += len;
    data_len -= len;
    sums++;
  }
  return 0;
}

int bulk_verify_crc(const uint8_t *data, size_t data_len,
                    const uint32_t *sums, int checksum_type,
                    int bytes_per_checksum,
                    crc32_error_t *error_info) {

#ifdef USE_PIPELINED
  uint32_t crc1, crc2, crc3;
  int n_blocks = data_len / bytes_per_checksum;
  int remainder = data_len % bytes_per_checksum;
  int do_pipelined = 0;
#endif
  uint32_t crc;
  crc_update_func_t crc_update_func;
  switch (checksum_type) {
    case CRC32_ZLIB_POLYNOMIAL:
      crc_update_func = crc32_zlib_sb8;
      break;
    case CRC32C_POLYNOMIAL:
      if (likely(cached_cpu_supports_crc32)) {
        crc_update_func = crc32c_hardware;
#ifdef USE_PIPELINED
        do_pipelined = 1;
#endif
      } else {
        crc_update_func = crc32c_sb8;
      }
      break;
    default:
      return INVALID_CHECKSUM_TYPE;
  }

#ifdef USE_PIPELINED
  if (do_pipelined) {
    /* Process three blocks at a time */
    while (likely(n_blocks >= 3)) {
      crc1 = crc2 = crc3 = CRC_INITIAL_VAL;
      pipelined_crc32c(&crc1, &crc2, &crc3, data, bytes_per_checksum, 3);

      crc = ntohl(crc_val(crc1));
      if ((crc = ntohl(crc_val(crc1))) != *sums)
        goto return_crc_error;
      sums++;
      data += bytes_per_checksum;
      if ((crc = ntohl(crc_val(crc2))) != *sums)
        goto return_crc_error;
      sums++;
      data += bytes_per_checksum;
      if ((crc = ntohl(crc_val(crc3))) != *sums)
        goto return_crc_error;
      sums++;
      data += bytes_per_checksum;
      n_blocks -= 3;
    }

    /* One or two blocks */
    if (n_blocks) {
      crc1 = crc2 = crc3 = CRC_INITIAL_VAL;
      pipelined_crc32c(&crc1, &crc2, &crc3, data, bytes_per_checksum, n_blocks);

      if ((crc = ntohl(crc_val(crc1))) != *sums)
        goto return_crc_error;
      data += bytes_per_checksum;
      sums++;
      if (n_blocks == 2) {
        if ((crc = ntohl(crc_val(crc2))) != *sums)
          goto return_crc_error;
        sums++;
        data += bytes_per_checksum;
      }
    }
 
    /* For something smaller than a block */
    if (remainder) {
      crc1 = crc2 = crc3 = CRC_INITIAL_VAL;
      pipelined_crc32c(&crc1, &crc2, &crc3, data, remainder, 1);

      if ((crc = ntohl(crc_val(crc1))) != *sums)
        goto return_crc_error;
    }
    return CHECKSUMS_VALID;
  }
#endif

  while (likely(data_len > 0)) {
    int len = likely(data_len >= bytes_per_checksum) ? bytes_per_checksum : data_len;
    crc = CRC_INITIAL_VAL;
    crc = crc_update_func(crc, data, len);
    crc = ntohl(crc_val(crc));
    if (unlikely(crc != *sums)) {
      goto return_crc_error;
    }
    data += len;
    data_len -= len;
    sums++;
  }
  return CHECKSUMS_VALID;

return_crc_error:
  if (error_info != NULL) {
    error_info->got_crc = crc;
    error_info->expected_crc = *sums;
    error_info->bad_data = data;
  }
  return INVALID_CHECKSUM_DETECTED;
}

/**
 * Extract the final result of a CRC
 */
uint32_t crc_val(uint32_t crc) {
  return ~crc;
}

/**
 * Computes the CRC32c checksum for the specified buffer using the slicing by 8 
 * algorithm over 64 bit quantities.
 */
static uint32_t crc32c_sb8(uint32_t crc, const uint8_t *buf, size_t length) {
  uint32_t running_length = ((length)/8)*8;
  uint32_t end_bytes = length - running_length; 
  int li;
  for (li=0; li < running_length/8; li++) {
	uint32_t term1;
	uint32_t term2;
    crc ^= *(uint32_t *)buf;
    buf += 4;
    term1 = CRC32C_T8_7[crc & 0x000000FF] ^
        CRC32C_T8_6[(crc >> 8) & 0x000000FF];
    term2 = crc >> 16;
    crc = term1 ^
        CRC32C_T8_5[term2 & 0x000000FF] ^ 
        CRC32C_T8_4[(term2 >> 8) & 0x000000FF];
    term1 = CRC32C_T8_3[(*(uint32_t *)buf) & 0x000000FF] ^
        CRC32C_T8_2[((*(uint32_t *)buf) >> 8) & 0x000000FF];
    
    term2 = (*(uint32_t *)buf) >> 16;
    crc =  crc ^ 
        term1 ^    
        CRC32C_T8_1[term2  & 0x000000FF] ^  
        CRC32C_T8_0[(term2 >> 8) & 0x000000FF];  
    buf += 4;
  }
  for (li=0; li < end_bytes; li++) {
    crc = CRC32C_T8_0[(crc ^ *buf++) & 0x000000FF] ^ (crc >> 8);
  }
  return crc;    
}

/**
 * Update a CRC using the "zlib" polynomial -- what Hadoop calls CHECKSUM_CRC32
 * using slicing-by-8
 */
static uint32_t crc32_zlib_sb8(
    uint32_t crc, const uint8_t *buf, size_t length) {
  uint32_t running_length = ((length)/8)*8;
  uint32_t end_bytes = length - running_length; 
  int li;
  for (li=0; li < running_length/8; li++) {
	uint32_t term1;
	uint32_t term2;
    crc ^= *(uint32_t *)buf;
    buf += 4;
    term1 = CRC32_T8_7[crc & 0x000000FF] ^
        CRC32_T8_6[(crc >> 8) & 0x000000FF];
    term2 = crc >> 16;
    crc = term1 ^
        CRC32_T8_5[term2 & 0x000000FF] ^ 
        CRC32_T8_4[(term2 >> 8) & 0x000000FF];
    term1 = CRC32_T8_3[(*(uint32_t *)buf) & 0x000000FF] ^
        CRC32_T8_2[((*(uint32_t *)buf) >> 8) & 0x000000FF];
    
    term2 = (*(uint32_t *)buf) >> 16;
    crc =  crc ^ 
        term1 ^    
        CRC32_T8_1[term2  & 0x000000FF] ^  
        CRC32_T8_0[(term2 >> 8) & 0x000000FF];  
    buf += 4;
  }
  for (li=0; li < end_bytes; li++) {
    crc = CRC32_T8_0[(crc ^ *buf++) & 0x000000FF] ^ (crc >> 8);
  }
  return crc;    
}

///////////////////////////////////////////////////////////////////////////
// Begin code for SSE4.2 specific hardware support of CRC32C
///////////////////////////////////////////////////////////////////////////

#if (defined(__amd64__) || defined(__i386)) && defined(__GNUC__) && !defined(__FreeBSD__)
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

/**
 * On library load, initiailize the cached value above for
 * whether the cpu supports SSE4.2's crc32 instruction.
 */
void __attribute__ ((constructor)) init_cpu_support_flag(void) {
  uint32_t ecx = cpuid(CPUID_FEATURES);
  cached_cpu_supports_crc32 = ecx & SSE42_FEATURE_BIT;
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
 * Hardware-accelerated CRC32C calculation using the 64-bit instructions.
 */
static uint32_t crc32c_hardware(uint32_t crc, const uint8_t* p_buf, size_t length) {
  // start directly at p_buf, even if it's an unaligned address. According
  // to the original author of this code, doing a small run of single bytes
  // to word-align the 64-bit instructions doesn't seem to help, but
  // we haven't reconfirmed those benchmarks ourselves.
  uint64_t crc64bit = crc;
  size_t i;
  for (i = 0; i < length / sizeof(uint64_t); i++) {
    crc64bit = _mm_crc32_u64(crc64bit, *(uint64_t*) p_buf);
    p_buf += sizeof(uint64_t);
  }

  // This ugly switch is slightly faster for short strings than the straightforward loop
  uint32_t crc32bit = (uint32_t) crc64bit;
  length &= sizeof(uint64_t) - 1;
  switch (length) {
    case 7:
      crc32bit = _mm_crc32_u8(crc32bit, *p_buf++);
    case 6:
      crc32bit = _mm_crc32_u16(crc32bit, *(uint16_t*) p_buf);
      p_buf += 2;
    // case 5 is below: 4 + 1
    case 4:
      crc32bit = _mm_crc32_u32(crc32bit, *(uint32_t*) p_buf);
      break;
    case 3:
      crc32bit = _mm_crc32_u8(crc32bit, *p_buf++);
    case 2:
      crc32bit = _mm_crc32_u16(crc32bit, *(uint16_t*) p_buf);
      break;
    case 5:
      crc32bit = _mm_crc32_u32(crc32bit, *(uint32_t*) p_buf);
      p_buf += 4;
    case 1:
      crc32bit = _mm_crc32_u8(crc32bit, *p_buf);
      break;
    case 0:
      break;
    default:
      // This should never happen; enable in debug code
      assert(0 && "ended up with 8 or more bytes at tail of calculation");
  }

  return crc32bit;
}

#ifdef USE_PIPELINED
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
#endif /* USE_PIPELINED */

# else  // 32-bit

/**
 * Hardware-accelerated CRC32C calculation using the 32-bit instructions.
 */
static uint32_t crc32c_hardware(uint32_t crc, const uint8_t* p_buf, size_t length) {
  // start directly at p_buf, even if it's an unaligned address. According
  // to the original author of this code, doing a small run of single bytes
  // to word-align the 64-bit instructions doesn't seem to help, but
  // we haven't reconfirmed those benchmarks ourselves.
  size_t i;
  for (i = 0; i < length / sizeof(uint32_t); i++) {
    crc = _mm_crc32_u32(crc, *(uint32_t*) p_buf);
    p_buf += sizeof(uint32_t);
  }

  // This ugly switch is slightly faster for short strings than the straightforward loop
  length &= sizeof(uint32_t) - 1;
  switch (length) {
    case 3:
      crc = _mm_crc32_u8(crc, *p_buf++);
    case 2:
      crc = _mm_crc32_u16(crc, *(uint16_t*) p_buf);
      break;
    case 1:
      crc = _mm_crc32_u8(crc, *p_buf);
      break;
    case 0:
      break;
    default:
      // This should never happen; enable in debug code
      assert(0 && "ended up with 4 or more bytes at tail of calculation");
  }

  return crc;
}

#ifdef USE_PIPELINED
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

#endif /* USE_PIPELINED */

# endif // 64-bit vs 32-bit

#else // end x86 architecture

static uint32_t crc32c_hardware(uint32_t crc, const uint8_t* data, size_t length) {
  // never called!
  assert(0 && "hardware crc called on an unsupported platform");
  return 0;
}

#endif
