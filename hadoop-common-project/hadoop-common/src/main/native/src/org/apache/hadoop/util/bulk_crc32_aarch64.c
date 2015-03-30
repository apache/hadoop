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
#include <stddef.h>    // for size_t

#include  "bulk_crc32.h"
#include "gcc_optimizations.h"

/**
 * Hardware-accelerated CRC32 calculation using the 64-bit instructions.
 * 2 variants:-
 *   pipelined_crc32c uses the Castagnoli polynomial 0x1EDC6F41
 *   pipelined_crc32_zlib uses the Zlib polynomial 0x04C11DB7
 */

// gcc doesn't know how to vectorize a 128 bit load, so use the following to tell it
#define LDP(x,y,p) asm("ldp %x[a], %x[b], [%x[c]], #16" : [a]"=r"(x),[b]"=r"(y),[c]"+r"(p))

#define CRC32CX(crc,value) asm("crc32cx %w[c], %w[c], %x[v]" : [c]"+r"(*&crc) : [v]"r"(+value))
#define CRC32CW(crc,value) asm("crc32cw %w[c], %w[c], %w[v]" : [c]"+r"(*&crc) : [v]"r"(+value))
#define CRC32CH(crc,value) asm("crc32ch %w[c], %w[c], %w[v]" : [c]"+r"(*&crc) : [v]"r"(+value))
#define CRC32CB(crc,value) asm("crc32cb %w[c], %w[c], %w[v]" : [c]"+r"(*&crc) : [v]"r"(+value))

#define CRC32ZX(crc,value) asm("crc32x %w[c], %w[c], %x[v]" : [c]"+r"(crc) : [v]"r"(value))
#define CRC32ZW(crc,value) asm("crc32w %w[c], %w[c], %w[v]" : [c]"+r"(crc) : [v]"r"(value))
#define CRC32ZH(crc,value) asm("crc32h %w[c], %w[c], %w[v]" : [c]"+r"(crc) : [v]"r"(value))
#define CRC32ZB(crc,value) asm("crc32b %w[c], %w[c], %w[v]" : [c]"+r"(crc) : [v]"r"(value))

/**
 * Pipelined version of hardware-accelerated CRC32 calculation using
 * the 64 bit crc32 instructions. 
 * One crc32 instruction takes three cycles, but two more with no data
 * dependency can be in the pipeline to achieve something close to single 
 * instruction/cycle. Here we feed three blocks in RR.
 *
 * 2 variants:-
 *   pipelined_crc32c uses the Castagnoli polynomial 0x1EDC6F41
 *   pipelined_crc32_zlib uses the Zlib polynomial 0x04C11DB7
 *
 *   crc1, crc2, crc3 : Store initial checksum for each block before
 *           calling. When it returns, updated checksums are stored.
 *   p_buf : The base address of the data buffer. The buffer should be
 *           at least as big as block_size * num_blocks.
 *   block_size : The size of each block in bytes.
 *   num_blocks : The number of blocks to work on. Min = 1, Max = 3
 */
static void pipelined_crc32c(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3, const uint8_t *p_buf1, size_t block_size, int num_blocks) {
  uint64_t c1 = *crc1;
  uint64_t c2 = *crc2;
  uint64_t c3 = *crc3;
  const uint8_t *p_buf2 = p_buf1 + block_size;
  const uint8_t *p_buf3 = p_buf1 + block_size * 2;
  uint64_t x1, y1, x2, y2, x3, y3;
  long len = block_size;

  /* We do switch here because the loop has to be tight in order
   * to fill the pipeline. Any other statement inside the loop
   * or inbetween crc32 instruction can slow things down.
   *
   * Do verify that this code generates the expected assembler
   * by disassembling test_bulk_crc32
   */

  asm(".cpu generic+crc");	// Allow crc instructions in asm
  switch (num_blocks) {
    case 3:
      /* Do three blocks */
      while ((len -= 2*sizeof(uint64_t)) >= 0) {
        LDP(x1,y1,p_buf1);
        LDP(x2,y2,p_buf2);
        LDP(x3,y3,p_buf3);
        CRC32CX(c1, x1);
        CRC32CX(c2, x2);
        CRC32CX(c3, x3);
        CRC32CX(c1, y1);
        CRC32CX(c2, y2);
        CRC32CX(c3, y3);
      }

      if (unlikely(len & sizeof(uint64_t))) {
        x1 = *(uint64_t*)p_buf1; p_buf1 += sizeof(uint64_t);
        x2 = *(uint64_t*)p_buf2; p_buf2 += sizeof(uint64_t);
        x3 = *(uint64_t*)p_buf3; p_buf3 += sizeof(uint64_t);
        CRC32CX(c1, x1);
        CRC32CX(c2, x2);
        CRC32CX(c3, x3);
      }
      if (unlikely(len & sizeof(uint32_t))) {
        x1 = *(uint32_t*)p_buf1; p_buf1 += sizeof(uint32_t);
        x2 = *(uint32_t*)p_buf2; p_buf2 += sizeof(uint32_t);
        x3 = *(uint32_t*)p_buf3; p_buf3 += sizeof(uint32_t);
        CRC32CW(c1, x1);
        CRC32CW(c2, x2);
        CRC32CW(c3, x3);
      }
      if (unlikely(len & sizeof(uint16_t))) {
        x1 = *(uint16_t*)p_buf1; p_buf1 += sizeof(uint16_t);
        x2 = *(uint16_t*)p_buf2; p_buf2 += sizeof(uint16_t);
        x3 = *(uint16_t*)p_buf3; p_buf3 += sizeof(uint16_t);
        CRC32CH(c1, x1);
        CRC32CH(c2, x2);
        CRC32CH(c3, x3);
      }
      if (unlikely(len & sizeof(uint8_t))) {
        x1 = *p_buf1;
        x2 = *p_buf2;
        x3 = *p_buf3;
        CRC32CB(c1, x1);
        CRC32CB(c2, x2);
        CRC32CB(c3, x3);
      }
      break;
    case 2:
      /* Do two blocks */
      while ((len -= 2*sizeof(uint64_t)) >= 0) {
        LDP(x1,y1,p_buf1);
        LDP(x2,y2,p_buf2);
        CRC32CX(c1, x1);
        CRC32CX(c2, x2);
        CRC32CX(c1, y1);
        CRC32CX(c2, y2);
      }

      if (unlikely(len & sizeof(uint64_t))) {
        x1 = *(uint64_t*)p_buf1; p_buf1 += sizeof(uint64_t);
        x2 = *(uint64_t*)p_buf2; p_buf2 += sizeof(uint64_t);
        CRC32CX(c1, x1);
        CRC32CX(c2, x2);
      }
      if (unlikely(len & sizeof(uint32_t))) {
        x1 = *(uint32_t*)p_buf1; p_buf1 += sizeof(uint32_t);
        x2 = *(uint32_t*)p_buf2; p_buf2 += sizeof(uint32_t);
        CRC32CW(c1, x1);
        CRC32CW(c2, x2);
      }
      if (unlikely(len & sizeof(uint16_t))) {
        x1 = *(uint16_t*)p_buf1; p_buf1 += sizeof(uint16_t);
        x2 = *(uint16_t*)p_buf2; p_buf2 += sizeof(uint16_t);
        CRC32CH(c1, x1);
        CRC32CH(c2, x2);
      }
      if (unlikely(len & sizeof(uint8_t))) {
        x1 = *p_buf1;
        x2 = *p_buf2;
        CRC32CB(c1, x1);
        CRC32CB(c2, x2);
      }
      break;
    case 1:
      /* single block */
      while ((len -= 2*sizeof(uint64_t)) >= 0) {
        LDP(x1,y1,p_buf1);
        CRC32CX(c1, x1);
        CRC32CX(c1, y1);
      }

      if (unlikely(len & sizeof(uint64_t))) {
        x1 = *(uint64_t*)p_buf1; p_buf1 += sizeof(uint64_t);
        CRC32CX(c1, x1);
      }
      if (unlikely(len & sizeof(uint32_t))) {
        x1 = *(uint32_t*)p_buf1; p_buf1 += sizeof(uint32_t);
        CRC32CW(c1, x1);
      }
      if (unlikely(len & sizeof(uint16_t))) {
        x1 = *(uint16_t*)p_buf1; p_buf1 += sizeof(uint16_t);
        CRC32CH(c1, x1);
      }
      if (unlikely(len & sizeof(uint8_t))) {
        x1 = *p_buf1;
        CRC32CB(c1, x1);
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

static void pipelined_crc32_zlib(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3, const uint8_t *p_buf1, size_t block_size, int num_blocks) {
  uint64_t c1 = *crc1;
  uint64_t c2 = *crc2;
  uint64_t c3 = *crc3;
  const uint8_t *p_buf2 = p_buf1 + block_size;
  const uint8_t *p_buf3 = p_buf1 + block_size * 2;
  uint64_t x1, y1, x2, y2, x3, y3;
  long len = block_size;

  /* We do switch here because the loop has to be tight in order
   * to fill the pipeline. Any other statement inside the loop
   * or inbetween crc32 instruction can slow things down.
   *
   * Do verify that this code generates the expected assembler
   * by disassembling test_bulk_crc32
   */

  asm(".cpu generic+crc");	// Allow crc instructions in asm
  switch (num_blocks) {
    case 3:
      /* Do three blocks */
      while ((len -= 2*sizeof(uint64_t)) >= 0) {
        LDP(x1,y1,p_buf1);
        LDP(x2,y2,p_buf2);
        LDP(x3,y3,p_buf3);
        CRC32ZX(c1, x1);
        CRC32ZX(c2, x2);
        CRC32ZX(c3, x3);
        CRC32ZX(c1, y1);
        CRC32ZX(c2, y2);
        CRC32ZX(c3, y3);
      }

      if (unlikely(len & sizeof(uint64_t))) {
        x1 = *(uint64_t*)p_buf1; p_buf1 += sizeof(uint64_t);
        x2 = *(uint64_t*)p_buf2; p_buf2 += sizeof(uint64_t);
        x3 = *(uint64_t*)p_buf3; p_buf3 += sizeof(uint64_t);
        CRC32ZX(c1, x1);
        CRC32ZX(c2, x2);
        CRC32ZX(c3, x3);
      }
      if (unlikely(len & sizeof(uint32_t))) {
        x1 = *(uint32_t*)p_buf1; p_buf1 += sizeof(uint32_t);
        x2 = *(uint32_t*)p_buf2; p_buf2 += sizeof(uint32_t);
        x3 = *(uint32_t*)p_buf3; p_buf3 += sizeof(uint32_t);
        CRC32ZW(c1, x1);
        CRC32ZW(c2, x2);
        CRC32ZW(c3, x3);
      }
      if (unlikely(len & sizeof(uint16_t))) {
        x1 = *(uint16_t*)p_buf1; p_buf1 += sizeof(uint16_t);
        x2 = *(uint16_t*)p_buf2; p_buf2 += sizeof(uint16_t);
        x3 = *(uint16_t*)p_buf3; p_buf3 += sizeof(uint16_t);
        CRC32ZH(c1, x1);
        CRC32ZH(c2, x2);
        CRC32ZH(c3, x3);
      }
      if (unlikely(len & sizeof(uint8_t))) {
        x1 = *p_buf1;
        x2 = *p_buf2;
        x3 = *p_buf3;
        CRC32ZB(c1, x1);
        CRC32ZB(c2, x2);
        CRC32ZB(c3, x3);
      }
      break;
    case 2:
      /* Do two blocks */
      while ((len -= 2*sizeof(uint64_t)) >= 0) {
        LDP(x1,y1,p_buf1);
        LDP(x2,y2,p_buf2);
        CRC32ZX(c1, x1);
        CRC32ZX(c2, x2);
        CRC32ZX(c1, y1);
        CRC32ZX(c2, y2);
      }

      if (unlikely(len & sizeof(uint64_t))) {
        x1 = *(uint64_t*)p_buf1; p_buf1 += sizeof(uint64_t);
        x2 = *(uint64_t*)p_buf2; p_buf2 += sizeof(uint64_t);
        CRC32ZX(c1, x1);
        CRC32ZX(c2, x2);
      }
      if (unlikely(len & sizeof(uint32_t))) {
        x1 = *(uint32_t*)p_buf1; p_buf1 += sizeof(uint32_t);
        x2 = *(uint32_t*)p_buf2; p_buf2 += sizeof(uint32_t);
        CRC32ZW(c1, x1);
        CRC32ZW(c2, x2);
      }
      if (unlikely(len & sizeof(uint16_t))) {
        x1 = *(uint16_t*)p_buf1; p_buf1 += sizeof(uint16_t);
        x2 = *(uint16_t*)p_buf2; p_buf2 += sizeof(uint16_t);
        CRC32ZH(c1, x1);
        CRC32ZH(c2, x2);
      }
      if (unlikely(len & sizeof(uint8_t))) {
        x1 = *p_buf1;
        x2 = *p_buf2;
        CRC32ZB(c1, x1);
        CRC32ZB(c2, x2);
      }
      break;
    case 1:
      /* single block */
      while ((len -= 2*sizeof(uint64_t)) >= 0) {
        LDP(x1,y1,p_buf1);
        CRC32ZX(c1, x1);
        CRC32ZX(c1, y1);
      }

      if (unlikely(len & sizeof(uint64_t))) {
        x1 = *(uint64_t*)p_buf1; p_buf1 += sizeof(uint64_t);
        CRC32ZX(c1, x1);
      }
      if (unlikely(len & sizeof(uint32_t))) {
        x1 = *(uint32_t*)p_buf1; p_buf1 += sizeof(uint32_t);
        CRC32ZW(c1, x1);
      }
      if (unlikely(len & sizeof(uint16_t))) {
        x1 = *(uint16_t*)p_buf1; p_buf1 += sizeof(uint16_t);
        CRC32ZH(c1, x1);
      }
      if (unlikely(len & sizeof(uint8_t))) {
        x1 = *p_buf1;
        CRC32ZB(c1, x1);
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

typedef void (*crc_pipelined_func_t)(uint32_t *, uint32_t *, uint32_t *, const uint8_t *, size_t, int);
extern crc_pipelined_func_t pipelined_crc32c_func;
extern crc_pipelined_func_t pipelined_crc32_zlib_func;

#include <sys/auxv.h>
#include <asm/hwcap.h>

#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif

/**
 * On library load, determine what sort of crc we are going to do
 * and set crc function pointers appropriately.
 */
void __attribute__ ((constructor)) init_cpu_support_flag(void) {
  unsigned long auxv = getauxval(AT_HWCAP);
  if (auxv & HWCAP_CRC32) {
    pipelined_crc32c_func = pipelined_crc32c;
    pipelined_crc32_zlib_func = pipelined_crc32_zlib;
  }
}
