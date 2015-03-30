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

#define CRC_INITIAL_VAL 0xffffffff

static uint32_t crc_val(uint32_t crc);

typedef void (*crc_pipelined_func_t)(uint32_t *, uint32_t *, uint32_t *, const uint8_t *, size_t, int);

// The software versions of pipelined crc
static void pipelined_crc32c_sb8(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3,
                                 const uint8_t *p_buf, size_t block_size, int num_blocks);
static void pipelined_crc32_zlib_sb8(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3,
                                 const uint8_t *p_buf, size_t block_size, int num_blocks);

// Satically initialise the function pointers to the software versions
// If a HW implementation is available they will subsequently be initialised in the dynamic
// initialisers to point to the HW routines.
crc_pipelined_func_t pipelined_crc32c_func = pipelined_crc32c_sb8;
crc_pipelined_func_t pipelined_crc32_zlib_func = pipelined_crc32_zlib_sb8;

static inline int store_or_verify(uint32_t *sums, uint32_t crc,
                                   int is_verify) {
  if (!is_verify) {
    *sums = crc;
    return 1;
  } else {
    return crc == *sums;
  }
}

int bulk_crc(const uint8_t *data, size_t data_len,
                    uint32_t *sums, int checksum_type,
                    int bytes_per_checksum,
                    crc32_error_t *error_info) {

  int is_verify = error_info != NULL;

  uint32_t crc1, crc2, crc3;
  int n_blocks = data_len / bytes_per_checksum;
  int remainder = data_len % bytes_per_checksum;
  uint32_t crc;
  crc_pipelined_func_t crc_pipelined_func;
  switch (checksum_type) {
    case CRC32_ZLIB_POLYNOMIAL:
      crc_pipelined_func = pipelined_crc32_zlib_func;
      break;
    case CRC32C_POLYNOMIAL:
      crc_pipelined_func = pipelined_crc32c_func;
      break;
    default:
      return is_verify ? INVALID_CHECKSUM_TYPE : -EINVAL;
  }

  /* Process three blocks at a time */
  while (likely(n_blocks >= 3)) {
    crc1 = crc2 = crc3 = CRC_INITIAL_VAL;
    crc_pipelined_func(&crc1, &crc2, &crc3, data, bytes_per_checksum, 3);

    if (unlikely(!store_or_verify(sums, (crc = ntohl(crc_val(crc1))), is_verify)))
      goto return_crc_error;
    sums++;
    data += bytes_per_checksum;
    if (unlikely(!store_or_verify(sums, (crc = ntohl(crc_val(crc2))), is_verify)))
      goto return_crc_error;
    sums++;
    data += bytes_per_checksum;
    if (unlikely(!store_or_verify(sums, (crc = ntohl(crc_val(crc3))), is_verify)))
      goto return_crc_error;
    sums++;
    data += bytes_per_checksum;
    n_blocks -= 3;
  }

  /* One or two blocks */
  if (n_blocks) {
    crc1 = crc2 = crc3 = CRC_INITIAL_VAL;
    crc_pipelined_func(&crc1, &crc2, &crc3, data, bytes_per_checksum, n_blocks);

    if (unlikely(!store_or_verify(sums, (crc = ntohl(crc_val(crc1))), is_verify)))
      goto return_crc_error;
    data += bytes_per_checksum;
    sums++;
    if (n_blocks == 2) {
      if (unlikely(!store_or_verify(sums, (crc = ntohl(crc_val(crc2))), is_verify)))
        goto return_crc_error;
      sums++;
      data += bytes_per_checksum;
    }
  }

  /* For something smaller than a block */
  if (remainder) {
    crc1 = crc2 = crc3 = CRC_INITIAL_VAL;
    crc_pipelined_func(&crc1, &crc2, &crc3, data, remainder, 1);

    if (unlikely(!store_or_verify(sums, (crc = ntohl(crc_val(crc1))), is_verify)))
      goto return_crc_error;
  }
  return is_verify ? CHECKSUMS_VALID : 0;

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
static uint32_t crc_val(uint32_t crc) {
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

static void pipelined_crc32c_sb8(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3,
                                 const uint8_t *p_buf, size_t block_size, int num_blocks) {
  assert(num_blocks >= 1 && num_blocks <=3 && "invalid num_blocks");
  *crc1 = crc32c_sb8(*crc1, p_buf, block_size);
  if (num_blocks >= 2)
    *crc2 = crc32c_sb8(*crc2, p_buf+block_size, block_size);
  if (num_blocks >= 3)
    *crc3 = crc32c_sb8(*crc3, p_buf+2*block_size, block_size);
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

static void pipelined_crc32_zlib_sb8(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3,
                                     const uint8_t *p_buf, size_t block_size, int num_blocks) {
  assert(num_blocks >= 1 && num_blocks <=3 && "invalid num_blocks");
  *crc1 = crc32_zlib_sb8(*crc1, p_buf, block_size);
  if (num_blocks >= 2)
    *crc2 = crc32_zlib_sb8(*crc2, p_buf+block_size, block_size);
  if (num_blocks >= 3)
    *crc3 = crc32_zlib_sb8(*crc3, p_buf+2*block_size, block_size);
}
