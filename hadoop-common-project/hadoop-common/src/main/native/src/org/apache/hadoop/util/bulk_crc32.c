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
#include <arpa/inet.h>
#include <stdint.h>
#include <unistd.h>

#include "crc32_zlib_polynomial_tables.h"
#include "crc32c_tables.h"
#include "bulk_crc32.h"
#include "gcc_optimizations.h"

typedef uint32_t (*crc_update_func_t)(uint32_t, const uint8_t *, size_t);
static uint32_t crc_init();
static uint32_t crc_val(uint32_t crc);
static uint32_t crc32_zlib_sb8(uint32_t crc, const uint8_t *buf, size_t length);
static uint32_t crc32c_sb8(uint32_t crc, const uint8_t *buf, size_t length);

int bulk_verify_crc(const uint8_t *data, size_t data_len,
                    const uint32_t *sums, int checksum_type,
                    int bytes_per_checksum,
                    crc32_error_t *error_info) {

  crc_update_func_t crc_update_func;
  switch (checksum_type) {
    case CRC32_ZLIB_POLYNOMIAL:
      crc_update_func = crc32_zlib_sb8;
      break;
    case CRC32C_POLYNOMIAL:
      crc_update_func = crc32c_sb8;
      break;
    default:
      return INVALID_CHECKSUM_TYPE;
  }

  while (likely(data_len > 0)) {
    int len = likely(data_len >= bytes_per_checksum) ? bytes_per_checksum : data_len;
    uint32_t crc = crc_init();
    crc = crc_update_func(crc, data, len);
    crc = ntohl(crc_val(crc));
    if (unlikely(crc != *sums)) {
      if (error_info != NULL) {
        error_info->got_crc = crc;
        error_info->expected_crc = *sums;
        error_info->bad_data = data;
      }
      return INVALID_CHECKSUM_DETECTED;
    }
    data += len;
    data_len -= len;
    sums++;
  }
  return CHECKSUMS_VALID;
}


/**
 * Initialize a CRC
 */
static uint32_t crc_init() {
  return 0xffffffff;
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
    crc ^= *(uint32_t *)buf;
    buf += 4;
    uint32_t term1 = CRC32C_T8_7[crc & 0x000000FF] ^
        CRC32C_T8_6[(crc >> 8) & 0x000000FF];
    uint32_t term2 = crc >> 16;
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
    crc ^= *(uint32_t *)buf;
    buf += 4;
    uint32_t term1 = CRC32_T8_7[crc & 0x000000FF] ^
        CRC32_T8_6[(crc >> 8) & 0x000000FF];
    uint32_t term2 = crc >> 16;
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
