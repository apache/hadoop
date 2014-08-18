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
#ifndef BULK_CRC32_H_INCLUDED
#define BULK_CRC32_H_INCLUDED

#include <stdint.h>

#ifdef UNIX
#include <unistd.h> /* for size_t */
#endif // UNIX

// Constants for different CRC algorithms
#define CRC32C_POLYNOMIAL 1
#define CRC32_ZLIB_POLYNOMIAL 2

// Return codes for bulk_verify_crc
#define CHECKSUMS_VALID 0
#define INVALID_CHECKSUM_DETECTED -1
#define INVALID_CHECKSUM_TYPE -2

// Return type for bulk verification when verification fails
typedef struct crc32_error {
  uint32_t got_crc;
  uint32_t expected_crc;
  const uint8_t *bad_data; // pointer to start of data chunk with error
} crc32_error_t;


/**
 * Either calculates checksums for or verifies a buffer of data.
 * Checksums performed in chunks of bytes_per_checksum bytes. The checksums
 * are each 32 bits and are stored in sequential indexes of the 'sums' array.
 * Verification is done (sums is assumed to already contain the checksums)
 * if error_info is non-null; otherwise calculation is done and checksums
 * are stored into sums.
 *
 * @param data                  The data to checksum
 * @param dataLen               Length of the data buffer
 * @param sums                  (out param) buffer to write checksums into or
 *                              where checksums are already stored.
 *                              It must contain at least
 *                              ((dataLen - 1) / bytes_per_checksum + 1) * 4 bytes.
 * @param checksum_type         One of the CRC32 algorithm constants defined 
 *                              above
 * @param bytes_per_checksum    How many bytes of data to process per checksum.
 * @param error_info            If non-NULL, verification will be performed and
 *                              it will be filled in if an error
 *                              is detected. Otherwise calculation is performed.
 *
 * @return                      0 for success, non-zero for an error, result codes
 *                              for verification are defined above
 */
extern int bulk_crc(const uint8_t *data, size_t data_len,
    uint32_t *sums, int checksum_type,
    int bytes_per_checksum,
    crc32_error_t *error_info);

#endif
