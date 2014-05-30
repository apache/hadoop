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

#ifndef HADOOP_CORE_VARINT_H
#define HADOOP_CORE_VARINT_H

#include <stdint.h>

/**
 * Compute the size of a varint.
 *
 * @param val           The value to encode.
 *
 * @return              The number of bytes it will take to encode.
 */
int varint32_size(int32_t val);

/**
 * Encode a 32-bit varint.
 *
 * @param val           The value to encode.
 * @param buf           The buffer to encode into.
 * @param len           The length of the buffer.
 * @param off           (inout) The offset to start writing at.  This will be
 *                      updated with the new offset after we finish the
 *                      encoding.
 *
 * @return              0 on success; -EINVAL if we ran out of space.
 */
int varint32_encode(int32_t val, uint8_t *buf, int32_t max, int32_t *off);

/**
 * Decode a 32-bit varint.
 *
 * @param val           (out param) The decoded value.
 * @param buf           The buffer to decode from.
 * @param len           The length of the buffer.
 * @param off           (inout) The offset to start reading at.  This will be
 *                      updated with the new offset after we finish the
 *                      decoding.
 *
 * @return              0 on success; -EINVAL if we ran out of space.
 */
int varint32_decode(int32_t *val, const uint8_t *buf, int32_t max,
                    int32_t *off);

/**
 * Encode a fixed-len, big-endian int32_t.
 *
 * @param val           The value to encode.
 * @param buf           The buffer to encode into.
 */
void be32_encode(int32_t val, uint8_t *buf);

/**
 * Decode a fixed-len, big-endian int32_t.
 *
 * @param buf           The buffer to decode from.  The buffer must be at
 *                        least 4 bytes long.
 *
 * @return              The decoded integer.
 */
int32_t be32_decode(const uint8_t *buf);

#endif

// vim: ts=4:sw=4:tw=79:et
