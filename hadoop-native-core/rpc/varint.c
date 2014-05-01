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

#include "varint.h"

#include <errno.h>
#include <stdint.h>

int varint32_size(int32_t val)
{
    if ((val & (0xffffffff <<  7)) == 0) return 1;
    if ((val & (0xffffffff << 14)) == 0) return 2;
    if ((val & (0xffffffff << 21)) == 0) return 3;
    if ((val & (0xffffffff << 28)) == 0) return 4;
    return 5;
}

int varint32_encode(int32_t val, uint8_t *buf, int32_t len, int32_t *off)
{
    int32_t o = *off;
    uint32_t var = (uint32_t)val;

    while (1) {
        if (o == len)
            return -ENOBUFS;
        if (var <= 127) {
            buf[o++] = var;
            break;
        }
        buf[o++] = 0x80 | (var & 0x7f);
        var >>= 7;
    }
    *off = o;
    return 0;
}

int varint32_decode(int32_t *val, const uint8_t *buf, int32_t len,
                    int32_t *off)
{
    uint32_t accum = 0;
    int shift = 0, o = *off, idx = 0;
    uint8_t b;

    do {
        if (o == len)
            return -ENOBUFS;
        if (idx++ > 5)
            return -EINVAL;
        b = buf[o++];
        accum += (b & 0x7f) << shift;
        shift += 7;
    } while(b & 0x80);
    *val = (int32_t)accum;
    *off = o;
    return 0;
}

void be32_encode(int32_t val, uint8_t *buf)
{
    buf[0] = (val >> 24) & 0xff;
    buf[1] = (val >> 16) & 0xff;
    buf[2] = (val >> 8) & 0xff;
    buf[3] = (val >> 0) & 0xff;
}

int32_t be32_decode(const uint8_t *buf)
{
    int32_t v = 0;
    v |= (buf[0] << 24);
    v |= (buf[1] << 16);
    v |= (buf[2] << 8);
    v |= (buf[3] << 0);
    return v;
}

// vim: ts=4:sw=4:tw=79:et
