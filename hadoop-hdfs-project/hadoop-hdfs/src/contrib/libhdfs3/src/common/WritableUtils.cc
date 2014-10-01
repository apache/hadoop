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

#include "WritableUtils.h"

#include <arpa/inet.h>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

namespace hdfs {
namespace internal {

WritableUtils::WritableUtils(char *b, size_t l) :
    buffer(b), len(l), current(0) {
}

int32_t WritableUtils::ReadInt32() {
    int64_t val;
    val = ReadInt64();

    if (val < std::numeric_limits<int32_t>::min()
            || val > std::numeric_limits<int32_t>::max()) {
        throw std::range_error("overflow");
    }

    return val;
}

int64_t WritableUtils::ReadInt64() {
    int64_t value;
    int firstByte = readByte();
    int len = decodeWritableUtilsSize(firstByte);

    if (len == 1) {
        value = firstByte;
        return value;
    }

    long i = 0;

    for (int idx = 0; idx < len - 1; idx++) {
        unsigned char b = readByte();
        i = i << 8;
        i = i | (b & 0xFF);
    }

    value = (isNegativeWritableUtils(firstByte) ? (i ^ -1L) : i);
    return value;
}

void WritableUtils::ReadRaw(char *buf, size_t size) {
    if (size > len - current) {
        throw std::range_error("overflow");
    }

    memcpy(buf, buffer + current, size);
    current += size;
}

std::string WritableUtils::ReadText() {
    int32_t length;
    std::string retval;
    length = ReadInt32();
    retval.resize(length);
    ReadRaw(&retval[0], length);
    return retval;
}

size_t WritableUtils::WriteInt32(int32_t value) {
    return WriteInt64(value);
}

size_t WritableUtils::WriteInt64(int64_t value) {
    size_t retval = 1;

    if (value >= -112 && value <= 127) {
        writeByte((int) value);
        return retval;
    }

    int len = -112;

    if (value < 0) {
        value ^= -1L; // take one's complement'
        len = -120;
    }

    long tmp = value;

    while (tmp != 0) {
        tmp = tmp >> 8;
        len--;
    }

    ++retval;
    writeByte((int) len);
    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
        int shiftbits = (idx - 1) * 8;
        long mask = 0xFFL << shiftbits;
        ++retval;
        writeByte((int)((value & mask) >> shiftbits));
    }

    return retval;
}

size_t WritableUtils::WriteRaw(const char *buf, size_t size) {
    if (size > len - current) {
        throw std::range_error("overflow");
    }

    memcpy(buffer + current, buf, size);
    current += size;
    return size;
}

int WritableUtils::decodeWritableUtilsSize(int value) {
    if (value >= -112) {
        return 1;
    } else if (value < -120) {
        return -119 - value;
    }

    return -111 - value;
}

int WritableUtils::readByte() {
    if (sizeof(char) > len - current) {
        throw std::range_error("overflow");
    }

    return buffer[current++];
}

void WritableUtils::writeByte(int val) {
    if (sizeof(char) > len - current) {
        throw std::range_error("overflow");
    }

    buffer[current++] = val;
}

size_t WritableUtils::WriteText(const std::string & str) {
    size_t retval = 0;
    int32_t length = str.length();
    retval += WriteInt32(length);
    retval += WriteRaw(&str[0], length);
    return retval;
}

bool WritableUtils::isNegativeWritableUtils(int value) {
    return value < -120 || (value >= -112 && value < 0);
}

int32_t WritableUtils::ReadBigEndian32() {
    char buf[sizeof(int32_t)];

    for (size_t i = 0; i < sizeof(int32_t); ++i) {
        buf[i] = readByte();
    }

    return ntohl(*(uint32_t *) buf);
}

}
}
