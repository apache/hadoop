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

#ifndef _HDFS_LIBHDFS3_COMMON_BIGENDIAN_H_
#define _HDFS_LIBHDFS3_COMMON_BIGENDIAN_H_

#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>

namespace hdfs {
namespace internal {

static inline int16_t ReadBigEndian16FromArray(const char * buffer) {
    int16_t retval;
    retval = ntohs(*reinterpret_cast<const int16_t *>(buffer));
    return retval;
}

static inline int32_t ReadBigEndian32FromArray(const char * buffer) {
    int32_t retval;
    retval = ntohl(*reinterpret_cast<const int32_t *>(buffer));
    return retval;
}

static inline char * WriteBigEndian16ToArray(int16_t value, char * buffer) {
    int16_t bigValue = htons(value);
    memcpy(buffer, reinterpret_cast<const char *>(&bigValue), sizeof(int16_t));
    return buffer + sizeof(int16_t);
}

static inline char * WriteBigEndian32ToArray(int32_t value, char * buffer) {
    int32_t bigValue = htonl(value);
    memcpy(buffer, reinterpret_cast<const char *>(&bigValue), sizeof(int32_t));
    return buffer + sizeof(int32_t);
}

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_BIGENDIAN_H_ */
