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

#ifndef _HDFS_LIBHDFS3_COMMON_CHECKSUM_H_
#define _HDFS_LIBHDFS3_COMMON_CHECKSUM_H_

#include <stdint.h>

#define CHECKSUM_TYPE_SIZE 1
#define CHECKSUM_BYTES_PER_CHECKSUM_SIZE 4
#define CHECKSUM_TYPE_CRC32C 2

namespace hdfs {
namespace internal {

/**
 * An abstract base CRC class.
 */
class Checksum {
public:
    /**
     * @return Returns the current checksum value.
     */
    virtual uint32_t getValue() = 0;

    /**
     * Resets the checksum to its initial value.
     */
    virtual void reset() = 0;

    /**
     * Updates the current checksum with the specified array of bytes.
     * @param b The buffer of data.
     * @param len The buffer length.
     */
    virtual void update(const void * b, int len) = 0;

    /**
     * Destroy the instance.
     */
    virtual ~Checksum() {
    }
};

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_CHECKSUM_H_ */
