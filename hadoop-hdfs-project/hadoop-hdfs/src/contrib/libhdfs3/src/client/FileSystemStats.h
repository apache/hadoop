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

#ifndef _HDFS_LIBHDFS3_CLIENT_FSSTATS_H_
#define _HDFS_LIBHDFS3_CLIENT_FSSTATS_H_

#include <stdint.h>

namespace hdfs {

/**
 * file system statistics
 */
class FileSystemStats {
public:
    /**
     * To construct a FileSystemStats.
     */
    FileSystemStats() : capacity(-1), used(-1), remaining(-1) {
    }

    /**
     * To construct a FileSystemStats with given values.
     * @param capacity the capacity of file system.
     * @param used the space which has been used.
     * @param remaining available space on file system.
     */
    FileSystemStats(int64_t capacity, int64_t used, int64_t remaining)
        : capacity(capacity), used(used), remaining(remaining) {
    }

    /**
     * Return the capacity in bytes of the file system
     * @return capacity of file system.
     */
    int64_t getCapacity() {
        return capacity;
    }

    /**
     * Return the number of bytes used on the file system
     * @return return used space.
     */
    int64_t getUsed() {
        return used;
    }

    /**
     * Return the number of remaining bytes on the file system
     * @return return available space.
     */
    int64_t getRemaining() {
        return remaining;
    }

private:
    int64_t capacity;
    int64_t used;
    int64_t remaining;
};
}
#endif /* _HDFS_LIBHDFS3_CLIENT_FSSTATS_H_ */
