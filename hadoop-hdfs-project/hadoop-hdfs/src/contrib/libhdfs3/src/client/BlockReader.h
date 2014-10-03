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

#ifndef _HDFS_LIBHDFS3_CLIENT_BLOCKREADER_H_
#define _HDFS_LIBHDFS3_CLIENT_BLOCKREADER_H_

#include <stdint.h>

namespace hdfs {
namespace internal {

class BlockReader {
public:
    virtual ~BlockReader() {
    }

    /**
     * Get how many bytes can be read without blocking.
     * @return The number of bytes can be read without blocking.
     */
    virtual int64_t available() = 0;

    /**
     * To read data from block.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     * @return return the number of bytes filled in the buffer,
     *  it may less than size. Return 0 if reach the end of block.
     */
    virtual int32_t read(char * buf, int32_t size) = 0;

    /**
     * Move the cursor forward len bytes.
     * @param len The number of bytes to skip.
     */
    virtual void skip(int64_t len) = 0;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_BLOCKREADER_H_ */
