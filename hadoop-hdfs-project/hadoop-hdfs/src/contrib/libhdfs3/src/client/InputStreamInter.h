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

#ifndef _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMINTER_H_
#define _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMINTER_H_

#include <SharedPtr.h>

#include <stdint.h>
#include <string>

namespace hdfs {
namespace internal {

class FileSystemInter;

/**
 * A input stream used read data from hdfs.
 */
class InputStreamInter {
public:

    virtual ~InputStreamInter() {
    }

    /**
     * Open a file to read
     * @param fs hdfs file system.
     * @param path the file to be read.
     * @param verifyChecksum verify the checksum.
     */
    virtual void open(shared_ptr<FileSystemInter> fs, const char * path,
                      bool verifyChecksum) = 0;

    /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
    virtual int32_t read(char * buf, int32_t size) = 0;

    /**
     * To read data from hdfs, block until get the given size of bytes.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     */
    virtual void readFully(char * buf, int64_t size) = 0;

    /**
     * Get how many bytes can be read without blocking.
     * @return The number of bytes can be read without blocking.
     */
    virtual int64_t available() = 0;

    /**
     * To move the file point to the given position.
     * @param pos the given position.
     */
    virtual void seek(int64_t pos) = 0;

    /**
     * To get the current file point position.
     * @return the position of current file point.
     */
    virtual int64_t tell() = 0;

    /**
     * Close the stream.
     */
    virtual void close() = 0;

    /**
     * Output a readable string of this input stream.
     */
    virtual std::string toString() = 0;
};

}
}
#endif /* _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMINTER_H_ */
