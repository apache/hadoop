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

#ifndef _HDFS_LIBHDFS3_CLIENT_INPUTSTREAM_H_
#define _HDFS_LIBHDFS3_CLIENT_INPUTSTREAM_H_

#include "Status.h"

namespace hdfs {
namespace internal {
class InputStreamImpl;
}

class FileSystem;

/**
 * A input stream used read data from hdfs.
 */
class InputStream {
public:
    /**
     * Construct an instance.
     */
    InputStream();

    /**
     * Destroy this instance.
     */
    ~InputStream();

    /**
     * Open a file to read
     * @param fs hdfs file system.
     * @param path the file to be read.
     * @param verifyChecksum verify the checksum.
     * @return the result status of this operation
     */
    Status open(FileSystem &fs, const std::string &path,
                bool verifyChecksum = true);

    /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than
     * size, -1 on error.
     */
    int32_t read(char *buf, int32_t size);

    /**
     * To read data from hdfs, block until get the given size of bytes.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     * @return the result status of this operation
     */
    Status readFully(char *buf, int64_t size);

    /**
     * Get how many bytes can be read without blocking.
     * @return The number of bytes can be read without blocking, -1 on error.
     */
    int64_t available();

    /**
     * To move the file point to the given position.
     * @param pos the given position.
     * @return the result status of this operation
     */
    Status seek(int64_t pos);

    /**
     * To get the current file point position.
     * @return the position of current file pointer, -1 on error.
     */
    int64_t tell();

    /**
     * Close the stream.
     * @return the result status of this operation
     */
    Status close();

    /**
     * Get the error status of the last operation.
     * @return the error status of the last operation.
     */
    Status getLastError();

private:
    InputStream(const InputStream &other);
    InputStream &operator=(InputStream &other);

    internal::InputStreamImpl *impl;
    Status lastError;
};
}

#endif /* _HDFS_LIBHDFS3_CLIENT_INPUTSTREAM_H_ */
