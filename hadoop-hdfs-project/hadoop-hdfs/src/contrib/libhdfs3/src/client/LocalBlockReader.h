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

#ifndef _HDFS_LIBHDFS3_CLIENT_LOCALBLOCKREADER_H_
#define _HDFS_LIBHDFS3_CLIENT_LOCALBLOCKREADER_H_

#include "BlockReader.h"
#include "Checksum.h"
#include "FileWrapper.h"
#include "SessionConfig.h"
#include "common/SharedPtr.h"
#include "server/BlockLocalPathInfo.h"

#include <stdint.h>
#include <vector>

namespace hdfs {
namespace internal {

class LocalBlockReader: public BlockReader {
public:
    LocalBlockReader(const BlockLocalPathInfo & info,
                     const ExtendedBlock & block, int64_t offset, bool verify,
                     SessionConfig & conf, std::vector<char> & buffer);

    ~LocalBlockReader();

    /**
     * Get how many bytes can be read without blocking.
     * @return The number of bytes can be read without blocking.
     */
    virtual int64_t available() {
        return length - cursor;
    }

    /**
     * To read data from block.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     * @return return the number of bytes filled in the buffer,
     *  it may less than size. Return 0 if reach the end of block.
     */
    virtual int32_t read(char * buf, int32_t size);

    /**
     * Move the cursor forward len bytes.
     * @param len The number of bytes to skip.
     */
    virtual void skip(int64_t len);

private:
    /**
     * Fill buffer and verify checksum.
     * @param bufferSize The size of buffer.
     */
    void readAndVerify(int32_t bufferSize);
    int32_t readInternal(char * buf, int32_t len);

private:
    bool verify; //verify checksum or not.
    const char *pbuffer;
    const char *pMetaBuffer;
    const ExtendedBlock &block;
    int checksumSize;
    int chunkSize;
    int localBufferSize;
    int position; //point in buffer.
    int size;  //data size in buffer.
    int64_t cursor; //point in block.
    int64_t length; //data size of block.
    shared_ptr<Checksum> checksum;
    shared_ptr<FileWrapper> dataFd;
    shared_ptr<FileWrapper> metaFd;
    std::string dataFilePath;
    std::string metaFilePath;
    std::vector<char> & buffer;
    std::vector<char> metaBuffer;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_LOCALBLOCKREADER_H_ */
