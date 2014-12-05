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

#ifndef _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAM_H_
#define _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAM_H_

#include "FileSystem.h"
#include "Status.h"

namespace hdfs {

namespace internal {
class OutputStreamImpl;
}

/**
 * Use the CreateFlag as follows:
 * <ol>
 * <li> CREATE - to create a file if it does not exist,
 * else throw FileAlreadyExists.</li>
 * <li> APPEND - to append to a file if it exists,
 * else throw FileNotFoundException.</li>
 * <li> OVERWRITE - to truncate a file if it exists,
 * else throw FileNotFoundException.</li>
 * <li> CREATE|APPEND - to create a file if it does not exist,
 * else append to an existing file.</li>
 * <li> CREATE|OVERWRITE - to create a file if it does not exist,
 * else overwrite an existing file.</li>
 * <li> SyncBlock - to force closed blocks to the disk device.
 * In addition {@link OutputStream::sync()} should be called after each write,
 * if true synchronous behavior is required.</li>
 * </ol>
 *
 * Following combination is not valid and will result in
 * {@link InvalidParameter}:
 * <ol>
 * <li> APPEND|OVERWRITE</li>
 * <li> CREATE|APPEND|OVERWRITE</li>
 * </ol>
 */
enum CreateFlag {
    Create = 0x01,
    Overwrite = 0x02,
    Append = 0x04,
    SyncBlock = 0x08
};

using hdfs::internal::shared_ptr;

/**
 * A output stream used to write data to hdfs.
 */
class OutputStream {
public:
    /**
     * Construct a new OutputStream.
     */
    OutputStream();
    /**
     * Destroy an OutputStream instance.
     */
    ~OutputStream();

    /**
     * To create or append a file.
     * @param fs hdfs file system.
     * @param path the file path.
     * @param flag creation flag, can be Create, Append or Create|Overwrite.
     * @param permission create a new file with given permission.
     * @param createParent if the parent does not exist, create it.
     * @param replication create a file with given number of replication.
     * @param blockSize  create a file with given block size.
     * @return the result status of this operation
     */
    Status open(FileSystem &fs, const std::string &path, int flag = Create,
                const Permission permission = Permission(0644),
                bool createParent = false, int replication = 0,
                int64_t blockSize = 0);

    /**
     * To append data to file.
     * @param buf the data used to append.
     * @param size the data size.
     * @return the result status of this operation
     */
    Status append(const char *buf, uint32_t size);

    /**
     * Flush all data in buffer and waiting for ack.
     * Will block until get all acks.
     * @return the result status of this operation
     */
    Status flush();

    /**
     * Flush all data in buffer and let the Datanode sync the data.
     * @return the result status of this operation
     */
    Status sync();

    /**
     * return the current file length.
     * @param output the pointer of the output parameter
     * @return the result status of this operation
     */
    Status tell(int64_t *output);

    /**
     * close the stream.
     * @return the result status of this operation
     */
    Status close();

private:
    OutputStream(const OutputStream &other);
    OutputStream &operator=(const OutputStream &other);

    internal::OutputStreamImpl *impl;
};
}

#endif /* _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAM_H_ */
