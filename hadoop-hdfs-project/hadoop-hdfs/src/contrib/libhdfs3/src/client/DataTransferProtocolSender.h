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

#ifndef _HDFS_LIBHDFS_3_CLIENT_DATATRANSFERPROTOCOLSENDER_H_
#define _HDFS_LIBHDFS_3_CLIENT_DATATRANSFERPROTOCOLSENDER_H_

#include "DataTransferProtocol.h"
#include "network/Socket.h"

/**
 * Version 28:
 *    Declare methods in DataTransferProtocol interface.
 */
#define DATA_TRANSFER_VERSION 28

namespace hdfs {
namespace internal {

enum DataTransferOp {
    WRITE_BLOCK = 80,
    READ_BLOCK = 81,
    READ_METADATA = 82,
    REPLACE_BLOCK = 83,
    COPY_BLOCK = 84,
    BLOCK_CHECKSUM = 85,
    TRANSFER_BLOCK = 86
};

/**
 * Transfer data to/from datanode using a streaming protocol.
 */
class DataTransferProtocolSender: public DataTransferProtocol {
public:
    DataTransferProtocolSender(Socket & sock, int writeTimeout,
                               const std::string & datanodeAddr);

    virtual ~DataTransferProtocolSender();

    /**
     * Read a block.
     *
     * @param blk the block being read.
     * @param blockToken security token for accessing the block.
     * @param clientName client's name.
     * @param blockOffset offset of the block.
     * @param length maximum number of bytes for this read.
     */
    virtual void readBlock(const ExtendedBlock & blk, const Token & blockToken,
                           const char * clientName, int64_t blockOffset, int64_t length);

    /**
     * Write a block to a datanode pipeline.
     *
     * @param blk the block being written.
     * @param blockToken security token for accessing the block.
     * @param clientName client's name.
     * @param targets target datanodes in the pipeline.
     * @param source source datanode.
     * @param stage pipeline stage.
     * @param pipelineSize the size of the pipeline.
     * @param minBytesRcvd minimum number of bytes received.
     * @param maxBytesRcvd maximum number of bytes received.
     * @param latestGenerationStamp the latest generation stamp of the block.
     */
    virtual void writeBlock(const ExtendedBlock & blk, const Token & blockToken,
                            const char * clientName, const std::vector<DatanodeInfo> & targets,
                            int stage, int pipelineSize, int64_t minBytesRcvd,
                            int64_t maxBytesRcvd, int64_t latestGenerationStamp,
                            int checksumType, int bytesPerChecksum);

    /**
     * Transfer a block to another datanode.
     * The block stage must be
     * either {@link BlockConstructionStage#TRANSFER_RBW}
     * or {@link BlockConstructionStage#TRANSFER_FINALIZED}.
     *
     * @param blk the block being transferred.
     * @param blockToken security token for accessing the block.
     * @param clientName client's name.
     * @param targets target datanodes.
     */
    virtual void transferBlock(const ExtendedBlock & blk,
                               const Token & blockToken, const char * clientName,
                               const std::vector<DatanodeInfo> & targets);

    /**
     * Get block checksum (MD5 of CRC32).
     *
     * @param blk a block.
     * @param blockToken security token for accessing the block.
     * @throw HdfsIOException
     */
    virtual void blockChecksum(const ExtendedBlock & blk,
                               const Token & blockToken);

private:
    Socket & sock;
    int writeTimeout;
    std::string datanode;
};

}
}

#endif /* _HDFS_LIBHDFS_3_CLIENT_DATATRANSFERPROTOCOLSENDER_H_ */
