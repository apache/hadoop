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

#ifndef _HDFS_LIBHDFS3_CLIENT_REMOTEBLOCKREADER_H_
#define _HDFS_LIBHDFS3_CLIENT_REMOTEBLOCKREADER_H_

#include "BlockReader.h"
#include "Checksum.h"
#include "DataTransferProtocol.h"
#include "PacketHeader.h"
#include "SessionConfig.h"
#include "common/SharedPtr.h"
#include "network/BufferedSocketReader.h"
#include "network/TcpSocket.h"
#include "server/DatanodeInfo.h"
#include "server/LocatedBlocks.h"

#include <stdint.h>

namespace hdfs {
namespace internal {

class RemoteBlockReader : public BlockReader {
public:
    RemoteBlockReader(const ExtendedBlock &eb, DatanodeInfo &datanode,
                      int64_t start, int64_t len, const Token &token,
                      const char *clientName, bool verify, SessionConfig &conf);

    ~RemoteBlockReader();

    /**
     * Get how many bytes can be read without blocking.
     * @return The number of bytes can be read without blocking.
     */
    virtual int64_t available();

    /**
     * To read data from block.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     * @return return the number of bytes filled in the buffer,
     *  it may less than size. Return 0 if reach the end of block.
     */
    virtual int32_t read(char *buf, int32_t len);

    /**
     * Move the cursor forward len bytes.
     * @param len The number of bytes to skip.
     */
    virtual void skip(int64_t len);

private:
    RemoteBlockReader(const RemoteBlockReader &other);
    RemoteBlockReader &operator=(RemoteBlockReader &other);
    bool readTrailingEmptyPacket();
    shared_ptr<PacketHeader> readPacketHeader();
    void checkResponse();
    void readNextPacket();
    void sendStatus();
    void verifyChecksum(int chunks);

    bool verify;  // verify checksum or not.
    DatanodeInfo &datanode;
    const ExtendedBlock &binfo;
    int checksumSize;
    int chunkSize;
    int connTimeout;
    int position;  // point in buffer.
    int readTimeout;
    int size;  // data size in buffer.
    int writeTimeout;
    int64_t cursor;     // point in block.
    int64_t endOffset;  // offset in block requested to read to.
    int64_t lastSeqNo;  // segno of the last chunk received
    shared_ptr<BufferedSocketReader> in;
    shared_ptr<Checksum> checksum;
    shared_ptr<DataTransferProtocol> sender;
    shared_ptr<PacketHeader> lastHeader;
    shared_ptr<Socket> sock;
    std::vector<char> buffer;
};
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_REMOTEBLOCKREADER_H_ */
