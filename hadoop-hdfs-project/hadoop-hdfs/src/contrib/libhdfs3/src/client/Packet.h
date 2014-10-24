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

#ifndef _HDFS_LIBHDFS3_CLIENT_PACKET_H_
#define _HDFS_LIBHDFS3_CLIENT_PACKET_H_

#include <stdint.h>
#include <vector>

#define HEART_BEAT_SEQNO -1

namespace hdfs {
namespace internal {

class ConstPacketBuffer {
public:
    ConstPacketBuffer(const char *buf, int size) : buffer(buf), size(size) {
    }

    const char *getBuffer() const {
        return buffer;
    }

    const int getSize() const {
        return size;
    }

private:
    const char *buffer;
    const int size;
};

/**
 * buffer is pointed into like follows:
 *  (C is checksum data, D is payload data)
 *
 * [HHHHHCCCCC________________DDDDDDDDDDDDDDDD___]
 *       ^    ^               ^               ^
 *       |    checksumPos     dataStart       dataPos
 *   checksumStart
 */
class Packet {
public:
    /**
     * create a heart beat packet
     */
    Packet();

    /**
     * create a new packet
     */
    Packet(int pktSize, int chunksPerPkt, int64_t offsetInBlock, int64_t seqno,
           int checksumSize);

    void reset(int pktSize, int chunksPerPkt, int64_t offsetInBlock,
               int64_t seqno, int checksumSize);

    void addChecksum(uint32_t checksum);

    void addData(const char *buf, int size);

    void setSyncFlag(bool sync);

    void increaseNumChunks();

    bool isFull();

    bool isHeartbeat();

    void setLastPacketInBlock(bool lastPacket);

    int getDataSize();

    const ConstPacketBuffer getBuffer();

    int64_t getLastByteOffsetBlock();

    int64_t getSeqno() const {
        return seqno;
    }

    bool isLastPacketInBlock() const {
        return lastPacketInBlock;
    }

    int64_t getOffsetInBlock() const {
        return offsetInBlock;
    }

private:
    bool lastPacketInBlock;  // is this the last packet in block
    bool syncBlock;          // sync block to disk?
    int checksumPos;
    int checksumSize;
    int checksumStart;
    int dataPos;
    int dataStart;
    int headerStart;
    int maxChunks;          // max chunks in packet
    int numChunks;          // number of chunks currently in packet
    int64_t offsetInBlock;  // offset in block
    int64_t seqno;          // sequence number of packet in block
    std::vector<char> buffer;
};
}
}
#endif /* _HDFS_LIBHDFS3_CLIENT_PACKET_H_ */
