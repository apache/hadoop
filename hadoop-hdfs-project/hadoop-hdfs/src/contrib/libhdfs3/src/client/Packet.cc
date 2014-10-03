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

#include "BigEndian.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Packet.h"
#include "PacketHeader.h"

namespace hdfs {
namespace internal {

Packet::Packet() :
    lastPacketInBlock(false), syncBlock(false), checksumPos(0), checksumSize(0),
    checksumStart(0), dataPos(0), dataStart(0), headerStart(0), maxChunks(
        0), numChunks(0), offsetInBlock(0), seqno(HEART_BEAT_SEQNO) {
    buffer.resize(PacketHeader::GetPkgHeaderSize());
}

Packet::Packet(int pktSize, int chunksPerPkt, int64_t offsetInBlock,
               int64_t seqno, int checksumSize) :
    lastPacketInBlock(false), syncBlock(false), checksumSize(checksumSize), headerStart(0),
    maxChunks(chunksPerPkt), numChunks(0), offsetInBlock(offsetInBlock), seqno(seqno), buffer(pktSize) {
    checksumPos = checksumStart = PacketHeader::GetPkgHeaderSize();
    dataPos = dataStart = checksumStart + chunksPerPkt * checksumSize;
    assert(dataPos >= 0);
}

void Packet::reset(int pktSize, int chunksPerPkt, int64_t offsetInBlock,
                   int64_t seqno, int checksumSize) {
    lastPacketInBlock = false;
    syncBlock = false;
    this->checksumSize = checksumSize;
    headerStart = 0;
    maxChunks = chunksPerPkt;
    numChunks = 0;
    this->offsetInBlock = offsetInBlock;
    this->seqno = seqno;
    checksumPos = checksumStart = PacketHeader::GetPkgHeaderSize();
    dataPos = dataStart = checksumStart + chunksPerPkt * checksumSize;

    if (pktSize > static_cast<int>(buffer.size())) {
        buffer.resize(pktSize);
    }

    assert(dataPos >= 0);
}

void Packet::addChecksum(uint32_t checksum) {
    if (checksumPos + static_cast<int>(sizeof(uint32_t)) > dataStart) {
        THROW(HdfsIOException,
              "Packet: failed to add checksum into packet, checksum is too large");
    }

    WriteBigEndian32ToArray(checksum, &buffer[checksumPos]);
    checksumPos += checksumSize;
}

void Packet::addData(const char * buf, int size) {
    if (size + dataPos > static_cast<int>(buffer.size())) {
        THROW(HdfsIOException,
              "Packet: failed add data to packet, packet size is too small");
    }

    memcpy(&buffer[dataPos], buf, size);
    dataPos += size;
    assert(dataPos >= 0);
}

void Packet::setSyncFlag(bool sync) {
    syncBlock = sync;
}

void Packet::increaseNumChunks() {
    ++numChunks;
}

bool Packet::isFull() {
    return numChunks >= maxChunks;
}

bool Packet::isHeartbeat() {
    return HEART_BEAT_SEQNO == seqno;
}

void Packet::setLastPacketInBlock(bool lastPacket) {
    lastPacketInBlock = lastPacket;
}

int Packet::getDataSize() {
    return dataPos - dataStart;
}

int64_t Packet::getLastByteOffsetBlock() {
    assert(offsetInBlock >= 0 && dataPos >= dataStart);
    assert(dataPos - dataStart <= maxChunks * static_cast<int>(buffer.size()));
    return offsetInBlock + dataPos - dataStart;
}

const ConstPacketBuffer Packet::getBuffer() {
    /*
     * Once this is called, no more data can be added to the packet.
     * This is called only when the packet is ready to be sent.
     */
    int dataLen = dataPos - dataStart;
    int checksumLen = checksumPos - checksumStart;

    if (checksumPos != dataStart) {
        /*
         * move the checksum to cover the gap.
         * This can happen for the last packet.
         */
        memmove(&buffer[dataStart - checksumLen], &buffer[checksumStart],
                checksumLen);
        headerStart = dataStart - checksumPos;
        checksumStart += dataStart - checksumPos;
        checksumPos = dataStart;
    }

    assert(dataPos >= 0);
    int pktLen = dataLen + checksumLen;
    PacketHeader header(pktLen + sizeof(int32_t)
                        /* why we add 4 bytes? Because the server will reduce 4 bytes. -_-*/
                        , offsetInBlock, seqno, lastPacketInBlock, dataLen);
    header.writeInBuffer(&buffer[headerStart],
                         PacketHeader::GetPkgHeaderSize());
    return ConstPacketBuffer(&buffer[headerStart],
                             PacketHeader::GetPkgHeaderSize() + pktLen);
}

}
}
