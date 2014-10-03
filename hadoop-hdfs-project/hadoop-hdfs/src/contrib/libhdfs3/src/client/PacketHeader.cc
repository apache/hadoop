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
#include "PacketHeader.h"

namespace hdfs {
namespace internal {

int PacketHeader::PkgHeaderSize = PacketHeader::CalcPkgHeaderSize();

int PacketHeader::CalcPkgHeaderSize() {
    PacketHeaderProto header;
    header.set_offsetinblock(0);
    header.set_datalen(0);
    header.set_lastpacketinblock(false);
    header.set_seqno(0);
    return header.ByteSize() + sizeof(int32_t) /*packet length*/ + sizeof(int16_t)/* proto length */;
}

int PacketHeader::GetPkgHeaderSize() {
    return PkgHeaderSize;
}

PacketHeader::PacketHeader() :
    packetLen(0) {
}

PacketHeader::PacketHeader(int packetLen, int64_t offsetInBlock, int64_t seqno,
                           bool lastPacketInBlock, int dataLen) :
    packetLen(packetLen) {
    proto.set_offsetinblock(offsetInBlock);
    proto.set_seqno(seqno);
    proto.set_lastpacketinblock(lastPacketInBlock);
    proto.set_datalen(dataLen);
}

int PacketHeader::getDataLen() {
    return proto.datalen();
}

bool PacketHeader::isLastPacketInBlock() {
    return proto.lastpacketinblock();
}

bool PacketHeader::sanityCheck(int64_t lastSeqNo) {
    // We should only have a non-positive data length for the last packet
    if (proto.datalen() <= 0 && !proto.lastpacketinblock())
        return false;

    // The last packet should not contain data
    if (proto.lastpacketinblock() && proto.datalen() != 0)
        return false;

    // Seqnos should always increase by 1 with each packet received
    if (proto.seqno() != lastSeqNo + 1)
        return false;

    return true;
}

int64_t PacketHeader::getSeqno() {
    return proto.seqno();
}

int64_t PacketHeader::getOffsetInBlock() {
    return proto.offsetinblock();
}

int PacketHeader::getPacketLen() {
    return packetLen;
}

void PacketHeader::readFields(const char * buf, size_t size) {
    int16_t protoLen;
    assert(size > sizeof(packetLen) + sizeof(protoLen));
    packetLen = ReadBigEndian32FromArray(buf);
    protoLen = ReadBigEndian16FromArray(buf + sizeof(packetLen));

    if (packetLen < static_cast<int>(sizeof(int32_t)) || protoLen < 0
            || static_cast<int>(sizeof(packetLen) + sizeof(protoLen)) + protoLen > static_cast<int>(size)) {
        THROW(HdfsIOException, "Invalid PacketHeader, packetLen is %d, protoLen is %hd, buf size is %zu", packetLen,
              protoLen, size);
    }

    if (!proto.ParseFromArray(buf + sizeof(packetLen) + sizeof(protoLen),
                              protoLen)) {
        THROW(HdfsIOException,
              "PacketHeader cannot parse PacketHeaderProto from datanode response.");
    }
}

void PacketHeader::writeInBuffer(char * buf, size_t size) {
    buf = WriteBigEndian32ToArray(packetLen, buf);
    buf = WriteBigEndian16ToArray(proto.ByteSize(), buf);
    proto.SerializeToArray(buf, size - sizeof(int32_t) - sizeof(int16_t));
}

}
}
