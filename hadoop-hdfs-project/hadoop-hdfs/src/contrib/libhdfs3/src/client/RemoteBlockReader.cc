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
#include "DataTransferProtocolSender.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "HWCrc32c.h"
#include "RemoteBlockReader.h"
#include "SWCrc32c.h"
#include "WriteBuffer.h"
#include "datatransfer.pb.h"

#include <inttypes.h>
#include <vector>

using hadoop::hdfs::ClientReadStatusProto;
using hadoop::hdfs::BlockOpResponseProto;
using hadoop::hdfs::ChecksumProto;
using hadoop::hdfs::ChecksumTypeProto;
using hadoop::hdfs::ReadOpChecksumInfoProto;
using hadoop::hdfs::Status;

namespace hdfs {
namespace internal {

RemoteBlockReader::RemoteBlockReader(const ExtendedBlock &eb,
          DatanodeInfo &datanode, int64_t start, int64_t len,
          const Token &token, const char * clientName, bool verify,
          SessionConfig &conf) :
    verify(verify), datanode(datanode), binfo(eb), checksumSize(0),
      chunkSize(0), position(0), size(0), cursor(
        start), endOffset(len + start), lastSeqNo(-1) {
    try {
        assert(start >= 0);
        readTimeout = conf.getInputReadTimeout();
        writeTimeout = conf.getInputWriteTimeout();
        connTimeout = conf.getInputConnTimeout();
        sock = shared_ptr<Socket>(new TcpSocketImpl());
        in = shared_ptr<BufferedSocketReader>(
                      new BufferedSocketReaderImpl(*sock));
        sock->connect(datanode.getIpAddr().c_str(), datanode.getXferPort(),
                      connTimeout);
        sender = shared_ptr<DataTransferProtocol>(
                     new DataTransferProtocolSender(*sock, writeTimeout,
                                                    datanode.formatAddress()));
        sender->readBlock(eb, token, clientName, start, len);
        checkResponse();
    } catch (const HdfsTimeoutException &e) {
        NESTED_THROW(HdfsIOException,
                     "RemoteBlockReader: Failed to setup remote block reader "
                     "for block %s from node %s",
                     eb.toString().c_str(), datanode.formatAddress().c_str());
    }
}

RemoteBlockReader::~RemoteBlockReader() {
    sock->close();
}

void RemoteBlockReader::checkResponse() {
    std::vector<char> respBuffer;
    int32_t respSize = in->readVarint32(readTimeout);

    if (respSize <= 0 || respSize > 10 * 1024 * 1024) {
        THROW(HdfsIOException, "RemoteBlockReader get a invalid response "
              "size: %d, Block: %s, from Datanode: %s",
              respSize, binfo.toString().c_str(),
              datanode.formatAddress().c_str());
    }

    respBuffer.resize(respSize);
    in->readFully(&respBuffer[0], respSize, readTimeout);
    BlockOpResponseProto resp;

    if (!resp.ParseFromArray(&respBuffer[0], respBuffer.size())) {
        THROW(HdfsIOException, "RemoteBlockReader cannot parse "
              "BlockOpResponseProto from Datanode response, "
              "Block: %s, from Datanode: %s", binfo.toString().c_str(),
              datanode.formatAddress().c_str());
    }

    if (resp.status() != hadoop::hdfs::SUCCESS) {
        std::string msg;

        if (resp.has_message()) {
            msg = resp.message();
        }

        if (resp.status() == hadoop::hdfs::ERROR_ACCESS_TOKEN) {
            THROW(HdfsInvalidBlockToken, "RemoteBlockReader: block's token "
                  "is invalid. Datanode: %s, Block: %s",
                  datanode.formatAddress().c_str(), binfo.toString().c_str());
        } else {
            THROW(HdfsIOException,
                  "RemoteBlockReader: Datanode return an error when sending "
                  "read request to Datanode: %s, Block: %s, %s.",
                  datanode.formatAddress().c_str(), binfo.toString().c_str(),
                  (msg.empty() ? "check Datanode's log for more information" :
                     msg.c_str()));
        }
    }

    const ReadOpChecksumInfoProto &checksumInfo = resp.readopchecksuminfo();
    const ChecksumProto &cs = checksumInfo.checksum();
    chunkSize = cs.bytesperchecksum();

    if (chunkSize < 0) {
        THROW(HdfsIOException,
              "RemoteBlockReader invalid chunk size: %d, expected range[0, %"
              PRId64 "], Block: %s, from Datanode: %s",
              chunkSize, binfo.getNumBytes(), binfo.toString().c_str(),
              datanode.formatAddress().c_str());
    }

    switch (cs.type()) {
    case ChecksumTypeProto::CHECKSUM_NULL:
        verify = false;
        checksumSize = 0;
        break;

    case ChecksumTypeProto::CHECKSUM_CRC32:
        THROW(HdfsIOException, "RemoteBlockReader does not support CRC32 "
              "checksum, Block: %s, from Datanode: %s",
              binfo.toString().c_str(), datanode.formatAddress().c_str());
        break;

    case ChecksumTypeProto::CHECKSUM_CRC32C:
        if (HWCrc32c::available()) {
            checksum = shared_ptr<Checksum>(new HWCrc32c());
        } else {
            checksum = shared_ptr<Checksum>(new SWCrc32c());
        }

        checksumSize = sizeof(int32_t);
        break;

    default:
        THROW(HdfsIOException, "RemoteBlockReader cannot recognize checksum "
              "type: %d, Block: %s, from Datanode: %s",
              static_cast<int>(cs.type()), binfo.toString().c_str(),
              datanode.formatAddress().c_str());
    }

    /*
     * The offset into the block at which the first packet
     * will start. This is necessary since reads will align
     * backwards to a checksum chunk boundary.
     */
    int64_t firstChunkOffset = checksumInfo.chunkoffset();

    if (firstChunkOffset < 0 || firstChunkOffset > cursor ||
          firstChunkOffset <= cursor - chunkSize) {
        THROW(HdfsIOException,
              "RemoteBlockReader invalid first chunk offset: %" PRId64
              ", expected range[0, %" PRId64 "], " "Block: %s, from Datanode: %s",
              firstChunkOffset, cursor, binfo.toString().c_str(),
              datanode.formatAddress().c_str());
    }
}

shared_ptr<PacketHeader> RemoteBlockReader::readPacketHeader() {
    try {
        shared_ptr<PacketHeader> retval;
        static const int packetHeaderLen = PacketHeader::GetPkgHeaderSize();
        std::vector<char> buf(packetHeaderLen);

        if (lastHeader && lastHeader->isLastPacketInBlock()) {
            THROW(HdfsIOException, "RemoteBlockReader: read over block end "
                 "from Datanode: %s, Block: %s.",
                 datanode.formatAddress().c_str(), binfo.toString().c_str());
        }

        in->readFully(&buf[0], packetHeaderLen, readTimeout);
        retval = shared_ptr<PacketHeader>(new PacketHeader);
        retval->readFields(&buf[0], packetHeaderLen);
        return retval;
    } catch (const HdfsIOException &e) {
        NESTED_THROW(HdfsIOException, "RemoteBlockReader: failed to read "
                 "block header for Block: %s from Datanode: %s.",
                 binfo.toString().c_str(), datanode.formatAddress().c_str());
    }
}

void RemoteBlockReader::readNextPacket() {
    assert(position >= size);
    lastHeader = readPacketHeader();
    int dataSize = lastHeader->getDataLen();
    int64_t pendingAhead = 0;

    if (!lastHeader->sanityCheck(lastSeqNo)) {
        THROW(HdfsIOException, "RemoteBlockReader: Packet failed on sanity "
              "check for block %s from Datanode %s.",
              binfo.toString().c_str(), datanode.formatAddress().c_str());
    }

    assert(dataSize > 0 || lastHeader->getPacketLen() == sizeof(int32_t));

    if (dataSize > 0) {
        int chunks = (dataSize + chunkSize - 1) / chunkSize;
        int checksumLen = chunks * checksumSize;
        size = checksumLen + dataSize;
        assert(size ==
             lastHeader->getPacketLen() - static_cast<int>(sizeof(int32_t)));
        buffer.resize(size);
        in->readFully(&buffer[0], size, readTimeout);
        lastSeqNo = lastHeader->getSeqno();

        if (lastHeader->getPacketLen() != static_cast<int>(sizeof(int32_t)) +
            dataSize + checksumLen) {
            THROW(HdfsIOException, "Invalid Packet, packetLen is %d, "
                  "dataSize is %d, checksum size is %d",
                  lastHeader->getPacketLen(), dataSize, checksumLen);
        }

        if (verify) {
            verifyChecksum(chunks);
        }

        /*
         * skip checksum
         */
        position = checksumLen;
        /*
         * the first packet we get may start at the position before we required
         */
        pendingAhead = cursor - lastHeader->getOffsetInBlock();
        pendingAhead = pendingAhead > 0 ? pendingAhead : 0;
        position += pendingAhead;
    }

    /*
     * we reach the end of the range we required, send status to datanode
     * if datanode do not sending data anymore.
     */

    if (cursor + dataSize - pendingAhead >= endOffset && readTrailingEmptyPacket()) {
        sendStatus();
    }
}

bool RemoteBlockReader::readTrailingEmptyPacket() {
    shared_ptr<PacketHeader> trailingHeader = readPacketHeader();

    if (!trailingHeader->isLastPacketInBlock() || trailingHeader->getDataLen() != 0) {
        return false;
    }

    return true;
}

void RemoteBlockReader::sendStatus() {
    ClientReadStatusProto status;

    if (verify) {
        status.set_status(hadoop::hdfs::CHECKSUM_OK);
    } else {
        status.set_status(hadoop::hdfs::SUCCESS);
    }

    WriteBuffer buffer;
    int size = status.ByteSize();
    buffer.writeVarint32(size);
    status.SerializeToArray(buffer.alloc(size), size);
    sock->writeFully(buffer.getBuffer(0), buffer.getDataSize(0), writeTimeout);
}

void RemoteBlockReader::verifyChecksum(int chunks) {
    int dataSize = lastHeader->getDataLen();
    char * pchecksum = &buffer[0];
    char * pdata = &buffer[0] + (chunks * checksumSize);

    for (int i = 0; i < chunks; ++i) {
        int size = chunkSize < dataSize ? chunkSize : dataSize;
        dataSize -= size;
        checksum->reset();
        checksum->update(pdata + (i * chunkSize), size);
        uint32_t result = checksum->getValue();
        uint32_t target =
          ReadBigEndian32FromArray(pchecksum + (i * checksumSize));

        if (result != target && size == chunkSize) {
            THROW(ChecksumException, "RemoteBlockReader: checksum not match "
                  "for Block: %s, on Datanode: %s",
                  binfo.toString().c_str(), datanode.formatAddress().c_str());
        }
    }

    assert(0 == dataSize);
}

int64_t RemoteBlockReader::available() {
    return size - position > 0 ? size - position : 0;
}

int32_t RemoteBlockReader::read(char * buf, int32_t len) {
    assert(0 != len && NULL != buf);

    if (cursor >= endOffset) {
        THROW(HdfsIOException, "RemoteBlockReader: read over block end from "
              "Datanode: %s, Block: %s.",
              datanode.formatAddress().c_str(), binfo.toString().c_str());
    }

    try {
        if (position >= size) {
            readNextPacket();
        }

        int32_t todo = len < size - position ? len : size - position;
        memcpy(buf, &buffer[position], todo);
        position += todo;
        cursor += todo;
        return todo;
    } catch (const HdfsTimeoutException &e) {
        NESTED_THROW(HdfsIOException, "RemoteBlockReader: failed to read "
                  "Block: %s from Datanode: %s.",
                  binfo.toString().c_str(), datanode.formatAddress().c_str());
    } catch (const HdfsNetworkException &e) {
        NESTED_THROW(HdfsIOException, "RemoteBlockReader: failed to read "
                  "Block: %s from Datanode: %s.",
                  binfo.toString().c_str(), datanode.formatAddress().c_str());
    }
}

void RemoteBlockReader::skip(int64_t len) {
    int64_t todo = len;
    assert(cursor + len <= endOffset);

    try {
        while (todo > 0) {
            if (cursor >= endOffset) {
                THROW(HdfsIOException, "RemoteBlockReader: skip over block "
                  "end from Datanode: %s, Block: %s.",
                  datanode.formatAddress().c_str(), binfo.toString().c_str());
            }

            if (position >= size) {
                readNextPacket();
            }

            int batch = size - position;
            batch = batch < todo ? batch : static_cast<int>(todo);
            position += batch;
            cursor += batch;
            todo -= batch;
        }
    } catch (const HdfsTimeoutException &e) {
        NESTED_THROW(HdfsIOException, "RemoteBlockReader: failed to read "
                 "Block: %s from Datanode: %s.",
                 binfo.toString().c_str(), datanode.formatAddress().c_str());
    } catch (const HdfsNetworkException &e) {
        NESTED_THROW(HdfsIOException, "RemoteBlockReader: failed to read "
                 "Block: %s from Datanode: %s.",
                 binfo.toString().c_str(), datanode.formatAddress().c_str());
    }
}

}
}
