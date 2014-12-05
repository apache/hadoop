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

#include "DateTime.h"
#include "Pipeline.h"
#include "Logger.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemImpl.h"
#include "DataTransferProtocolSender.h"
#include "datatransfer.pb.h"

#include <inttypes.h>
#include <vector>

using std::vector;

using namespace ::hadoop::common;
using namespace ::hadoop::hdfs;

namespace hdfs {
namespace internal {

Pipeline::Pipeline(bool append, const char *path, const SessionConfig &conf,
                   shared_ptr<FileSystemImpl> filesystem, int checksumType,
                   int chunkSize, int replication, int64_t bytesSent,
                   shared_ptr<LocatedBlock> lastBlock)
    : checksumType(checksumType),
      chunkSize(chunkSize),
      errorIndex(-1),
      replication(replication),
      bytesAcked(bytesSent),
      bytesSent(bytesSent),
      filesystem(filesystem),
      lastBlock(lastBlock),
      path(path) {
    canAddDatanode = conf.canAddDatanode();
    blockWriteRetry = conf.getBlockWriteRetry();
    connectTimeout = conf.getOutputConnTimeout();
    readTimeout = conf.getOutputReadTimeout();
    writeTimeout = conf.getOutputWriteTimeout();
    clientName = filesystem->getClientName();

    if (append) {
        LOG(DEBUG2,
            "create pipeline for file %s to append to %s at "
            "position %" PRId64,
            path, lastBlock->toString().c_str(), lastBlock->getNumBytes());
        stage = PIPELINE_SETUP_APPEND;
        assert(lastBlock);
        nodes = lastBlock->getLocations();
        storageIDs = lastBlock->getStorageIDs();
        buildForAppendOrRecovery(false);
        stage = DATA_STREAMING;
    } else {
        LOG(DEBUG2, "create pipeline for file %s to write to a new block",
            path);
        stage = PIPELINE_SETUP_CREATE;
        buildForNewBlock();
        stage = DATA_STREAMING;
    }
}

int Pipeline::findNewDatanode(const std::vector<DatanodeInfo> &original) {
    if (nodes.size() != original.size() + 1) {
        THROW(HdfsIOException,
              "Failed to acquire a datanode for block "
              "%s from namenode.",
              lastBlock->toString().c_str());
    }

    for (size_t i = 0; i < nodes.size(); i++) {
        size_t j = 0;

        for (; j < original.size() && !(nodes[i] == original[j]); j++)
            ;

        if (j == original.size()) {
            return i;
        }
    }

    THROW(HdfsIOException, "Cannot add new datanode for block %s.",
          lastBlock->toString().c_str());
}

void Pipeline::transfer(const ExtendedBlock &blk, const DatanodeInfo &src,
                        const vector<DatanodeInfo> &targets,
                        const Token &token) {
    shared_ptr<Socket> so(new TcpSocketImpl);
    shared_ptr<BufferedSocketReader> in(new BufferedSocketReaderImpl(*so));
    so->connect(src.getIpAddr().c_str(), src.getXferPort(), connectTimeout);
    DataTransferProtocolSender sender(*so, writeTimeout, src.formatAddress());
    sender.transferBlock(blk, token, clientName.c_str(), targets);
    int size;
    size = in->readVarint32(readTimeout);
    std::vector<char> buf(size);
    in->readFully(&buf[0], size, readTimeout);
    BlockOpResponseProto resp;

    if (!resp.ParseFromArray(&buf[0], size)) {
        THROW(HdfsIOException,
              "cannot parse datanode response from %s "
              "for block %s.",
              src.formatAddress().c_str(), lastBlock->toString().c_str());
    }

    if (::hadoop::hdfs::Status::SUCCESS != resp.status()) {
        THROW(HdfsIOException,
              "Failed to transfer block to a new datanode "
              "%s for block %s.",
              targets[0].formatAddress().c_str(),
              lastBlock->toString().c_str());
    }
}

bool Pipeline::addDatanodeToPipeline(
    const vector<DatanodeInfo> &excludedNodes) {
    try {
        /*
         * get a new datanode
         */
        std::vector<DatanodeInfo> original = nodes;
        shared_ptr<LocatedBlock> lb;
        lb = filesystem->getAdditionalDatanode(path, *lastBlock, nodes,
                                               storageIDs, excludedNodes, 1);
        nodes = lb->getLocations();
        storageIDs = lb->getStorageIDs();

        /*
         * failed to add new datanode into pipeline.
         */
        if (original.size() == nodes.size()) {
            LOG(LOG_ERROR,
                "Failed to add new datanode into pipeline for block: %s "
                "file %s.",
                lastBlock->toString().c_str(), path.c_str());
        } else {
            /*
             * find the new datanode
             */
            int d = findNewDatanode(original);
            /*
             * in case transfer block fail.
             */
            errorIndex = d;
            /*
             * transfer replica
             */
            DatanodeInfo &src = d == 0 ? nodes[1] : nodes[d - 1];
            std::vector<DatanodeInfo> targets;
            targets.push_back(nodes[d]);
            LOG(INFO, "Replicate block %s from %s to %s for file %s.",
                lastBlock->toString().c_str(), src.formatAddress().c_str(),
                targets[0].formatAddress().c_str(), path.c_str());
            transfer(*lastBlock, src, targets, lb->getToken());
            errorIndex = -1;
            return true;
        }
    } catch (const HdfsCanceled &e) {
        throw;
    } catch (const HdfsFileSystemClosed &e) {
        throw;
    } catch (const SafeModeException &e) {
        throw;
    } catch (const HdfsException &e) {
        LOG(LOG_ERROR,
            "Failed to add a new datanode into pipeline for block: "
            "%s file %s.\n%s",
            lastBlock->toString().c_str(), path.c_str(), GetExceptionDetail(e));
    }

    return false;
}

void Pipeline::checkPipelineWithReplicas() {
    if (static_cast<int>(nodes.size()) < replication) {
        std::stringstream ss;
        int size = nodes.size();

        for (int i = 0; i < size - 1; ++i) {
            ss << nodes[i].formatAddress() << ", ";
        }

        if (nodes.empty()) {
            ss << "Empty";
        } else {
            ss << nodes.back().formatAddress();
        }

        LOG(WARNING,
            "the number of nodes in pipeline is %d [%s], is less than the "
            "expected number of replica %d for block %s file %s",
            static_cast<int>(nodes.size()), ss.str().c_str(), replication,
            lastBlock->toString().c_str(), path.c_str());
    }
}

void Pipeline::buildForAppendOrRecovery(bool recovery) {
    int64_t gs = 0;
    int retry = blockWriteRetry;
    exception_ptr lastException;
    std::vector<DatanodeInfo> excludedNodes;
    shared_ptr<LocatedBlock> lb;

    do {
        /*
         * Remove bad datanode from list of datanodes.
         * If errorIndex was not set (i.e. appends), then do not remove
         * any datanodes
         */
        if (errorIndex >= 0) {
            assert(lastBlock);
            LOG(LOG_ERROR,
                "Pipeline: node %s is invalid and removed from "
                "pipeline when %s block %s for file %s, stage = %s.",
                nodes[errorIndex].formatAddress().c_str(),
                (recovery ? "recovery" : "append to"),
                lastBlock->toString().c_str(), path.c_str(),
                StageToString(stage));
            excludedNodes.push_back(nodes[errorIndex]);
            nodes.erase(nodes.begin() + errorIndex);

            if (!storageIDs.empty()) {
                storageIDs.erase(storageIDs.begin() + errorIndex);
            }

            if (nodes.empty()) {
                THROW(HdfsIOException,
                      "Build pipeline to %s block %s failed: all datanodes "
                      "are bad.",
                      (recovery ? "recovery" : "append to"),
                      lastBlock->toString().c_str());
            }

            errorIndex = -1;
        }

        try {
            gs = 0;

            /*
             * Check if the number of datanodes in pipeline satisfy the
             * replication requirement,
             * add new datanode if not
             */
            if (stage != PIPELINE_SETUP_CREATE && stage != PIPELINE_CLOSE &&
                static_cast<int>(nodes.size()) < replication &&
                canAddDatanode) {
                if (!addDatanodeToPipeline(excludedNodes)) {
                    THROW(HdfsIOException,
                          "Failed to add new datanode "
                          "to pipeline for block: %s file %s, set "
                          "\"output.replace-datanode-on-failure\" to "
                          "\"false\" to disable this feature.",
                          lastBlock->toString().c_str(), path.c_str());
                }
            }

            if (errorIndex >= 0) {
                continue;
            }

            checkPipelineWithReplicas();
            /*
             * Update generation stamp and access token
             */
            lb = filesystem->updateBlockForPipeline(*lastBlock);
            gs = lb->getGenerationStamp();
            /*
             * Try to build pipeline
             */
            createBlockOutputStream(lb->getToken(), gs, recovery);
            /*
             * everything is ok, reset errorIndex.
             */
            errorIndex = -1;
            lastException = exception_ptr();
            break;  // break on success
        } catch (const HdfsInvalidBlockToken &e) {
            lastException = current_exception();
            recovery = true;
            LOG(LOG_ERROR,
                "Pipeline: Failed to build pipeline for block %s file %s, "
                "new generation stamp is %" PRId64 ",\n%s",
                lastBlock->toString().c_str(), path.c_str(), gs,
                GetExceptionDetail(e));
            LOG(INFO, "Try to recovery pipeline for block %s file %s.",
                lastBlock->toString().c_str(), path.c_str());
        } catch (const HdfsTimeoutException &e) {
            lastException = current_exception();
            recovery = true;
            LOG(LOG_ERROR,
                "Pipeline: Failed to build pipeline for block %s file %s, "
                "new generation stamp is %" PRId64 ",\n%s",
                lastBlock->toString().c_str(), path.c_str(), gs,
                GetExceptionDetail(e));
            LOG(INFO, "Try to recovery pipeline for block %s file %s.",
                lastBlock->toString().c_str(), path.c_str());
        } catch (const HdfsIOException &e) {
            lastException = current_exception();
            /*
             * Set recovery flag to true in case of failed to create a pipeline
             * for appending.
             */
            recovery = true;
            LOG(LOG_ERROR,
                "Pipeline: Failed to build pipeline for block %s file %s, "
                "new generation stamp is %" PRId64 ",\n%s",
                lastBlock->toString().c_str(), path.c_str(), gs,
                GetExceptionDetail(e));
            LOG(INFO, "Try to recovery pipeline for block %s file %s.",
                lastBlock->toString().c_str(), path.c_str());
        }

        /*
         * we don't known what happened, no datanode is reported failure, reduce
         * retry count in case infinite loop.
         * it may caused by rpc call throw HdfsIOException
         */
        if (errorIndex < 0) {
            --retry;
        }
    } while (retry > 0);

    if (lastException) {
        rethrow_exception(lastException);
    }

    /*
     * Update pipeline at the namenode, non-idempotent RPC call.
     */
    lb->setPoolId(lastBlock->getPoolId());
    lb->setBlockId(lastBlock->getBlockId());
    lb->setLocations(nodes);
    lb->setNumBytes(lastBlock->getNumBytes());
    lb->setOffset(lastBlock->getOffset());
    filesystem->updatePipeline(*lastBlock, *lb, nodes, storageIDs);
    lastBlock = lb;
}

void Pipeline::locateNextBlock(const std::vector<DatanodeInfo> &excludedNodes) {
    milliseconds sleeptime(400);
    int retry = blockWriteRetry;

    while (true) {
        try {
            lastBlock =
                filesystem->addBlock(path, lastBlock.get(), excludedNodes);
            assert(lastBlock);
            return;
        } catch (const NotReplicatedYetException &e) {
            LOG(DEBUG1,
                "Got NotReplicatedYetException when try to addBlock "
                "for block %s, already retry %d times, max retry %d times",
                lastBlock->toString().c_str(), blockWriteRetry - retry,
                blockWriteRetry);

            if (retry--) {
                try {
                    sleep_for(sleeptime);
                } catch (...) {
                }

                sleeptime *= 2;
            } else {
                throw;
            }
        }
    }
}

static std::string FormatExcludedNodes(
    const std::vector<DatanodeInfo> &excludedNodes) {
    std::stringstream ss;
    ss << "[";
    int size = excludedNodes.size();

    for (int i = 0; i < size - 1; ++i) {
        ss << excludedNodes[i].formatAddress() << ", ";
    }

    if (excludedNodes.empty()) {
        ss << "Empty";
    } else {
        ss << excludedNodes.back().formatAddress();
    }

    ss << "]";
    return ss.str();
}

void Pipeline::buildForNewBlock() {
    int retryAllocNewBlock = 0, retry = blockWriteRetry;
    LocatedBlock lb;
    std::vector<DatanodeInfo> excludedNodes;
    shared_ptr<LocatedBlock> block = lastBlock;

    do {
        errorIndex = -1;
        lastBlock = block;

        try {
            locateNextBlock(excludedNodes);
            lastBlock->setNumBytes(0);
            nodes = lastBlock->getLocations();
        } catch (const HdfsRpcException &e) {
            const char *lastBlockName =
                lastBlock ? lastBlock->toString().c_str() : "Null";
            LOG(LOG_ERROR,
                "Failed to allocate a new empty block for file %s, last "
                "block %s, excluded nodes %s.\n%s",
                path.c_str(), lastBlockName,
                FormatExcludedNodes(excludedNodes).c_str(),
                GetExceptionDetail(e));

            if (retryAllocNewBlock > blockWriteRetry) {
                throw;
            }

            LOG(INFO,
                "Retry to allocate a new empty block for file %s, last "
                "block %s, excluded nodes %s.",
                path.c_str(), lastBlockName,
                FormatExcludedNodes(excludedNodes).c_str());
            ++retryAllocNewBlock;
            continue;
        } catch (const HdfsException &e) {
            const char *lastBlockName =
                lastBlock ? lastBlock->toString().c_str() : "Null";
            LOG(LOG_ERROR,
                "Failed to allocate a new empty block for file %s, "
                "last block %s, excluded nodes %s.\n%s",
                path.c_str(), lastBlockName,
                FormatExcludedNodes(excludedNodes).c_str(),
                GetExceptionDetail(e));
            throw;
        }

        retryAllocNewBlock = 0;
        checkPipelineWithReplicas();

        if (nodes.empty()) {
            THROW(HdfsIOException,
                  "No datanode is available to create a pipeline for "
                  "block %s file %s.",
                  lastBlock->toString().c_str(), path.c_str());
        }

        try {
            createBlockOutputStream(lastBlock->getToken(), 0, false);
            break;  // break on success
        } catch (const HdfsInvalidBlockToken &e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(),
                GetExceptionDetail(e));
        } catch (const HdfsTimeoutException &e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(),
                GetExceptionDetail(e));
        } catch (const HdfsIOException &e) {
            LOG(LOG_ERROR,
                "Failed to setup the pipeline for new block %s file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(),
                GetExceptionDetail(e));
        }

        LOG(INFO, "Abandoning block: %s for file %s.",
            lastBlock->toString().c_str(), path.c_str());

        try {
            filesystem->abandonBlock(*lastBlock, path);
        } catch (const HdfsException &e) {
            LOG(LOG_ERROR,
                "Failed to abandon useless block %s for file %s.\n%s",
                lastBlock->toString().c_str(), path.c_str(),
                GetExceptionDetail(e));
            throw;
        }

        if (errorIndex >= 0) {
            LOG(INFO,
                "Excluding invalid datanode: %s for block %s for "
                "file %s",
                nodes[errorIndex].formatAddress().c_str(),
                lastBlock->toString().c_str(), path.c_str());
            excludedNodes.push_back(nodes[errorIndex]);
        } else {
            /*
             * we don't known what happened, no datanode is reported failure,
             * reduce retry count in case of infinite loop.
             */
            --retry;
        }
    } while (retry);
}

/*
 * bad link node must be either empty or a "IP:PORT"
 */
void Pipeline::checkBadLinkFormat(const std::string &n) {
    std::string node = n;

    if (node.empty()) {
        return;
    }

    do {
        const char *host = &node[0], *port;
        size_t pos = node.find_last_of(":");

        if (pos == node.npos || pos + 1 == node.length()) {
            break;
        }

        node[pos] = 0;
        port = &node[pos + 1];
        struct addrinfo hints, *addrs;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = PF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
        int p;
        char *end;
        p = strtol(port, &end, 0);

        if (p >= 65536 || p <= 0 || end != port + strlen(port)) {
            break;
        }

        if (getaddrinfo(host, port, &hints, &addrs)) {
            break;
        }

        freeaddrinfo(addrs);
        return;
    } while (0);

    LOG(FATAL, "Cannot parser the firstBadLink string %s", n.c_str());
    THROW(HdfsException, "Cannot parse the firstBadLink string %s.", n.c_str());
}

void Pipeline::createBlockOutputStream(const Token &token, int64_t gs,
                                       bool recovery) {
    std::string firstBadLink;
    exception_ptr lastError;
    bool needWrapException = true;

    try {
        sock = shared_ptr<Socket>(new TcpSocketImpl);
        reader = shared_ptr<BufferedSocketReader>(
            new BufferedSocketReaderImpl(*sock));
        sock->connect(nodes[0].getIpAddr().c_str(), nodes[0].getXferPort(),
                      connectTimeout);
        std::vector<DatanodeInfo> targets;

        for (size_t i = 1; i < nodes.size(); ++i) {
            targets.push_back(nodes[i]);
        }

        DataTransferProtocolSender sender(*sock, writeTimeout,
                                          nodes[0].formatAddress());
        sender.writeBlock(*lastBlock, token, clientName.c_str(), targets,
                          (recovery ? (stage | 0x1) : stage), nodes.size(),
                          lastBlock->getNumBytes(), bytesSent, gs, checksumType,
                          chunkSize);
        int size;
        size = reader->readVarint32(readTimeout);
        std::vector<char> buf(size);
        reader->readFully(&buf[0], size, readTimeout);
        BlockOpResponseProto resp;

        if (!resp.ParseFromArray(&buf[0], size)) {
            THROW(HdfsIOException,
                  "cannot parse datanode response from %s "
                  "for block %s.",
                  nodes[0].formatAddress().c_str(),
                  lastBlock->toString().c_str());
        }

        ::hadoop::hdfs::Status pipelineStatus = resp.status();
        firstBadLink = resp.firstbadlink();

        if (::hadoop::hdfs::Status::SUCCESS != pipelineStatus) {
            needWrapException = false;

            if (::hadoop::hdfs::Status::ERROR_ACCESS_TOKEN == pipelineStatus) {
                THROW(HdfsInvalidBlockToken,
                      "Got access token error for connect ack with "
                      "firstBadLink as %s for block %s",
                      firstBadLink.c_str(), lastBlock->toString().c_str());
            } else {
                THROW(HdfsIOException,
                      "Bad connect ack with firstBadLink "
                      "as %s for block %s",
                      firstBadLink.c_str(), lastBlock->toString().c_str());
            }
        }

        return;
    } catch (...) {
        errorIndex = 0;
        lastError = current_exception();
    }

    checkBadLinkFormat(firstBadLink);

    if (!firstBadLink.empty()) {
        for (size_t i = 0; i < nodes.size(); ++i) {
            if (nodes[i].getXferAddr() == firstBadLink) {
                errorIndex = i;
                break;
            }
        }
    }

    assert(lastError);

    if (!needWrapException) {
        rethrow_exception(lastError);
    }

    try {
        rethrow_exception(lastError);
    } catch (const HdfsException &e) {
        NESTED_THROW(HdfsIOException,
                     "Cannot create block output stream for block %s, "
                     "recovery flag: %s, with last generate stamp %" PRId64 ".",
                     lastBlock->toString().c_str(),
                     (recovery ? "true" : "false"), gs);
    }
}

void Pipeline::resend() {
    assert(stage != PIPELINE_CLOSE);

    for (size_t i = 0; i < packets.size(); ++i) {
        ConstPacketBuffer b = packets[i]->getBuffer();
        sock->writeFully(b.getBuffer(), b.getSize(), writeTimeout);
        int64_t tmp = packets[i]->getLastByteOffsetBlock();
        bytesSent = bytesSent > tmp ? bytesSent : tmp;
    }
}

void Pipeline::send(shared_ptr<Packet> packet) {
    ConstPacketBuffer buffer = packet->getBuffer();

    if (!packet->isHeartbeat()) {
        packets.push_back(packet);
    }

    bool failover = false;

    do {
        try {
            if (failover) {
                resend();
            } else {
                assert(sock);
                sock->writeFully(buffer.getBuffer(), buffer.getSize(),
                                 writeTimeout);
                int64_t tmp = packet->getLastByteOffsetBlock();
                bytesSent = bytesSent > tmp ? bytesSent : tmp;
            }

            checkResponse(false);
            return;
        } catch (const HdfsIOException &e) {
            if (errorIndex < 0) {
                errorIndex = 0;
            }

            sock.reset();
        }

        buildForAppendOrRecovery(true);
        failover = true;

        if (stage == PIPELINE_CLOSE) {
            assert(packets.size() == 1 && packets[0]->isLastPacketInBlock());
            packets.clear();
            break;
        }
    } while (true);
}

void Pipeline::processAck(PipelineAck &ack) {
    assert(!ack.isInvalid());
    int64_t seqno = ack.getSeqno();

    if (HEART_BEAT_SEQNO == seqno) {
        return;
    }

    assert(!packets.empty());
    Packet &packet = *packets[0];

    if (ack.isSuccess()) {
        if (packet.getSeqno() != seqno) {
            THROW(HdfsIOException,
                  "processAck: pipeline ack expecting seqno %" PRId64
                  "  but received %" PRId64 " for block %s.",
                  packet.getSeqno(), seqno, lastBlock->toString().c_str());
        }

        int64_t tmp = packet.getLastByteOffsetBlock();
        bytesAcked = tmp > bytesAcked ? tmp : bytesAcked;
        assert(lastBlock);
        lastBlock->setNumBytes(bytesAcked);

        if (packet.isLastPacketInBlock()) {
            sock.reset();
        }

        packets[0].reset();
        packets.pop_front();
    } else {
        for (int i = ack.getNumOfReplies() - 1; i >= 0; --i) {
            if (::hadoop::hdfs::Status::SUCCESS != ack.getReply(i)) {
                errorIndex = i;
                /*
                 * handle block token expiry the same as HdfsIOException.
                 */
                THROW(HdfsIOException,
                      "processAck: ack report error at node: %s for block %s.",
                      nodes[i].formatAddress().c_str(),
                      lastBlock->toString().c_str());
            }
        }
    }
}

void Pipeline::processResponse() {
    PipelineAck ack;
    std::vector<char> buf;
    int size = reader->readVarint32(readTimeout);
    ack.reset();
    buf.resize(size);
    reader->readFully(&buf[0], size, readTimeout);
    ack.readFrom(&buf[0], size);

    if (ack.isInvalid()) {
        THROW(HdfsIOException,
              "processAllAcks: get an invalid DataStreamer packet ack "
              "for block %s",
              lastBlock->toString().c_str());
    }

    processAck(ack);
}

void Pipeline::checkResponse(bool wait) {
    int timeout = wait ? readTimeout : 0;
    bool readable = reader->poll(timeout);

    if (readable) {
        processResponse();
    } else if (wait) {
        THROW(HdfsIOException,
              "Timed out reading response from datanode "
              "%s for block %s.",
              nodes[0].formatAddress().c_str(), lastBlock->toString().c_str());
    }
}

void Pipeline::flush() {
    waitForAcks();
}

void Pipeline::waitForAcks() {
    bool failover = false;

    while (!packets.empty()) {
        try {
            if (failover) {
                resend();
            }
            checkResponse(true);
            failover = false;
        } catch (const HdfsIOException &e) {
            if (errorIndex < 0) {
                errorIndex = 0;
            }

            LOG(LOG_ERROR,
                "Failed to flush pipeline on datanode %s for block "
                "%s file %s.\n%s",
                nodes[errorIndex].formatAddress().c_str(),
                lastBlock->toString().c_str(), path.c_str(),
                GetExceptionDetail(e));
            LOG(INFO, "Rebuild pipeline to flush for block %s file %s.",
                lastBlock->toString().c_str(), path.c_str());
            sock.reset();
            failover = true;
        }

        if (failover) {
            buildForAppendOrRecovery(true);

            if (stage == PIPELINE_CLOSE) {
                assert(packets.size() == 1 &&
                       packets[0]->isLastPacketInBlock());
                packets.clear();
                break;
            }
        }
    }
}

shared_ptr<LocatedBlock> Pipeline::close(shared_ptr<Packet> lastPacket) {
    waitForAcks();
    lastPacket->setLastPacketInBlock(true);
    stage = PIPELINE_CLOSE;
    send(lastPacket);
    waitForAcks();
    sock.reset();
    lastBlock->setNumBytes(bytesAcked);
    LOG(DEBUG2, "close pipeline for file %s, block %s with length %" PRId64,
        path.c_str(), lastBlock->toString().c_str(), lastBlock->getNumBytes());
    return lastBlock;
}
}
}
