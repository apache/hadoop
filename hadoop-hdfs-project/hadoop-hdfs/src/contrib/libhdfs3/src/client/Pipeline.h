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

#ifndef _HDFS_LIBHDFS3_CLIENT_PIPELINE_H_
#define _HDFS_LIBHDFS3_CLIENT_PIPELINE_H_

#include "Packet.h"
#include "PipelineAck.h"
#include "SessionConfig.h"
#include "SharedPtr.h"
#include "Thread.h"
#include "network/BufferedSocketReader.h"
#include "network/TcpSocket.h"
#include "server/DatanodeInfo.h"
#include "server/LocatedBlock.h"
#include "server/Namenode.h"

#include <deque>
#include <vector>

namespace hdfs {
namespace internal {

class FileSystemImpl;

enum BlockConstructionStage {
    /**
     * The enumerates are always listed as regular stage followed by the
     * recovery stage.
     * Changing this order will make getRecoveryStage not working.
     */
    // pipeline set up for block append
    PIPELINE_SETUP_APPEND = 0,
    // pipeline set up for failed PIPELINE_SETUP_APPEND recovery
    PIPELINE_SETUP_APPEND_RECOVERY = 1,
    // data streaming
    DATA_STREAMING = 2,
    // pipeline setup for failed data streaming recovery
    PIPELINE_SETUP_STREAMING_RECOVERY = 3,
    // close the block and pipeline
    PIPELINE_CLOSE = 4,
    // Recover a failed PIPELINE_CLOSE
    PIPELINE_CLOSE_RECOVERY = 5,
    // pipeline set up for block creation
    PIPELINE_SETUP_CREATE = 6
};

static inline const char *StageToString(BlockConstructionStage stage) {
    switch (stage) {
        case PIPELINE_SETUP_APPEND:
            return "PIPELINE_SETUP_APPEND";

        case PIPELINE_SETUP_APPEND_RECOVERY:
            return "PIPELINE_SETUP_APPEND_RECOVERY";

        case DATA_STREAMING:
            return "DATA_STREAMING";

        case PIPELINE_SETUP_STREAMING_RECOVERY:
            return "PIPELINE_SETUP_STREAMING_RECOVERY";

        case PIPELINE_CLOSE:
            return "PIPELINE_CLOSE";

        case PIPELINE_CLOSE_RECOVERY:
            return "PIPELINE_CLOSE_RECOVERY";

        case PIPELINE_SETUP_CREATE:
            return "PIPELINE_SETUP_CREATE";

        default:
            return "UNKNOWN STAGE";
    }
}

class Packet;
class OutputStreamImpl;

/**
 * setup, data transfer, close, and failover.
 */
class Pipeline {
public:
    /**
     * construct and setup the pipeline for append.
     */
    Pipeline(bool append, const char *path, const SessionConfig &conf,
             shared_ptr<FileSystemImpl> filesystem, int checksumType,
             int chunkSize, int replication, int64_t bytesSent,
             shared_ptr<LocatedBlock> lastBlock);

    /**
     * send all data and wait for all ack.
     */
    void flush();

    /**
     * send LastPacket and close the pipeline.
     */
    shared_ptr<LocatedBlock> close(shared_ptr<Packet> lastPacket);

    /**
     * send a packet, retry on error until fatal.
     * @param packet
     */
    void send(shared_ptr<Packet> packet);

private:
    bool addDatanodeToPipeline(const std::vector<DatanodeInfo> &excludedNodes);
    void buildForAppendOrRecovery(bool recovery);
    void buildForNewBlock();
    void checkPipelineWithReplicas();
    void checkResponse(bool wait);
    void createBlockOutputStream(const Token &token, int64_t gs, bool recovery);
    void locateNextBlock(const std::vector<DatanodeInfo> &excludedNodes);
    void processAck(PipelineAck &ack);
    void processResponse();
    void resend();
    void waitForAcks();
    void transfer(const ExtendedBlock &blk, const DatanodeInfo &src,
                  const std::vector<DatanodeInfo> &targets, const Token &token);
    int findNewDatanode(const std::vector<DatanodeInfo> &original);
    static void checkBadLinkFormat(const std::string &node);

private:
    Pipeline(const Pipeline &other);
    Pipeline &operator=(const Pipeline &other);

    BlockConstructionStage stage;
    bool canAddDatanode;
    int blockWriteRetry;
    int checksumType;
    int chunkSize;
    int connectTimeout;
    int errorIndex;
    int readTimeout;
    int replication;
    int writeTimeout;
    int64_t bytesAcked;  // the size of bytes the ack received.
    int64_t bytesSent;   // the size of bytes has sent.
    shared_ptr<BufferedSocketReader> reader;
    shared_ptr<FileSystemImpl> filesystem;
    shared_ptr<LocatedBlock> lastBlock;
    shared_ptr<Socket> sock;
    std::deque<shared_ptr<Packet>> packets;
    std::string clientName;
    std::string path;
    std::vector<DatanodeInfo> nodes;
    std::vector<std::string> storageIDs;
};
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_PIPELINE_H_ */
