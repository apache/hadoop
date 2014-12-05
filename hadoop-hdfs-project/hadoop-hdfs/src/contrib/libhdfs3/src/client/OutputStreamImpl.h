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

#ifndef _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAMIMPL_H_
#define _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAMIMPL_H_

#include "Atomic.h"
#include "Checksum.h"
#include "DateTime.h"
#include "ExceptionInternal.h"
#include "FileSystem.h"
#include "Permission.h"
#include "Pipeline.h"
#include "SessionConfig.h"
#include "SharedPtr.h"
#include "Thread.h"
#include "server/LocatedBlock.h"
#ifdef MOCK
#include "PipelineStub.h"
#endif

namespace hdfs {
namespace internal {

/**
 * An output stream used to write data to hdfs.
 */
class OutputStreamImpl {
public:
    OutputStreamImpl();

    ~OutputStreamImpl();

    /**
     * To create or append a file.
     * @param fs hdfs file system.
     * @param path the file path.
     * @param flag creation flag, can be Create, Append or Create|Overwrite.
     * @param permission create a new file with given permission.
     * @param createParent if the parent does not exist, create it.
     * @param replication create a file with given number of replication.
     * @param blockSize  create a file with given block size.
     */
    void open(shared_ptr<FileSystemImpl> fs, const char *path, int flag,
              const Permission &permission, bool createParent, int replication,
              int64_t blockSize);

    /**
     * To append data to file.
     * @param buf the data used to append.
     * @param size the data size.
     */
    void append(const char *buf, uint32_t size);

    /**
     * Flush all data in buffer and waiting for ack.
     * Will block until get all acks.
     */
    void flush();

    /**
     * return the current file length.
     * @return current file length.
     */
    int64_t tell();

    /**
     * Flush all data in buffer and let the Datanode sync the data.
     */
    void sync();

    /**
     * close the stream.
     */
    void close();

    /**
     * Output a readable string of this output stream.
     */
    std::string toString();

    /**
     * Keep the last error of this stream.
     * @error the error to be kept.
     */
    void setError(const exception_ptr &error);

private:
    void appendChunkToPacket(const char *buf, int size);
    void appendInternal(const char *buf, uint32_t size);
    void checkStatus();
    void closePipeline();
    void completeFile();
    void computePacketChunkSize();
    void flushInternal(bool needSync);
    // void heartBeatSenderRoutine();
    void initAppend();
    void openInternal(shared_ptr<FileSystemImpl> fs, const char *path, int flag,
                      const Permission &permission, bool createParent,
                      int replication, int64_t blockSize);
    void reset();
    void sendPacket(shared_ptr<Packet> packet);
    void setupPipeline();

private:
    OutputStreamImpl(const OutputStreamImpl &other);
    OutputStreamImpl &operator=(const OutputStreamImpl &other);

    // atomic<bool> heartBeatStop;
    bool closed;
    bool isAppend;
    bool syncBlock;
    // condition_variable condHeartBeatSender;
    exception_ptr lastError;
    int checksumSize;
    int chunkSize;
    int chunksPerPacket;
    int closeTimeout;
    int heartBeatInterval;
    int packetSize;
    int position;  // cursor in buffer
    int replication;
    int64_t blockSize;  // max size of block

    // The size of bytes to be written into the message.  This does not include
    // the data in the chunk buffer.
    int64_t bytesWritten;

    int64_t cursor;       // cursor in file.
    int64_t lastFlushed;  // the position last flushed
    int64_t nextSeqNo;
    mutex mut;
    shared_ptr<Checksum> checksum;
    shared_ptr<FileSystemImpl> filesystem;
    shared_ptr<LocatedBlock> lastBlock;
    shared_ptr<Packet> currentPacket;
    shared_ptr<Pipeline> pipeline;
    shared_ptr<SessionConfig> conf;
    std::string path;
    std::vector<char> buffer;
    steady_clock::time_point lastSend;
// thread heartBeatSender;

#ifdef MOCK
private:
    Hdfs::Mock::PipelineStub *stub;
#endif
};
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAMIMPL_H_ */
