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

#ifndef _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_
#define _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_

#include "platform.h"

#include "BlockReader.h"
#include "ExceptionInternal.h"
#include "FileSystem.h"
#include "Hash.h"
#include "InputStreamInter.h"
#include "LruMap.h"
#include "SessionConfig.h"
#include "SharedPtr.h"
#include "Thread.h"
#include "UnorderedMap.h"
#include "rpc/RpcAuth.h"
#include "server/BlockLocalPathInfo.h"
#include "server/Datanode.h"
#include "server/LocatedBlock.h"
#include "server/LocatedBlocks.h"

#ifdef MOCK
#include "TestDatanodeStub.h"
#endif

namespace hdfs {
namespace internal {

typedef std::pair<int64_t, std::string> LocalBlockInforCacheKey;
typedef LruMap<LocalBlockInforCacheKey, BlockLocalPathInfo> LocalBlockInforCacheType;

/**
 * A input stream used read data from hdfs.
 */
class InputStreamImpl: public InputStreamInter {
public:
    InputStreamImpl();
    ~InputStreamImpl();

    /**
     * Open a file to read
     * @param fs hdfs file system.
     * @param path the file to be read.
     * @param verifyChecksum verify the checksum.
     */
    void open(shared_ptr<FileSystemInter> fs, const char * path, bool verifyChecksum);

    /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
    int32_t read(char * buf, int32_t size);

    /**
     * To read data from hdfs, block until get the given size of bytes.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     */
    void readFully(char * buf, int64_t size);

    int64_t available();

    /**
     * To move the file point to the given position.
     * @param pos the given position.
     */
    void seek(int64_t pos);

    /**
     * To get the current file point position.
     * @return the position of current file point.
     */
    int64_t tell();

    /**
     * Close the stream.
     */
    void close();

    std::string toString();

private:
    BlockLocalPathInfo getBlockLocalPathInfo(LocalBlockInforCacheType & cache,
            const LocatedBlock & b);
    bool choseBestNode();
    bool isLocalNode();
    int32_t readInternal(char * buf, int32_t size);
    int32_t readOneBlock(char * buf, int32_t size, bool shouldUpdateMetadataOnFailure);
    int64_t getFileLength();
    int64_t readBlockLength(const LocatedBlock & b);
    LocalBlockInforCacheType & getBlockLocalPathInfoCache(uint32_t port);
    void checkStatus();
    void invalidCacheEntry(LocalBlockInforCacheType & cache,
                           const LocatedBlock & b);
    void openInternal(shared_ptr<FileSystemInter> fs, const char * path,
                      bool verifyChecksum);
    void readFullyInternal(char * buf, int64_t size);
    void seekInternal(int64_t pos);
    void seekToBlock(const LocatedBlock & lb);
    void setupBlockReader(bool temporaryDisableLocalRead);
    void updateBlockInfos();

private:
    bool closed;
    bool localRead;
    bool readFromUnderConstructedBlock;
    bool verify;
    DatanodeInfo curNode;
    exception_ptr lastError;
    FileStatus fileInfo;
    int maxGetBlockInfoRetry;
    int64_t cursor;
    int64_t endOfCurBlock;
    int64_t lastBlockBeingWrittenLength;
    int64_t prefetchSize;
    RpcAuth auth;
    shared_ptr<BlockReader> blockReader;
    shared_ptr<FileSystemInter> filesystem;
    shared_ptr<LocatedBlock> curBlock;
    shared_ptr<LocatedBlocks> lbs;
    shared_ptr<SessionConfig> conf;
    std::string path;
    std::vector<DatanodeInfo> failedNodes;
    std::vector<char> localReaderBuffer;

    static mutex MutLocalBlockInforCache;
    static unordered_map<uint32_t, shared_ptr<LocalBlockInforCacheType> > LocalBlockInforCache;
#ifdef MOCK
private:
    hdfs::mock::TestDatanodeStub * stub;
#endif
};

}
}

#ifdef NEED_BOOST

namespace boost {
template<>
struct hash<hdfs::internal::LocalBlockInforCacheKey> {
    std::size_t operator()(
        const hdfs::internal::LocalBlockInforCacheKey & key) const {
        size_t values[] = {hdfs::internal::Int64Hasher(key.first),
                           hdfs::internal::StringHasher(key.second)
                          };
        return hdfs::internal::CombineHasher(values,
                                             sizeof(values) / sizeof(values[0]));
    }
};
}

#else

namespace std {
template<>
struct hash<hdfs::internal::LocalBlockInforCacheKey> {
    std::size_t operator()(
        const hdfs::internal::LocalBlockInforCacheKey & key) const {
        size_t values[] = { hdfs::internal::Int64Hasher(key.first),
                            hdfs::internal::StringHasher(key.second)
                          };
        return hdfs::internal::CombineHasher(values,
                                             sizeof(values) / sizeof(values[0]));
    }
};
}

#endif

#endif /* _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_ */
