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

#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemImpl.h"
#include "InputStreamImpl.h"
#include "LocalBlockReader.h"
#include "Logger.h"
#include "RemoteBlockReader.h"
#include "Thread.h"
#include "UnorderedMap.h"
#include "server/Datanode.h"

#include <algorithm>
#include <ifaddrs.h>
#include <inttypes.h>
#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace hdfs {
namespace internal {

mutex InputStreamImpl::MutLocalBlockInforCache;
unordered_map<uint32_t, shared_ptr<LocalBlockInforCacheType>>
    InputStreamImpl::LocalBlockInforCache;

unordered_set<std::string> BuildLocalAddrSet() {
    unordered_set<std::string> set;
    struct ifaddrs *ifAddr = NULL;
    struct ifaddrs *pifAddr = NULL;
    struct sockaddr *addr;

    if (getifaddrs(&ifAddr)) {
        THROW(HdfsNetworkException,
              "InputStreamImpl: cannot get local network interface: %s",
              GetSystemErrorInfo(errno));
    }

    try {
        std::vector<char> host;
        const char *pHost;
        host.resize(INET6_ADDRSTRLEN + 1);

        for (pifAddr = ifAddr; pifAddr != NULL; pifAddr = pifAddr->ifa_next) {
            addr = pifAddr->ifa_addr;
            memset(&host[0], 0, INET6_ADDRSTRLEN + 1);

            if (addr->sa_family == AF_INET) {
                pHost = inet_ntop(
                    addr->sa_family,
                    &(reinterpret_cast<struct sockaddr_in *>(addr))->sin_addr,
                    &host[0], INET6_ADDRSTRLEN);
            } else if (addr->sa_family == AF_INET6) {
                pHost = inet_ntop(
                    addr->sa_family,
                    &(reinterpret_cast<struct sockaddr_in6 *>(addr))->sin6_addr,
                    &host[0], INET6_ADDRSTRLEN);
            } else {
                continue;
            }

            if (NULL == pHost) {
                THROW(HdfsNetworkException,
                      "InputStreamImpl: cannot get convert network address "
                      "to textual form: %s",
                      GetSystemErrorInfo(errno));
            }

            set.insert(pHost);
        }

        /*
         * add hostname.
         */
        long hostlen = sysconf(_SC_HOST_NAME_MAX);
        host.resize(hostlen + 1);

        if (gethostname(&host[0], host.size())) {
            THROW(HdfsNetworkException,
                  "InputStreamImpl: cannot get hostname: %s",
                  GetSystemErrorInfo(errno));
        }

        set.insert(&host[0]);
    } catch (...) {
        if (ifAddr != NULL) {
            freeifaddrs(ifAddr);
        }

        throw;
    }

    if (ifAddr != NULL) {
        freeifaddrs(ifAddr);
    }

    return set;
}

InputStreamImpl::InputStreamImpl()
    : closed(true),
      localRead(true),
      readFromUnderConstructedBlock(false),
      verify(true),
      maxGetBlockInfoRetry(3),
      cursor(0),
      endOfCurBlock(0),
      lastBlockBeingWrittenLength(0),
      prefetchSize(0) {
#ifdef MOCK
    stub = NULL;
#endif
}

InputStreamImpl::~InputStreamImpl() {
}

void InputStreamImpl::checkStatus() {
    if (closed) {
        THROW(HdfsIOException, "InputStreamImpl: stream is not opened.");
    }

    if (lastError != exception_ptr()) {
        rethrow_exception(lastError);
    }
}

int64_t InputStreamImpl::readBlockLength(const LocatedBlock &b) {
    const std::vector<DatanodeInfo> &nodes = b.getLocations();
    int replicaNotFoundCount = nodes.size();

    for (size_t i = 0; i < nodes.size(); ++i) {
        try {
            int64_t n = 0;
            shared_ptr<Datanode> dn;
            RpcAuth a = auth;
            a.getUser().addToken(b.getToken());
#ifdef MOCK

            if (stub) {
                dn = stub->getDatanode();
            } else {
                dn = shared_ptr<Datanode>(
                    new DatanodeImpl(nodes[i].getIpAddr().c_str(),
                                     nodes[i].getIpcPort(), *conf, a));
            }

#else
            dn = shared_ptr<Datanode>(new DatanodeImpl(
                nodes[i].getIpAddr().c_str(), nodes[i].getIpcPort(), *conf, a));
#endif
            n = dn->getReplicaVisibleLength(b);

            if (n >= 0) {
                return n;
            }
        } catch (const ReplicaNotFoundException &e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block "
                "visible length for Block: %s file %s from Datanode: %s\n%s",
                b.toString().c_str(), path.c_str(),
                nodes[i].formatAddress().c_str(), GetExceptionDetail(e));
            LOG(INFO,
                "InputStreamImpl: retry get block visible length for Block: "
                "%s file %s from other datanode",
                b.toString().c_str(), path.c_str());
            --replicaNotFoundCount;
        } catch (const HdfsIOException &e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block visible length for "
                "Block: %s file %s from Datanode: %s\n%s",
                b.toString().c_str(), path.c_str(),
                nodes[i].formatAddress().c_str(), GetExceptionDetail(e));
            LOG(INFO,
                "InputStreamImpl: retry get block visible length for Block: "
                "%s file %s from other datanode",
                b.toString().c_str(), path.c_str());
        }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all 3 because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
        return 0;
    }

    return -1;
}

/**
 * Getting blocks locations'information from namenode
 */
void InputStreamImpl::updateBlockInfos() {
    int retry = maxGetBlockInfoRetry;

    for (int i = 0; i < retry; ++i) {
        try {
            if (!lbs) {
                lbs = shared_ptr<LocatedBlocks>(new LocatedBlocks);
            }

            filesystem->getBlockLocations(path, cursor, prefetchSize, *lbs);

            if (lbs->isLastBlockComplete()) {
                lastBlockBeingWrittenLength = 0;
            } else {
                shared_ptr<LocatedBlock> last = lbs->getLastBlock();

                if (!last) {
                    lastBlockBeingWrittenLength = 0;
                } else {
                    lastBlockBeingWrittenLength = readBlockLength(*last);

                    if (lastBlockBeingWrittenLength == -1) {
                        if (i + 1 >= retry) {
                            THROW(HdfsIOException,
                                  "InputStreamImpl: failed "
                                  "to get block visible length for Block: "
                                  "%s from all Datanode.",
                                  last->toString().c_str());
                        } else {
                            LOG(LOG_ERROR,
                                "InputStreamImpl: failed to get block visible "
                                "length for Block: %s file %s from all "
                                "Datanode.",
                                last->toString().c_str(), path.c_str());

                            try {
                                sleep_for(milliseconds(4000));
                            } catch (...) {
                            }

                            continue;
                        }
                    }

                    last->setNumBytes(lastBlockBeingWrittenLength);
                }
            }

            return;
        } catch (const HdfsRpcException &e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block information "
                "for file %s, %s",
                path.c_str(), GetExceptionDetail(e));

            if (i + 1 >= retry) {
                throw;
            }
        }

        LOG(INFO,
            "InputStreamImpl: retry to get block information for "
            "file: %s, already tried %d time(s).",
            path.c_str(), i + 1);
    }
}

int64_t InputStreamImpl::getFileLength() {
    int64_t length = lbs->getFileLength();

    if (!lbs->isLastBlockComplete()) {
        length += lastBlockBeingWrittenLength;
    }

    return length;
}

void InputStreamImpl::seekToBlock(const LocatedBlock &lb) {
    if (cursor >= lbs->getFileLength()) {
        assert(!lbs->isLastBlockComplete());
        readFromUnderConstructedBlock = true;
    } else {
        readFromUnderConstructedBlock = false;
    }

    assert(cursor >= lb.getOffset() &&
           cursor < lb.getOffset() + lb.getNumBytes());
    curBlock = shared_ptr<LocatedBlock>(new LocatedBlock(lb));
    int64_t blockSize = curBlock->getNumBytes();
    assert(blockSize > 0);
    endOfCurBlock = blockSize + curBlock->getOffset();
    failedNodes.clear();
    blockReader.reset();
}

bool InputStreamImpl::choseBestNode() {
    const std::vector<DatanodeInfo> &nodes = curBlock->getLocations();

    for (size_t i = 0; i < nodes.size(); ++i) {
        if (std::binary_search(failedNodes.begin(), failedNodes.end(),
                               nodes[i])) {
            continue;
        }

        curNode = nodes[i];
        return true;
    }

    return false;
}

bool InputStreamImpl::isLocalNode() {
    static const unordered_set<std::string> LocalAddrSet = BuildLocalAddrSet();
    bool retval = LocalAddrSet.find(curNode.getIpAddr()) != LocalAddrSet.end();
    return retval;
}

BlockLocalPathInfo InputStreamImpl::getBlockLocalPathInfo(
    LocalBlockInforCacheType &cache, const LocatedBlock &b) {
    BlockLocalPathInfo retval;

    try {
        if (!cache.find(LocalBlockInforCacheKey(b.getBlockId(), b.getPoolId()),
                        retval)) {
            RpcAuth a = auth;
            /*
             * only kerberos based authentication is allowed, do not add token
             */
            shared_ptr<Datanode> dn = shared_ptr<Datanode>(new DatanodeImpl(
                curNode.getIpAddr().c_str(), curNode.getIpcPort(), *conf, a));
            dn->getBlockLocalPathInfo(b, b.getToken(), retval);
            cache.insert(LocalBlockInforCacheKey(b.getBlockId(), b.getPoolId()),
                         retval);
        }
    } catch (const HdfsIOException &e) {
        throw;
    } catch (const HdfsException &e) {
        NESTED_THROW(
            HdfsIOException,
            "InputStreamImpl: Failed to get block local path information.");
    }

    return retval;
}

void InputStreamImpl::invalidCacheEntry(LocalBlockInforCacheType &cache,
                                        const LocatedBlock &b) {
    cache.erase(LocalBlockInforCacheKey(b.getBlockId(), b.getPoolId()));
}

LocalBlockInforCacheType &InputStreamImpl::getBlockLocalPathInfoCache(
    uint32_t port) {
    lock_guard<mutex> lock(MutLocalBlockInforCache);
    unordered_map<uint32_t, shared_ptr<LocalBlockInforCacheType>>::iterator it;
    it = LocalBlockInforCache.find(port);

    if (it == LocalBlockInforCache.end()) {
        shared_ptr<LocalBlockInforCacheType> retval;
        retval =
            shared_ptr<LocalBlockInforCacheType>(new LocalBlockInforCacheType(
                conf->getMaxLocalBlockInfoCacheSize()));
        LocalBlockInforCache[port] = retval;
        return *retval;
    } else {
        return *(it->second);
    }
}

void InputStreamImpl::setupBlockReader(bool temporaryDisableLocalRead) {
    bool lastReadFromLocal = false;
    exception_ptr lastException;

    while (true) {
        if (!choseBestNode()) {
            try {
                if (lastException) {
                    rethrow_exception(lastException);
                }
            } catch (...) {
                NESTED_THROW(
                    HdfsIOException,
                    "InputStreamImpl: all nodes have been tried and no valid "
                    "replica can be read for Block: %s.",
                    curBlock->toString().c_str());
            }

            THROW(HdfsIOException,
                  "InputStreamImpl: all nodes have been tried and no valid "
                  "replica can be read for Block: %s.",
                  curBlock->toString().c_str());
        }

        try {
            int64_t offset, len;
            offset = cursor - curBlock->getOffset();
            assert(offset >= 0);
            len = curBlock->getNumBytes() - offset;
            assert(len > 0);

            if (auth.getProtocol() == AuthProtocol::NONE &&
                !temporaryDisableLocalRead && !lastReadFromLocal &&
                !readFromUnderConstructedBlock && localRead && isLocalNode()) {
                lastReadFromLocal = true;
                LocalBlockInforCacheType &cache =
                    getBlockLocalPathInfoCache(curNode.getXferPort());
                BlockLocalPathInfo info =
                    getBlockLocalPathInfo(cache, *curBlock);
                assert(curBlock->getBlockId() == info.getBlock().getBlockId() &&
                       curBlock->getPoolId() == info.getBlock().getPoolId());
                LOG(DEBUG2,
                    "%p setup local block reader for file %s from "
                    "local block %s, block offset %" PRId64
                    ", read block "
                    "length %" PRId64 " end of Block %" PRId64
                    ", local "
                    "block file path %s",
                    this, path.c_str(), curBlock->toString().c_str(), offset,
                    len, offset + len, info.getLocalBlockPath());

                if (0 != access(info.getLocalMetaPath(), R_OK)) {
                    invalidCacheEntry(cache, *curBlock);
                    continue;
                }

                try {
                    blockReader = shared_ptr<BlockReader>(
                        new LocalBlockReader(info, *curBlock, offset, verify,
                                             *conf, localReaderBuffer));
                } catch (...) {
                    invalidCacheEntry(cache, *curBlock);
                    throw;
                }
            } else {
                const char *clientName;
                LOG(DEBUG2,
                    "%p setup remote block reader for file %s from "
                    "remote block %s, block offset %" PRId64
                    ""
                    ", read block length %" PRId64 " end of block %" PRId64
                    ", from node %s",
                    this, path.c_str(), curBlock->toString().c_str(), offset,
                    len, offset + len, curNode.formatAddress().c_str());
                clientName = filesystem->getClientName();
                lastReadFromLocal = false;
                blockReader = shared_ptr<BlockReader>(new RemoteBlockReader(
                    *curBlock, curNode, offset, len, curBlock->getToken(),
                    clientName, verify, *conf));
            }

            break;
        } catch (const HdfsIOException &e) {
            lastException = current_exception();

            if (lastReadFromLocal) {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s "
                    "on Datanode: %s.\n%s\n"
                    "retry the same node but disable reading from local block",
                    curBlock->toString().c_str(), path.c_str(),
                    curNode.formatAddress().c_str(), GetExceptionDetail(e));
                /*
                 * do not add node into failedNodes since we will retry the same
                 * node
                 * but
                 * disable local block reading
                 */
            } else {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on "
                    "Datanode: %s.\n%s\nretry another node",
                    curBlock->toString().c_str(), path.c_str(),
                    curNode.formatAddress().c_str(), GetExceptionDetail(e));
                failedNodes.push_back(curNode);
                std::sort(failedNodes.begin(), failedNodes.end());
            }
        }
    }
}

void InputStreamImpl::open(shared_ptr<FileSystemImpl> fs, const char *path,
                           bool verifyChecksum) {
    if (NULL == path || 0 == strlen(path)) {
        THROW(InvalidParameter, "path is invalid.");
    }

    try {
        openInternal(fs, path, verifyChecksum);
    } catch (...) {
        close();
        throw;
    }
}

void InputStreamImpl::openInternal(shared_ptr<FileSystemImpl> fs,
                                   const char *path, bool verifyChecksum) {
    try {
        filesystem = fs;
        verify = verifyChecksum;
        this->path = fs->getStandardPath(path);
        LOG(DEBUG2, "%p, open file %s for read, verfyChecksum is %s", this,
            this->path.c_str(), (verifyChecksum ? "true" : "false"));
        conf = shared_ptr<SessionConfig>(new SessionConfig(fs->getConf()));
        this->auth = RpcAuth(fs->getUserInfo(),
                             RpcAuth::ParseMethod(conf->getRpcAuthMethod()));
        prefetchSize = conf->getDefaultBlockSize() * conf->getPrefetchSize();
        localRead = conf->isReadFromLocal();
        maxGetBlockInfoRetry = conf->getMaxGetBlockInfoRetry();
        updateBlockInfos();
        closed = false;
    } catch (const HdfsCanceled &e) {
        throw;
    } catch (const FileNotFoundException &e) {
        throw;
    } catch (const HdfsException &e) {
        NESTED_THROW(HdfsIOException, "InputStreamImpl: cannot open file: %s.",
                     this->path.c_str());
    }
}

int32_t InputStreamImpl::read(char *buf, int32_t size) {
    checkStatus();

    try {
        int64_t prvious = cursor;
        int32_t done = readInternal(buf, size);
        LOG(DEBUG3, "%p read file %s size is %d, offset %" PRId64
                    " done %d, "
                    "next pos %" PRId64,
            this, path.c_str(), size, prvious, done, cursor);
        return done;
    } catch (const HdfsEndOfStream &e) {
        throw;
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

int32_t InputStreamImpl::readOneBlock(char *buf, int32_t size,
                                      bool shouldUpdateMetadataOnFailure) {
    bool temporaryDisableLocalRead = false;

    while (true) {
        try {
            /*
             * Setup block reader here and handle failure.
             */
            if (!blockReader) {
                setupBlockReader(temporaryDisableLocalRead);
                temporaryDisableLocalRead = false;
            }
        } catch (const HdfsInvalidBlockToken &e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s, \n%s, "
                "retry after updating block informations.",
                curBlock->toString().c_str(), path.c_str(),
                GetExceptionDetail(e));
            return -1;
        } catch (const HdfsIOException &e) {
            /*
             * In setupBlockReader, we have tried all the replicas.
             * We now update block informations once, and try again.
             */
            if (shouldUpdateMetadataOnFailure) {
                LOG(LOG_ERROR,
                    "InputStreamImpl: failed to read Block: %s file %s, \n%s, "
                    "retry after updating block informations.",
                    curBlock->toString().c_str(), path.c_str(),
                    GetExceptionDetail(e));
                return -1;
            } else {
                /*
                 * We have updated block informations and failed again.
                 */
                throw;
            }
        }

        /*
         * Block reader has been setup, read from block reader.
         */
        try {
            int32_t todo = size;
            todo = todo < endOfCurBlock - cursor
                       ? todo
                       : static_cast<int32_t>(endOfCurBlock - cursor);
            assert(blockReader);
            todo = blockReader->read(buf, todo);
            cursor += todo;
            /*
             * Exit the loop and function from here if success.
             */
            return todo;
        } catch (const HdfsIOException &e) {
            /*
             * Failed to read from current block reader,
             * add the current datanode to invalid node list and try again.
             */
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s from "
                "Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e));

            if (conf->doesNotRetryAnotherNode()) {
                throw;
            }
        } catch (const ChecksumException &e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s "
                "from Datanode: %s, \n%s, retry read again from "
                "another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e));
        }

        /*
         * Successfully create the block reader but failed to read.
         * Disable the local block reader and try the same node again.
         */
        if (!blockReader ||
            dynamic_cast<LocalBlockReader *>(blockReader.get())) {
            temporaryDisableLocalRead = true;
        } else {
            /*
             * Remote block reader failed to read, try another node.
             */
            LOG(INFO,
                "IntputStreamImpl: Add invalid datanode %s to failed "
                "datanodes and try another datanode again for file %s.",
                curNode.formatAddress().c_str(), path.c_str());
            failedNodes.push_back(curNode);
            std::sort(failedNodes.begin(), failedNodes.end());
        }

        blockReader.reset();
    }
}

/**
 * To read data from hdfs.
 * @param buf the buffer used to filled.
 * @param size buffer size.
 * @return return the number of bytes filled in the buffer, it may less than
 * size.
 */
int32_t InputStreamImpl::readInternal(char *buf, int32_t size) {
    int updateMetadataOnFailure = conf->getMaxReadBlockRetry();

    try {
        do {
            const LocatedBlock *lb = NULL;

            /*
             * Check if we have got the block information we need.
             */
            if (!lbs || cursor >= getFileLength() ||
                (cursor >= endOfCurBlock && !(lb = lbs->findBlock(cursor)))) {
                /*
                 * Get block information from namenode.
                 * Do RPC failover work in updateBlockInfos.
                 */
                updateBlockInfos();

                /*
                 * We already have the up-to-date block information,
                 * Check if we reach the end of file.
                 */
                if (cursor >= getFileLength()) {
                    THROW(HdfsEndOfStream,
                          "InputStreamImpl: read over EOF, current position: "
                          "%" PRId64 ", read size: %d, from file: %s",
                          cursor, size, path.c_str());
                }
            }

            /*
             * If we reach the end of block or the block information has just
             * updated,
             * seek to the right block to read.
             */
            if (cursor >= endOfCurBlock) {
                lb = lbs->findBlock(cursor);

                if (!lb) {
                    THROW(HdfsIOException,
                          "InputStreamImpl: cannot find block information at "
                          "position: %" PRId64 " for file: %s",
                          cursor, path.c_str());
                }

                /*
                 * Seek to the right block, setup all needed variable,
                 * but do not setup block reader, setup it latter.
                 */
                seekToBlock(*lb);
            }

            int32_t retval =
                readOneBlock(buf, size, updateMetadataOnFailure > 0);

            /*
             * Now we have tried all replicas and failed.
             * We will update metadata once and try again.
             */
            if (retval < 0) {
                lbs.reset();
                endOfCurBlock = 0;
                --updateMetadataOnFailure;

                try {
                    sleep_for(seconds(1));
                } catch (...) {
                }

                continue;
            }

            return retval;
        } while (true);
    } catch (const HdfsCanceled &e) {
        throw;
    } catch (const HdfsEndOfStream &e) {
        throw;
    } catch (const HdfsException &e) {
        /*
         * wrap the underlying error and rethrow.
         */
        NESTED_THROW(HdfsIOException,
                     "InputStreamImpl: cannot read file: %s, from "
                     "position %" PRId64 ", size: %d.",
                     path.c_str(), cursor, size);
    }
}

/**
 * To read data from hdfs, block until get the given size of bytes.
 * @param buf the buffer used to filled.
 * @param size the number of bytes to be read.
 */
void InputStreamImpl::readFully(char *buf, int64_t size) {
    LOG(DEBUG3, "readFully file %s size is %" PRId64 ", offset %" PRId64,
        path.c_str(), size, cursor);
    checkStatus();

    try {
        return readFullyInternal(buf, size);
    } catch (const HdfsEndOfStream &e) {
        throw;
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

void InputStreamImpl::readFullyInternal(char *buf, int64_t size) {
    int32_t done;
    int64_t pos = cursor, todo = size;

    try {
        while (todo > 0) {
            done = todo < std::numeric_limits<int32_t>::max()
                       ? static_cast<int32_t>(todo)
                       : std::numeric_limits<int32_t>::max();
            done = readInternal(buf + (size - todo), done);
            todo -= done;
        }
    } catch (const HdfsCanceled &e) {
        throw;
    } catch (const HdfsEndOfStream &e) {
        THROW(HdfsEndOfStream,
              "InputStreamImpl: read over EOF, current position: %" PRId64
              ", read size: %" PRId64 ", from file: %s",
              pos, size, path.c_str());
    } catch (const HdfsException &e) {
        NESTED_THROW(HdfsIOException,
                     "InputStreamImpl: cannot read fully from file: %s, "
                     "from position %" PRId64 ", size: %" PRId64 ".",
                     path.c_str(), pos, size);
    }
}

int64_t InputStreamImpl::available() {
    checkStatus();

    try {
        if (blockReader) {
            return blockReader->available();
        }
    } catch (...) {
        lastError = current_exception();
        throw;
    }

    return 0;
}

/**
 * To move the file point to the given position.
 * @param size the given position.
 */
void InputStreamImpl::seek(int64_t pos) {
    LOG(DEBUG2, "%p seek file %s to %" PRId64 ", offset %" PRId64, this,
        path.c_str(), pos, cursor);
    checkStatus();

    try {
        seekInternal(pos);
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

void InputStreamImpl::seekInternal(int64_t pos) {
    if (cursor == pos) {
        return;
    }

    if (!lbs || pos > getFileLength()) {
        updateBlockInfos();

        if (pos > getFileLength()) {
            THROW(HdfsEndOfStream,
                  "InputStreamImpl: seek over EOF, current position: %" PRId64
                  ", seek target: %" PRId64 ", in file: %s",
                  cursor, pos, path.c_str());
        }
    }

    try {
        if (blockReader && pos > cursor && pos < endOfCurBlock) {
            blockReader->skip(pos - cursor);
            cursor = pos;
            return;
        }
    } catch (const HdfsIOException &e) {
        LOG(LOG_ERROR, "InputStreamImpl: failed to skip %" PRId64
                       " bytes in current block reader for file %s\n%s",
            pos - cursor, path.c_str(), GetExceptionDetail(e));
        LOG(INFO, "InputStreamImpl: retry to seek to position %" PRId64
                  " for file %s",
            pos, path.c_str());
    } catch (const ChecksumException &e) {
        LOG(LOG_ERROR, "InputStreamImpl: failed to skip %" PRId64
                       " bytes in current block reader for file %s\n%s",
            pos - cursor, path.c_str(), GetExceptionDetail(e));
        LOG(INFO, "InputStreamImpl: retry to seek to position %" PRId64
                  " for file %s",
            pos, path.c_str());
    }

    /**
     * the seek target exceed the current block or skip failed in current block
     * reader.
     * reset current block reader and set the cursor to the target position to
     * seek.
     */
    endOfCurBlock = 0;
    blockReader.reset();
    cursor = pos;
}

/**
 * To get the current file point position.
 * @return the position of current file point.
 */
int64_t InputStreamImpl::tell() {
    checkStatus();
    LOG(DEBUG2, "tell file %s at %" PRId64, path.c_str(), cursor);
    return cursor;
}

/**
 * Close the stream.
 */
void InputStreamImpl::close() {
    LOG(DEBUG2, "%p close file %s for read", this, path.c_str());
    closed = true;
    localRead = true;
    readFromUnderConstructedBlock = false;
    verify = true;
    filesystem.reset();
    cursor = 0;
    endOfCurBlock = 0;
    lastBlockBeingWrittenLength = 0;
    prefetchSize = 0;
    blockReader.reset();
    curBlock.reset();
    lbs.reset();
    conf.reset();
    failedNodes.clear();
    path.clear();
    localReaderBuffer.resize(0);
    lastError = exception_ptr();
}

std::string InputStreamImpl::toString() {
    if (path.empty()) {
        return std::string("InputStream for path ") + path;
    } else {
        return std::string("InputStream (not opened)");
    }
}
}
}
