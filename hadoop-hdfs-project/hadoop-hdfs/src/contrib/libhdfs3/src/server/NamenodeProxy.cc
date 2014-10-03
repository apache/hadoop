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
#include "Logger.h"
#include "NamenodeImpl.h"
#include "NamenodeProxy.h"
#include "StringUtil.h"

#include <string>

#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/file.h>

namespace hdfs {
namespace internal {

static uint32_t GetInitNamenodeIndex(const std::string &id) {
    std::string path = "/tmp/";
    path += id;
    int fd;
    uint32_t index = 0;
    /*
     * try create the file
     */
    fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);

    if (fd < 0) {
        if (errno == EEXIST) {
            /*
             * the file already exist, try to open it
             */
            fd = open(path.c_str(), O_RDONLY);
        } else {
            /*
             * failed to create, do not care why
             */
            return 0;
        }
    } else {
        if (0 != flock(fd, LOCK_EX)) {
            /*
             * failed to lock
             */
            close(fd);
            return index;
        }

        /*
         * created file, initialize it with 0
         */
        write(fd, &index, sizeof(index));
        flock(fd, LOCK_UN);
        close(fd);
        return index;
    }

    /*
     * the file exist, read it.
     */
    if (fd >= 0) {
        if (0 != flock(fd, LOCK_SH)) {
            /*
             * failed to lock
             */
            close(fd);
            return index;
        }

        if (sizeof(index) != read(fd, &index, sizeof(index))) {
            /*
             * failed to read, do not care why
             */
            index = 0;
        }

        flock(fd, LOCK_UN);
        close(fd);
    }

    return index;
}

static void SetInitNamenodeIndex(const std::string &id, uint32_t index) {
    std::string path = "/tmp/";
    path += id;
    int fd;
    /*
     * try open the file for write
     */
    fd = open(path.c_str(), O_WRONLY);

    if (fd > 0) {
        if (0 != flock(fd, LOCK_EX)) {
            /*
             * failed to lock
             */
            close(fd);
            return;
        }

        write(fd, &index, sizeof(index));
        flock(fd, LOCK_UN);
        close(fd);
    }
}

NamenodeProxy::NamenodeProxy(const std::vector<NamenodeInfo> &namenodeInfos,
                             const std::string &tokenService,
                             const SessionConfig &c, const RpcAuth &a) :
    clusterid(tokenService), currentNamenode(0) {
    if (namenodeInfos.size() == 1) {
        enableNamenodeHA = false;
        maxNamenodeHARetry = 0;
    } else {
        enableNamenodeHA = true;
        maxNamenodeHARetry = c.getRpcMaxHaRetry();
    }

    for (size_t i = 0; i < namenodeInfos.size(); ++i) {
        std::vector<std::string> nninfo = StringSplit(namenodeInfos[i].getRpcAddr(), ":");

        if (nninfo.size() != 2) {
            THROW(InvalidParameter, "Cannot create namenode proxy, %s does not contain host or port",
                  namenodeInfos[i].getRpcAddr().c_str());
        }

        namenodes.push_back(
            shared_ptr<Namenode>(
                new NamenodeImpl(nninfo[0].c_str(), nninfo[1].c_str(), clusterid, c, a)));
    }

    if (enableNamenodeHA) {
        currentNamenode = GetInitNamenodeIndex(clusterid) % namenodeInfos.size();
    }
}

NamenodeProxy::~NamenodeProxy() {
}

shared_ptr<Namenode> NamenodeProxy::getActiveNamenode(uint32_t &oldValue) {
    lock_guard<mutex> lock(mut);

    if (namenodes.empty()) {
        THROW(HdfsFileSystemClosed, "NamenodeProxy is closed.");
    }

    oldValue = currentNamenode;
    return namenodes[currentNamenode % namenodes.size()];
}

void NamenodeProxy::failoverToNextNamenode(uint32_t oldValue) {
    lock_guard<mutex> lock(mut);

    if (oldValue != currentNamenode) {
        //already failover in another thread.
        return;
    }

    ++currentNamenode;
    currentNamenode = currentNamenode % namenodes.size();
    SetInitNamenodeIndex(clusterid, currentNamenode);
}

static void HandleHdfsFailoverException(const HdfsFailoverException &e) {
    try {
        rethrow_if_nested(e);
    } catch (...) {
        NESTED_THROW(hdfs::HdfsRpcException, "%s", e.what());
    }

    //should not reach here
    abort();
}

#define NAMENODE_HA_RETRY_BEGIN() \
    do { \
        int __count = 0; \
        do { \
            uint32_t __oldValue = 0; \
            shared_ptr<Namenode> namenode =  getActiveNamenode(__oldValue); \
            try { \
                (void)0

#define NAMENODE_HA_RETRY_END() \
    break; \
    } catch (const NameNodeStandbyException &e) { \
        if (!enableNamenodeHA || __count++ > maxNamenodeHARetry) { \
            throw; \
        } \
    } catch (const HdfsFailoverException &e) { \
        if (!enableNamenodeHA || __count++ > maxNamenodeHARetry) { \
            HandleHdfsFailoverException(e); \
        } \
    } \
    failoverToNextNamenode(__oldValue); \
    LOG(WARNING, "NamenodeProxy: Failover to another Namenode."); \
    } while (true); \
    } while (0)

void NamenodeProxy::getBlockLocations(const std::string &src, int64_t offset,
        int64_t length, LocatedBlocks &lbs) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->getBlockLocations(src, offset, length, lbs);
    NAMENODE_HA_RETRY_END();
}

void NamenodeProxy::create(const std::string &src, const Permission &masked,
        const std::string &clientName, int flag, bool createParent,
        short replication, int64_t blockSize) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->create(src, masked, clientName, flag, createParent, replication, blockSize);
    NAMENODE_HA_RETRY_END();
}

shared_ptr<LocatedBlock> NamenodeProxy::append(const std::string &src,
        const std::string &clientName) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->append(src, clientName);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return shared_ptr<LocatedBlock>();
}

bool NamenodeProxy::setReplication(const std::string &src, short replication) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->setReplication(src, replication);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return false;
}

void NamenodeProxy::setPermission(const std::string &src,
        const Permission &permission) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->setPermission(src, permission);
    NAMENODE_HA_RETRY_END();
}

void NamenodeProxy::setOwner(const std::string &src,
        const std::string &username, const std::string &groupname) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->setOwner(src, username, groupname);
    NAMENODE_HA_RETRY_END();
}

void NamenodeProxy::abandonBlock(const ExtendedBlock &b,
        const std::string &src, const std::string &holder) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->abandonBlock(b, src, holder);
    NAMENODE_HA_RETRY_END();
}

shared_ptr<LocatedBlock> NamenodeProxy::addBlock(const std::string &src,
        const std::string &clientName, const ExtendedBlock * previous,
        const std::vector<DatanodeInfo> &excludeNodes) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->addBlock(src, clientName, previous, excludeNodes);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return shared_ptr<LocatedBlock>();
}

shared_ptr<LocatedBlock> NamenodeProxy::getAdditionalDatanode(
    const std::string &src, const ExtendedBlock &blk,
    const std::vector<DatanodeInfo> &existings,
    const std::vector<std::string> &storageIDs,
    const std::vector<DatanodeInfo> &excludes, int numAdditionalNodes,
    const std::string &clientName) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getAdditionalDatanode(src, blk, existings,
                  storageIDs, excludes, numAdditionalNodes, clientName);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return shared_ptr<LocatedBlock>();
}

bool NamenodeProxy::complete(const std::string &src,
                  const std::string &clientName, const ExtendedBlock *last) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->complete(src, clientName, last);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return false;
}

/*void NamenodeProxy::reportBadBlocks(const std::vector<LocatedBlock> &blocks) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->reportBadBlocks(blocks);
    NAMENODE_HA_RETRY_END();
}*/

bool NamenodeProxy::rename(const std::string &src, const std::string &dst) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->rename(src, dst);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return false;
}

/*
void NamenodeProxy::concat(const std::string &trg,
                           const std::vector<std::string> &srcs) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->concat(trg, srcs);
    NAMENODE_HA_RETRY_END();
}
*/

bool NamenodeProxy::deleteFile(const std::string &src, bool recursive) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->deleteFile(src, recursive);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return false;
}

bool NamenodeProxy::mkdirs(const std::string &src, const Permission &masked,
          bool createParent) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->mkdirs(src, masked, createParent);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return false;
}

bool NamenodeProxy::getListing(const std::string &src,
          const std::string &startAfter, bool needLocation,
          std::vector<FileStatus> &dl) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getListing(src, startAfter, needLocation, dl);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return false;
}

void NamenodeProxy::renewLease(const std::string &clientName) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->renewLease(clientName);
    NAMENODE_HA_RETRY_END();
}

bool NamenodeProxy::recoverLease(const std::string &src,
                                 const std::string &clientName) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->recoverLease(src, clientName);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return false;
}

std::vector<int64_t> NamenodeProxy::getFsStats() {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getFsStats();
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return std::vector<int64_t>();
}

/*void NamenodeProxy::metaSave(const std::string &filename) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->metaSave(filename);
    NAMENODE_HA_RETRY_END();
}*/

FileStatus NamenodeProxy::getFileInfo(const std::string &src) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getFileInfo(src);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return FileStatus();
}

/*FileStatus NamenodeProxy::getFileLinkInfo(const std::string &src) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getFileLinkInfo(src);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return FileStatus();
}*/

/*ContentSummary NamenodeProxy::getContentSummary(const std::string &path) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getContentSummary(path);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return ContentSummary();
}*/

/*void NamenodeProxy::setQuota(const std::string &path, int64_t namespaceQuota,
                             int64_t diskspaceQuota) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->setQuota(path, namespaceQuota, diskspaceQuota);
    NAMENODE_HA_RETRY_END();
}*/

void NamenodeProxy::fsync(const std::string &src, const std::string &client) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->fsync(src, client);
    NAMENODE_HA_RETRY_END();
}

void NamenodeProxy::setTimes(const std::string &src, int64_t mtime,
                             int64_t atime) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->setTimes(src, mtime, atime);
    NAMENODE_HA_RETRY_END();
}

/*void NamenodeProxy::createSymlink(const std::string &target,
                const std::string &link, const Permission &dirPerm,
                bool createParent) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->createSymlink(target, link, dirPerm, createParent);
    NAMENODE_HA_RETRY_END();
}*/

/*std::string NamenodeProxy::getLinkTarget(const std::string &path) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getLinkTarget(path);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return "";
}*/

shared_ptr<LocatedBlock> NamenodeProxy::updateBlockForPipeline(
          const ExtendedBlock &block, const std::string &clientName) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->updateBlockForPipeline(block, clientName);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return shared_ptr<LocatedBlock>();
}

void NamenodeProxy::updatePipeline(const std::string &clientName,
          const ExtendedBlock &oldBlock, const ExtendedBlock &newBlock,
          const std::vector<DatanodeInfo> &newNodes,
          const std::vector<std::string> &storageIDs) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->updatePipeline(clientName, oldBlock, newBlock,
                             newNodes, storageIDs);
    NAMENODE_HA_RETRY_END();
}

Token NamenodeProxy::getDelegationToken(const std::string &renewer) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->getDelegationToken(renewer);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return Token();
}

int64_t NamenodeProxy::renewDelegationToken(const Token &token) {
    NAMENODE_HA_RETRY_BEGIN();
    return namenode->renewDelegationToken(token);
    NAMENODE_HA_RETRY_END();
    assert(!"should not reach here");
    return 0;
}

void NamenodeProxy::cancelDelegationToken(const Token &token) {
    NAMENODE_HA_RETRY_BEGIN();
    namenode->cancelDelegationToken(token);
    NAMENODE_HA_RETRY_END();
}

void NamenodeProxy::close() {
    lock_guard<mutex> lock(mut);
    namenodes.clear();
}

}
}
