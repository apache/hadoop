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

#ifndef _HDFS_LIBHDFS3_SERVER_NAMENODEPROXY_H_
#define _HDFS_LIBHDFS3_SERVER_NAMENODEPROXY_H_

#include "Namenode.h"
#include "NamenodeInfo.h"
#include "SharedPtr.h"
#include "Thread.h"

namespace hdfs {
namespace internal {

class NamenodeProxy: public Namenode {
public:
    NamenodeProxy(const std::vector<NamenodeInfo> &namenodeInfos,
            const std::string &tokenService,
            const SessionConfig &c, const RpcAuth &a);
    ~NamenodeProxy();

public:

    void getBlockLocations(const std::string &src, int64_t offset,
            int64_t length, LocatedBlocks &lbs);

    void create(const std::string &src, const Permission &masked,
            const std::string &clientName, int flag, bool createParent,
            short replication, int64_t blockSize);

    shared_ptr<LocatedBlock> append(const std::string &src,
            const std::string &clientName);

    bool setReplication(const std::string &src, short replication);

    void setPermission(const std::string &src, const Permission &permission);

    void setOwner(const std::string &src, const std::string &username,
            const std::string &groupname);

    void abandonBlock(const ExtendedBlock &b, const std::string &src,
            const std::string &holder);

    shared_ptr<LocatedBlock> addBlock(const std::string &src,
            const std::string &clientName, const ExtendedBlock *previous,
            const std::vector<DatanodeInfo> &excludeNodes);

    shared_ptr<LocatedBlock> getAdditionalDatanode(const std::string &src,
            const ExtendedBlock &blk,
            const std::vector<DatanodeInfo> &existings,
            const std::vector<std::string> &storageIDs,
            const std::vector<DatanodeInfo> &excludes, int numAdditionalNodes,
            const std::string &clientName);

    bool complete(const std::string &src, const std::string &clientName,
            const ExtendedBlock *last);

    void reportBadBlocks(const std::vector<LocatedBlock> &blocks);

    bool rename(const std::string &src, const std::string &dst);

    void concat(const std::string &trg, const std::vector<std::string> &srcs);

    /*void rename2(const std::string &src, const std::string &dst)
     throw (AccessControlException, DSQuotaExceededException,
     FileAlreadyExistsException, FileNotFoundException,
     NSQuotaExceededException, ParentNotDirectoryException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) ;*/

    bool deleteFile(const std::string &src, bool recursive);

    bool mkdirs(const std::string &src, const Permission &masked,
            bool createParent);

    bool getListing(const std::string &src, const std::string &startAfter,
            bool needLocation, std::vector<FileStatus> &dl);

    void renewLease(const std::string &clientName);

    bool recoverLease(const std::string &src, const std::string &clientName);

    std::vector<int64_t> getFsStats();

    void metaSave(const std::string &filename);

    FileStatus getFileInfo(const std::string &src);

    FileStatus getFileLinkInfo(const std::string &src);

    void setQuota(const std::string &path, int64_t namespaceQuota,
            int64_t diskspaceQuota);

    void fsync(const std::string &src, const std::string &client);

    void setTimes(const std::string &src, int64_t mtime, int64_t atime);

    void createSymlink(const std::string &target, const std::string &link,
            const Permission &dirPerm, bool createParent);

    std::string getLinkTarget(const std::string &path);

    shared_ptr<LocatedBlock> updateBlockForPipeline(const ExtendedBlock &block,
            const std::string &clientName);

    void updatePipeline(const std::string &clientName,
            const ExtendedBlock &oldBlock, const ExtendedBlock &newBlock,
            const std::vector<DatanodeInfo> &newNodes,
            const std::vector<std::string> &storageIDs);

    Token getDelegationToken(const std::string &renewer);

    int64_t renewDelegationToken(const Token &token);

    void cancelDelegationToken(const Token &token);

    void close();

private:
    shared_ptr<Namenode> getActiveNamenode(uint32_t &oldValue);
    void failoverToNextNamenode(uint32_t oldValue);

private:
    bool enableNamenodeHA;
    int maxNamenodeHARetry;
    mutex mut;
    std::string clusterid;
    std::vector<shared_ptr<Namenode> > namenodes;
    uint32_t currentNamenode;
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_NAMENODEPROXY_H_ */
