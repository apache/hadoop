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

#ifndef _HDFS_LIBHDFS3_SERVER_NAMENODEIMPL_H_
#define _HDFS_LIBHDFS3_SERVER_NAMENODEIMPL_H_

#include "Namenode.h"

namespace hdfs {
namespace internal {

class NamenodeImpl : public Namenode {
public:
    NamenodeImpl(const char *host, const char *port,
                 const std::string &tokenService, const SessionConfig &c,
                 const RpcAuth &a);

    ~NamenodeImpl();

    // Idempotent
    void getBlockLocations(
        const std::string &src, int64_t offset, int64_t length,
        LocatedBlocks &lbs) /* throw (AccessControlException,
             FileNotFoundException, UnresolvedLinkException,
             HdfsIOException) */;

    void create(const std::string &src, const Permission &masked,
                const std::string &clientName, int flag, bool createParent,
                short replication,
                int64_t blockSize) /* throw (AccessControlException,
             AlreadyBeingCreatedException, DSQuotaExceededException,
             FileAlreadyExistsException, FileNotFoundException,
             NSQuotaExceededException, ParentNotDirectoryException,
             SafeModeException, UnresolvedLinkException, HdfsIOException) */;

    shared_ptr<LocatedBlock> append(const std::string &src,
                                    const std::string &clientName)
        /* throw (AccessControlException,
             DSQuotaExceededException, FileNotFoundException,
             SafeModeException, UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    bool setReplication(const std::string &src, short replication)
        /* throw (AccessControlException, DSQuotaExceededException,
     FileNotFoundException, SafeModeException, UnresolvedLinkException,
     HdfsIOException) */;

    // Idempotent
    void setPermission(const std::string &src, const Permission &permission)
        /* throw (AccessControlException, FileNotFoundException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    void setOwner(
        const std::string &src, const std::string &username,
        const std::string &groupname) /* throw (AccessControlException,
             FileNotFoundException, SafeModeException,
             UnresolvedLinkException, HdfsIOException) */;

    void abandonBlock(
        const ExtendedBlock &b, const std::string &src,
        const std::string &holder) /* throw (AccessControlException,
             FileNotFoundException, UnresolvedLinkException,
             HdfsIOException) */;

    shared_ptr<LocatedBlock> addBlock(
        const std::string &src, const std::string &clientName,
        const ExtendedBlock *previous,
        const std::vector<DatanodeInfo> &excludeNodes)
        /* throw (AccessControlException, FileNotFoundException,
     NotReplicatedYetException, SafeModeException,
     UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    shared_ptr<LocatedBlock> getAdditionalDatanode(
        const std::string &src, const ExtendedBlock &blk,
        const std::vector<DatanodeInfo> &existings,
        const std::vector<std::string> &storageIDs,
        const std::vector<DatanodeInfo> &excludes, int numAdditionalNodes,
        const std::string &clientName)
        /* throw (AccessControlException, FileNotFoundException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) */;

    bool complete(const std::string &src, const std::string &clientName,
                  const ExtendedBlock *last) /* throw (AccessControlException,
             FileNotFoundException, SafeModeException,
             UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    void reportBadBlocks(const std::vector<LocatedBlock> &blocks)
        /* throw (HdfsIOException) */;

    bool rename(const std::string &src, const std::string &dst)
        /* throw (UnresolvedLinkException, HdfsIOException) */;

    void concat(const std::string &trg, const std::vector<std::string> &srcs)
        /* throw (HdfsIOException, UnresolvedLinkException) */;

    /*void rename2(const std::string &src, const std::string &dst)
     throw (AccessControlException, DSQuotaExceededException,
     FileAlreadyExistsException, FileNotFoundException,
     NSQuotaExceededException, ParentNotDirectoryException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) ;*/

    bool deleteFile(const std::string &src, bool recursive)
        /* throw (AccessControlException, FileNotFoundException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    bool mkdirs(const std::string &src, const Permission &masked,
                bool createParent) /* throw (AccessControlException,
             FileAlreadyExistsException, FileNotFoundException,
             NSQuotaExceededException, ParentNotDirectoryException,
             SafeModeException, UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    bool getListing(const std::string &src, const std::string &startAfter,
                    bool needLocation, std::vector<FileStatus> &dl)
        /* throw (AccessControlException, FileNotFoundException,
     UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    void renewLease(const std::string &clientName)
        /* throw (AccessControlException, HdfsIOException) */;

    // Idempotent
    bool recoverLease(const std::string &src, const std::string &clientName)
        /* throw (HdfsIOException) */;

    // Idempotent
    std::vector<int64_t> getFsStats() /* throw (HdfsIOException) */;

    void metaSave(const std::string &filename) /* throw (HdfsIOException) */;

    // Idempotent
    FileStatus getFileInfo(const std::string &src)
        /* throw (AccessControlException, FileNotFoundException,
     UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    FileStatus getFileLinkInfo(const std::string &src)
        /* throw (AccessControlException, UnresolvedLinkException,
     HdfsIOException) */;

    /*    //Idempotent
        ContentSummary getContentSummary(const std::string &path)
         throw (AccessControlException, FileNotFoundException,
         UnresolvedLinkException, HdfsIOException) ;*/

    // Idempotent
    void setQuota(const std::string &path, int64_t namespaceQuota,
                  int64_t diskspaceQuota) /* throw (AccessControlException,
             FileNotFoundException, UnresolvedLinkException,
             HdfsIOException) */;

    // Idempotent
    void fsync(const std::string &src, const std::string &client)
        /* throw (AccessControlException, FileNotFoundException,
     UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    void setTimes(const std::string &src, int64_t mtime, int64_t atime)
        /* throw (AccessControlException, FileNotFoundException,
     UnresolvedLinkException, HdfsIOException) */;

    void createSymlink(const std::string &target, const std::string &link,
                       const Permission &dirPerm, bool createParent)
        /* throw (AccessControlException, FileAlreadyExistsException,
     FileNotFoundException, ParentNotDirectoryException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) */;

    // Idempotent
    std::string getLinkTarget(const std::string &path)
        /* throw (AccessControlException, FileNotFoundException,
     HdfsIOException) */;

    // Idempotent
    shared_ptr<LocatedBlock> updateBlockForPipeline(
        const ExtendedBlock &block, const std::string &clientName)
        /* throw (HdfsIOException) */;

    void updatePipeline(const std::string &clientName,
                        const ExtendedBlock &oldBlock,
                        const ExtendedBlock &newBlock,
                        const std::vector<DatanodeInfo> &newNodes,
                        const std::vector<std::string> &
                            storageIDs) /* throw (HdfsIOException) */;

    // Idempotent
    Token getDelegationToken(const std::string &renewer)
        /* throws IOException*/;

    // Idempotent
    int64_t renewDelegationToken(const Token &token)
        /*throws IOException*/;

    // Idempotent
    void cancelDelegationToken(const Token &token)
        /*throws IOException*/;

private:
    void invoke(const RpcCall &call);
    NamenodeImpl(const NamenodeImpl &other);
    NamenodeImpl &operator=(const NamenodeImpl &other);

    RpcAuth auth;
    RpcClient &client;
    RpcConfig conf;
    RpcProtocolInfo protocol;
    RpcServerInfo server;
};
}
}

#endif /* _HDFS_LIBHDFS3_SERVER_NAMENODEIMPL_H_ */
