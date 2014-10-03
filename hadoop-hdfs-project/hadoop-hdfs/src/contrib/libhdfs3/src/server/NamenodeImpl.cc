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

#include "ClientNamenodeProtocol.pb.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Namenode.h"
#include "NamenodeImpl.h"
#include "rpc/RpcCall.h"
#include "rpc/RpcClient.h"
#include "RpcHelper.h"

#define NAMENODE_VERSION 1
#define NAMENODE_PROTOCOL "org.apache.hadoop.hdfs.protocol.ClientProtocol"
#define DELEGATION_TOKEN_KIND "HDFS_DELEGATION_TOKEN"

using namespace google::protobuf;
using namespace hadoop::common;
using namespace hadoop::hdfs;

namespace hdfs {
namespace internal {

NamenodeImpl::NamenodeImpl(const char *host, const char *port,
        const std::string &tokenService, const SessionConfig &c,
        const RpcAuth &a) :
    auth(a), client(RpcClient::getClient()), conf(c), protocol(
        NAMENODE_VERSION, NAMENODE_PROTOCOL, DELEGATION_TOKEN_KIND),
        server(tokenService, host, port) {
}

NamenodeImpl::~NamenodeImpl() {
}

void NamenodeImpl::invoke(const RpcCall &call) {
    RpcChannel &channel = client.getChannel(auth, protocol, server, conf);

    try {
        channel.invoke(call);
    } catch (...) {
        channel.close(false);
        throw;
    }

    channel.close(false);
}

//Idempotent
void NamenodeImpl::getBlockLocations(const std::string &src, int64_t offset,
        int64_t length, LocatedBlocks &lbs) /* throw (AccessControlException,
         FileNotFoundException, UnresolvedLinkException, HdfsIOException) */ {
    try {
        GetBlockLocationsRequestProto request;
        GetBlockLocationsResponseProto response;
        request.set_length(length);
        request.set_offset(offset);
        request.set_src(src);
        invoke(RpcCall(true, "getBlockLocations", &request, &response));
        Convert(lbs, response.locations());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

void NamenodeImpl::create(const std::string &src, const Permission &masked,
                          const std::string &clientName, int flag, bool createParent,
                          short replication, int64_t blockSize)
    /* throw (AccessControlException,
         AlreadyBeingCreatedException, DSQuotaExceededException,
         FileAlreadyExistsException, FileNotFoundException,
         NSQuotaExceededException, ParentNotDirectoryException,
          UnresolvedLinkException, HdfsIOException) */{
    try {
        CreateRequestProto request;
        CreateResponseProto response;
        request.set_blocksize(blockSize);
        request.set_clientname(clientName);
        request.set_createflag(flag);
        request.set_createparent(createParent);
        request.set_replication(replication);
        request.set_src(src);
        Build(masked, request.mutable_masked());
        invoke(RpcCall(false, "create", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < AlreadyBeingCreatedException,
                  DSQuotaExceededException, FileAlreadyExistsException,
                  FileNotFoundException, NSQuotaExceededException,
                  ParentNotDirectoryException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

shared_ptr<LocatedBlock> NamenodeImpl::append(const std::string &src,
        const std::string &clientName)
/* throw (AlreadyBeingCreatedException, DSQuotaExceededException,
 FileNotFoundException,
 UnresolvedLinkException, HdfsIOException) */{
    try {
        AppendRequestProto request;
        AppendResponseProto response;
        request.set_clientname(clientName);
        request.set_src(src);
        invoke(RpcCall(false, "append", &request, &response));

        if (response.has_block()) {
            return Convert(response.block());
        } else {
            return shared_ptr<LocatedBlock>();
        }
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < AlreadyBeingCreatedException, AccessControlException,
                  DSQuotaExceededException, FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(
                      e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
bool NamenodeImpl::setReplication(const std::string &src, short replication)
/* throw (DSQuotaExceededException,
 FileNotFoundException,  UnresolvedLinkException,
 HdfsIOException) */{
    try {
        SetReplicationRequestProto request;
        SetReplicationResponseProto response;
        request.set_src(src.c_str());
        request.set_replication(static_cast<uint32>(replication));
        invoke(RpcCall(true, "setReplication", &request, &response));
        return response.result();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < DSQuotaExceededException,
                  FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
void NamenodeImpl::setPermission(const std::string &src,
                                 const Permission &permission) /* throw (AccessControlException,
         FileNotFoundException,
         UnresolvedLinkException, HdfsIOException) */{
    try {
        SetPermissionRequestProto request;
        SetPermissionResponseProto response;
        request.set_src(src);
        Build(permission, request.mutable_permission());
        invoke(RpcCall(true, "setPermission", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(
                      e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
void NamenodeImpl::setOwner(const std::string &src,
                            const std::string &username, const std::string &groupname)
/* throw (FileNotFoundException,
  UnresolvedLinkException, HdfsIOException) */{
    try {
        SetOwnerRequestProto request;
        SetOwnerResponseProto response;
        request.set_src(src);

        if (!username.empty()) {
            request.set_username(username);
        }

        if (!groupname.empty()) {
            request.set_groupname(groupname);
        }

        invoke(RpcCall(true, "setOwner", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(
                      e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

void NamenodeImpl::abandonBlock(const ExtendedBlock &b,
                                const std::string &src, const std::string &holder)
/* throw (FileNotFoundException,
 UnresolvedLinkException, HdfsIOException) */{
    try {
        AbandonBlockRequestProto request;
        AbandonBlockResponseProto response;
        request.set_holder(holder);
        request.set_src(src);
        Build(b, request.mutable_b());
        invoke(RpcCall(false, "abandonBlock", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

shared_ptr<LocatedBlock> NamenodeImpl::addBlock(const std::string &src,
        const std::string &clientName, const ExtendedBlock *previous,
        const std::vector<DatanodeInfo> &excludeNodes)
/* throw (FileNotFoundException,
 NotReplicatedYetException,
 UnresolvedLinkException, HdfsIOException) */{
    try {
        AddBlockRequestProto request;
        AddBlockResponseProto response;
        request.set_clientname(clientName);
        request.set_src(src);

        if (previous) {
            Build(*previous, request.mutable_previous());
        }

        if (excludeNodes.size()) {
            Build(excludeNodes, request.mutable_excludenodes());
        }

        invoke(RpcCall(true, "addBlock", &request, &response));
        return Convert(response.block());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  NotReplicatedYetException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
shared_ptr<LocatedBlock> NamenodeImpl::getAdditionalDatanode(
    const std::string &src, const ExtendedBlock &blk,
    const std::vector<DatanodeInfo> &existings,
    const std::vector<std::string> &storageIDs,
    const std::vector<DatanodeInfo> &excludes, int numAdditionalNodes,
    const std::string &clientName)
/* throw ( FileNotFoundException,
  UnresolvedLinkException, HdfsIOException) */{
    try {
        GetAdditionalDatanodeRequestProto request;
        GetAdditionalDatanodeResponseProto response;
        request.set_src(src);
        Build(existings, request.mutable_existings());
        Build(storageIDs, request.mutable_existingstorageuuids());
        Build(excludes, request.mutable_excludes());
        Build(blk, request.mutable_blk());
        request.set_clientname(clientName);
        request.set_numadditionalnodes(numAdditionalNodes);
        invoke(RpcCall(true, "getAdditionalDatanode", &request, &response));
        return Convert(response.block());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper <
        FileNotFoundException,
        NotReplicatedYetException,
        UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

bool NamenodeImpl::complete(const std::string &src,
                            const std::string &clientName, const ExtendedBlock *last)
/* throw (FileNotFoundException,
  UnresolvedLinkException, HdfsIOException) */{
    try {
        CompleteRequestProto request;
        CompleteResponseProto response;
        request.set_clientname(clientName);
        request.set_src(src);

        if (last) {
            Build(*last, request.mutable_last());
        }

        invoke(RpcCall(false, "complete", &request, &response));
        return response.result();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(
                      e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
/*void NamenodeImpl::reportBadBlocks(const std::vector<LocatedBlock> &blocks)
 throw (HdfsIOException) {
    try {
        ReportBadBlocksRequestProto request;
        ReportBadBlocksResponseProto response;
        Build(blocks, request.mutable_blocks());
        invoke(RpcCall(true, "reportBadBlocks", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

bool NamenodeImpl::rename(const std::string &src, const std::string &dst)
      /* throw (UnresolvedLinkException, HdfsIOException) */{
    try {
        RenameRequestProto request;
        RenameResponseProto response;
        request.set_src(src);
        request.set_dst(dst);
        invoke(RpcCall(false, "rename", &request, &response));
        return response.result();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<UnresolvedLinkException, HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

/*void NamenodeImpl::concat(const std::string &trg,
          const std::vector<std::string> &srcs)
      throw (UnresolvedLinkException, HdfsIOException) {
    try {
        ConcatRequestProto request;
        ConcatResponseProto response;
        request.set_trg(trg);
        Build(srcs, request.mutable_srcs());
        invoke(RpcCall(false, "concat", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<UnresolvedLinkException, HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

bool NamenodeImpl::deleteFile(const std::string &src, bool recursive)
      /* throw (FileNotFoundException, UnresolvedLinkException,
       * HdfsIOException) */ {
    try {
        DeleteRequestProto request;
        DeleteResponseProto response;
        request.set_src(src);
        request.set_recursive(recursive);
        invoke(RpcCall(false, "delete", &request, &response));
        return response.result();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(
                      e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
bool NamenodeImpl::mkdirs(const std::string &src, const Permission &masked,
                          bool createParent) /* throw (AccessControlException,
         FileAlreadyExistsException, FileNotFoundException,
         NSQuotaExceededException, ParentNotDirectoryException,
          UnresolvedLinkException, HdfsIOException) */{
    try {
        MkdirsRequestProto request;
        MkdirsResponseProto response;
        request.set_src(src);
        request.set_createparent(createParent);
        Build(masked, request.mutable_masked());
        invoke(RpcCall(true, "mkdirs", &request, &response));
        return response.result();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileAlreadyExistsException,
                  FileNotFoundException, NSQuotaExceededException,
                  ParentNotDirectoryException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
bool NamenodeImpl::getListing(const std::string &src,
          const std::string &startAfter, bool needLocation,
          std::vector<FileStatus> &dl) /* throw (AccessControlException,
         FileNotFoundException, UnresolvedLinkException, HdfsIOException) */ {
    try {
        GetListingRequestProto request;
        GetListingResponseProto response;
        request.set_src(src);
        request.set_startafter(startAfter);
        request.set_needlocation(needLocation);
        invoke(RpcCall(true, "getListing", &request, &response));

        if (response.has_dirlist()) {
            const DirectoryListingProto &lists = response.dirlist();
            Convert(dl, lists);
            return lists.remainingentries() > 0;
        }

        THROW(FileNotFoundException, "%s not found.", src.c_str());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
void NamenodeImpl::renewLease(const std::string &clientName)
          /* throw (HdfsIOException) */{
    try {
        RenewLeaseRequestProto request;
        RenewLeaseResponseProto response;
        request.set_clientname(clientName);
        invoke(RpcCall(true, "renewLease", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
bool NamenodeImpl::recoverLease(const std::string &src,
           const std::string &clientName) /* throw (HdfsIOException) */ {
    try {
        RecoverLeaseRequestProto request;
        RecoverLeaseResponseProto response;
        request.set_src(src);
        request.set_clientname(clientName);
        invoke(RpcCall(true, "recoverLease", &request, &response));
        return response.result();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }

    return false;
}

//Idempotent
std::vector<int64_t> NamenodeImpl::getFsStats() {
      /* throw (HdfsIOException) */
    try {
        GetFsStatusRequestProto request;
        GetFsStatsResponseProto response;
        invoke(RpcCall(true, "getFsStats", &request, &response));
        std::vector<int64_t> retval;
        retval.push_back(response.capacity());
        retval.push_back(response.used());
        retval.push_back(response.remaining());
        retval.push_back(response.under_replicated());
        retval.push_back(response.corrupt_blocks());
        retval.push_back(response.missing_blocks());
        return retval;
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(
                      e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }

    return std::vector<int64_t>();
}

/*void NamenodeImpl::metaSave(const std::string &filename)
 throw (HdfsIOException) {
    try {
        MetaSaveRequestProto request;
        MetaSaveResponseProto response;
        request.set_filename(filename);
        invoke(RpcCall(true, "metaSave", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

//Idempotent
FileStatus NamenodeImpl::getFileInfo(const std::string &src)
/* throw (FileNotFoundException,
 UnresolvedLinkException, HdfsIOException) */{
    FileStatus retval;

    try {
        GetFileInfoRequestProto request;
        GetFileInfoResponseProto response;
        request.set_src(src);
        invoke(RpcCall(true, "getFileInfo", &request, &response));

        if (response.has_fs()) {
            Convert(retval, response.fs());
            assert(src.find_last_of('/') != src.npos);
            const char *path = src.c_str() + src.find_last_of('/') + 1;
            path = src == "/" ? "/" : path;
            retval.setPath(path);
            return retval;
        }

        THROW(FileNotFoundException, "Path %s does not exist.", src.c_str());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
/*FileStatus NamenodeImpl::getFileLinkInfo(const std::string &src)
 throw (UnresolvedLinkException, HdfsIOException) {
    FileStatus fileStatus;

    try {
        GetFileLinkInfoRequestProto request;
        GetFileLinkInfoResponseProto response;
        request.set_src(src);
        invoke(RpcCall(true, "getFileLinkInfo", &request, &response));

        if (response.has_fs()) {
            Convert(fileStatus, response.fs());
            return fileStatus;
        }

        THROW(FileNotFoundException, "Path %s does not exist.", src.c_str());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < UnresolvedLinkException,
                  HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

//Idempotent
/*ContentSummary NamenodeImpl::getContentSummary(const std::string &path)
 throw (FileNotFoundException,
 UnresolvedLinkException, HdfsIOException) {
    ContentSummary contentSummary;

    try {
        GetContentSummaryRequestProto request;
        GetContentSummaryResponseProto response;
        request.set_path(path);
        invoke(RpcCall(true, "getContentSummary", &request, &response));

        if (response.has_summary()) {
            Convert(contentSummary, response.summary());
            return contentSummary;
        }

        THROW(FileNotFoundException, "Path %s does not exist.", path.c_str());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

//Idempotent
/*void NamenodeImpl::setQuota(const std::string &path, int64_t namespaceQuota,
                            int64_t diskspaceQuota)  throw (AccessControlException,
         FileNotFoundException, UnresolvedLinkException, HdfsIOException) {
    try {
        SetQuotaRequestProto request;
        SetQuotaResponseProto response;
        request.set_path(path);
        request.set_namespacequota(namespaceQuota);
        request.set_diskspacequota(diskspaceQuota);
        invoke(RpcCall(true, "diskspaceQuota", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

//Idempotent
void NamenodeImpl::fsync(const std::string &src, const std::string &client)
/* throw (FileNotFoundException,
 UnresolvedLinkException, HdfsIOException) */{
    try {
        FsyncRequestProto request;
        FsyncResponseProto response;
        request.set_client(client);
        request.set_src(src);
        invoke(RpcCall(true, "fsync", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

//Idempotent
void NamenodeImpl::setTimes(const std::string &src, int64_t mtime,
                            int64_t atime) /* throw (FileNotFoundException,
         UnresolvedLinkException, HdfsIOException) */{
    try {
        SetTimesRequestProto request;
        SetTimesResponseProto response;
        request.set_src(src);
        request.set_mtime(mtime);
        request.set_atime(atime);
        invoke(RpcCall(true, "setTimes", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper < FileNotFoundException,
                  UnresolvedLinkException, HdfsIOException > unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

/*void NamenodeImpl::createSymlink(const std::string &target,
              const std::string &link, const Permission &dirPerm,
              bool createParent)  throw (AccessControlException,
         FileAlreadyExistsException, FileNotFoundException,
         ParentNotDirectoryException,
         UnresolvedLinkException, HdfsIOException) {
    try {
        CreateSymlinkRequestProto request;
        CreateSymlinkResponseProto response;
        request.set_target(target);
        request.set_link(link);
        request.set_createparent(createParent);
        Build(dirPerm, request.mutable_dirperm());
        invoke(RpcCall(true, "createSymlink", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<FileNotFoundException, HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

//Idempotent
/*std::string NamenodeImpl::getLinkTarget(const std::string &path)
 throw (FileNotFoundException, HdfsIOException) {
    try {
        GetLinkTargetRequestProto request;
        GetLinkTargetResponseProto response;
        request.set_path(path);
        invoke(RpcCall(true, "getLinkTarget", &request, &response));
        return response.targetpath();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<FileNotFoundException, HdfsIOException> unwrapper(
            e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}*/

//Idempotent
shared_ptr<LocatedBlock> NamenodeImpl::updateBlockForPipeline(
        const ExtendedBlock &block, const std::string &clientName)
/* throw (HdfsIOException) */{
    try {
        UpdateBlockForPipelineRequestProto request;
        UpdateBlockForPipelineResponseProto response;
        request.set_clientname(clientName);
        Build(block, request.mutable_block());
        invoke(RpcCall(true, "updateBlockForPipeline", &request, &response));
        return Convert(response.block());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

void NamenodeImpl::updatePipeline(const std::string &clientName,
          const ExtendedBlock &oldBlock, const ExtendedBlock &newBlock,
          const std::vector<DatanodeInfo> &newNodes,
          const std::vector<std::string> &storageIDs) {
    /* throw (HdfsIOException) */
    try {
        UpdatePipelineRequestProto request;
        UpdatePipelineResponseProto response;
        request.set_clientname(clientName);
        Build(oldBlock, request.mutable_oldblock());
        Build(newBlock, request.mutable_newblock());
        Build(newNodes, request.mutable_newnodes());
        Build(storageIDs, request.mutable_storageids());
        invoke(RpcCall(false, "updatePipeline", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

Token NamenodeImpl::getDelegationToken(const std::string &renewer) {
    try {
        GetDelegationTokenRequestProto request;
        GetDelegationTokenResponseProto response;
        request.set_renewer(renewer);
        invoke(RpcCall(true, "getDelegationToken", &request, &response));
        return Convert(response.token());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

int64_t NamenodeImpl::renewDelegationToken(const Token &token) {
    try {
        RenewDelegationTokenRequestProto request;
        RenewDelegationTokenResponseProto response;
        Build(token, request.mutable_token());
        invoke(RpcCall(true, "renewDelegationToken", &request, &response));
        return response.newexpirytime();
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsInvalidBlockToken, HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

void NamenodeImpl::cancelDelegationToken(const Token &token) {
    try {
        CancelDelegationTokenRequestProto request;
        CancelDelegationTokenResponseProto response;
        Build(token, request.mutable_token());
        invoke(RpcCall(true, "cancelDelegationToken", &request, &response));
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<HdfsInvalidBlockToken, HdfsIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

}
}
