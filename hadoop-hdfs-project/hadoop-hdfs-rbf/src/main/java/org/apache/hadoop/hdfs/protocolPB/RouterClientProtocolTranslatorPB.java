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

package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsPartialListing;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AllowSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CheckAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBatchedListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEnclosingRootRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLocatedFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetQuotaUsageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSlowDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.HAServiceStateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListOpenFilesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.OpenFilesBatchResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2RequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.TruncateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UnsetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SatisfyStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.EncryptionZoneProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListReencryptionStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ZoneReencryptionStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ReencryptEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.AddErasureCodingPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.RemoveErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.EnableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.DisableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.SetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.UnsetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.CodecProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BatchedDirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetECTopologyResultForPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrRequestProto;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.util.Lists;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpcClient;

/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to the
 * new PB types.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RouterClientProtocolTranslatorPB extends ClientNamenodeProtocolTranslatorPB {
  private final ClientNamenodeProtocolPB rpcProxy;

  public RouterClientProtocolTranslatorPB(ClientNamenodeProtocolPB proxy) {
    super(proxy);
    rpcProxy = proxy;
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getBlockLocations(src, offset, length);
    }
    GetBlockLocationsRequestProto req = GetBlockLocationsRequestProto
        .newBuilder()
        .setSrc(src)
        .setOffset(offset)
        .setLength(length)
        .build();

    return asyncIpcClient(() -> rpcProxy.getBlockLocations(null, req),
        res -> res.hasLocations() ? PBHelperClient.convert(res.getLocations()) : null,
        LocatedBlocks.class);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getServerDefaults();
    }
    GetServerDefaultsRequestProto req = VOID_GET_SERVER_DEFAULT_REQUEST;

    return asyncIpcClient(() -> rpcProxy.getServerDefaults(null, req),
        res -> PBHelperClient.convert(res.getServerDefaults()),
        FsServerDefaults.class);
  }

  @Override
  public HdfsFileStatus create(
      String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
      String storagePolicy) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.create(
          src, masked, clientName, flag, createParent, replication,
          blockSize, supportedVersions, ecPolicyName, storagePolicy);
    }

    CreateRequestProto.Builder builder = CreateRequestProto.newBuilder()
        .setSrc(src)
        .setMasked(PBHelperClient.convert(masked))
        .setClientName(clientName)
        .setCreateFlag(PBHelperClient.convertCreateFlag(flag))
        .setCreateParent(createParent)
        .setReplication(replication)
        .setBlockSize(blockSize);
    if (ecPolicyName != null) {
      builder.setEcPolicyName(ecPolicyName);
    }
    if (storagePolicy != null) {
      builder.setStoragePolicy(storagePolicy);
    }
    FsPermission unmasked = masked.getUnmasked();
    if (unmasked != null) {
      builder.setUnmasked(PBHelperClient.convert(unmasked));
    }
    builder.addAllCryptoProtocolVersion(
        PBHelperClient.convert(supportedVersions));
    CreateRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.create(null, req),
        res -> res.hasFs() ? PBHelperClient.convert(res.getFs()) : null,
        HdfsFileStatus.class);
  }

  @Override
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.truncate(src, newLength, clientName);
    }

    TruncateRequestProto req = TruncateRequestProto.newBuilder()
        .setSrc(src)
        .setNewLength(newLength)
        .setClientName(clientName)
        .build();

    return asyncIpcClient(() -> rpcProxy.truncate(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public LastBlockWithStatus append(String src, String clientName,
                                    EnumSetWritable<CreateFlag> flag) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.append(src, clientName, flag);
    }

    AppendRequestProto req = AppendRequestProto.newBuilder().setSrc(src)
        .setClientName(clientName).setFlag(
            PBHelperClient.convertCreateFlag(flag))
        .build();

    return asyncIpcClient(() -> rpcProxy.append(null, req),
        res -> {
          LocatedBlock lastBlock = res.hasBlock() ? PBHelperClient
              .convertLocatedBlockProto(res.getBlock()) : null;
          HdfsFileStatus stat = (res.hasStat()) ?
              PBHelperClient.convert(res.getStat()) : null;
          return new LastBlockWithStatus(lastBlock, stat);
        }, LastBlockWithStatus.class);
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.setReplication(src, replication);
    }
    SetReplicationRequestProto req = SetReplicationRequestProto.newBuilder()
        .setSrc(src)
        .setReplication(replication)
        .build();

    return asyncIpcClient(() -> rpcProxy.setReplication(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setPermission(src, permission);
      return;
    }
    SetPermissionRequestProto req = SetPermissionRequestProto.newBuilder()
        .setSrc(src)
        .setPermission(PBHelperClient.convert(permission))
        .build();

    asyncIpcClient(() -> rpcProxy.setPermission(null, req),
        res -> null, null);
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setOwner(src, username, groupname);
      return;
    }
    SetOwnerRequestProto.Builder req = SetOwnerRequestProto.newBuilder()
        .setSrc(src);
    if (username != null) {
      req.setUsername(username);
    }
    if (groupname != null) {
      req.setGroupname(groupname);
    }

    asyncIpcClient(() -> rpcProxy.setOwner(null, req.build()),
        res -> null, null);
  }

  @Override
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
                           String holder) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.abandonBlock(b, fileId, src, holder);
      return;
    }
    AbandonBlockRequestProto req = AbandonBlockRequestProto.newBuilder()
        .setB(PBHelperClient.convert(b)).setSrc(src).setHolder(holder)
        .setFileId(fileId).build();
    asyncIpcClient(() -> rpcProxy.abandonBlock(null, req),
        res -> null, null);
  }

  @Override
  public LocatedBlock addBlock(
      String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
      String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.addBlock(src, clientName, previous, excludeNodes,
          fileId, favoredNodes, addBlockFlags);
    }
    AddBlockRequestProto.Builder req = AddBlockRequestProto.newBuilder()
        .setSrc(src).setClientName(clientName).setFileId(fileId);
    if (previous != null) {
      req.setPrevious(PBHelperClient.convert(previous));
    }
    if (excludeNodes != null) {
      req.addAllExcludeNodes(PBHelperClient.convert(excludeNodes));
    }
    if (favoredNodes != null) {
      req.addAllFavoredNodes(Arrays.asList(favoredNodes));
    }
    if (addBlockFlags != null) {
      req.addAllFlags(PBHelperClient.convertAddBlockFlags(
          addBlockFlags));
    }

    return asyncIpcClient(() -> rpcProxy.addBlock(null, req.build()),
        res -> PBHelperClient.convertLocatedBlockProto(res.getBlock()),
        LocatedBlock.class);
  }

  @Override
  public LocatedBlock getAdditionalDatanode(
      String src, long fileId,
      ExtendedBlock blk, DatanodeInfo[] existings, String[] existingStorageIDs,
      DatanodeInfo[] excludes, int numAdditionalNodes, String clientName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getAdditionalDatanode(src, fileId, blk, existings,
          existingStorageIDs, excludes, numAdditionalNodes, clientName);
    }
    GetAdditionalDatanodeRequestProto req = GetAdditionalDatanodeRequestProto
        .newBuilder()
        .setSrc(src)
        .setFileId(fileId)
        .setBlk(PBHelperClient.convert(blk))
        .addAllExistings(PBHelperClient.convert(existings))
        .addAllExistingStorageUuids(Arrays.asList(existingStorageIDs))
        .addAllExcludes(PBHelperClient.convert(excludes))
        .setNumAdditionalNodes(numAdditionalNodes)
        .setClientName(clientName)
        .build();

    return asyncIpcClient(() -> rpcProxy.getAdditionalDatanode(null, req),
        res -> PBHelperClient.convertLocatedBlockProto(res.getBlock()),
        LocatedBlock.class);
  }

  @Override
  public boolean complete(String src, String clientName,
                          ExtendedBlock last, long fileId) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.complete(src, clientName, last, fileId);
    }
    CompleteRequestProto.Builder req = CompleteRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName)
        .setFileId(fileId);
    if (last != null) {
      req.setLast(PBHelperClient.convert(last));
    }

    return asyncIpcClient(() -> rpcProxy.complete(null, req.build()),
        res -> res.getResult(),
        Boolean.class);
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.reportBadBlocks(blocks);
      return;
    }
    ReportBadBlocksRequestProto req = ReportBadBlocksRequestProto.newBuilder()
        .addAllBlocks(Arrays.asList(
            PBHelperClient.convertLocatedBlocks(blocks)))
        .build();

    asyncIpcClient(() -> rpcProxy.reportBadBlocks(null, req),
        res -> null, Void.class);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.rename(src, dst);
    }
    RenameRequestProto req = RenameRequestProto.newBuilder()
        .setSrc(src)
        .setDst(dst).build();

    return asyncIpcClient(() -> rpcProxy.rename(null, req),
        res -> res.getResult(),
        Boolean.class);
  }


  @Override
  public void rename2(String src, String dst, Rename... options)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.rename2(src, dst, options);
      return;
    }
    boolean overwrite = false;
    boolean toTrash = false;
    if (options != null) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
        if (option == Rename.TO_TRASH) {
          toTrash = true;
        }
      }
    }
    Rename2RequestProto req = Rename2RequestProto.newBuilder().
        setSrc(src).
        setDst(dst).
        setOverwriteDest(overwrite).
        setMoveToTrash(toTrash).
        build();

    asyncIpcClient(() -> rpcProxy.rename2(null, req),
        res -> null, Void.class);
  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.concat(trg, srcs);
      return;
    }
    ConcatRequestProto req = ConcatRequestProto.newBuilder().
        setTrg(trg).
        addAllSrcs(Arrays.asList(srcs)).build();

    asyncIpcClient(() -> rpcProxy.concat(null, req),
        res -> null, Void.class);
  }


  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.delete(src, recursive);
    }
    DeleteRequestProto req = DeleteRequestProto.newBuilder().setSrc(src)
        .setRecursive(recursive).build();

    return asyncIpcClient(() -> rpcProxy.delete(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.mkdirs(src, masked, createParent);
    }
    MkdirsRequestProto.Builder builder = MkdirsRequestProto.newBuilder()
        .setSrc(src)
        .setMasked(PBHelperClient.convert(masked))
        .setCreateParent(createParent);
    FsPermission unmasked = masked.getUnmasked();
    if (unmasked != null) {
      builder.setUnmasked(PBHelperClient.convert(unmasked));
    }
    MkdirsRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.mkdirs(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public DirectoryListing getListing(
      String src, byte[] startAfter, boolean needLocation) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getListing(src, startAfter, needLocation);
    }
    GetListingRequestProto req = GetListingRequestProto.newBuilder()
        .setSrc(src)
        .setStartAfter(ByteString.copyFrom(startAfter))
        .setNeedLocation(needLocation).build();

    return asyncIpcClient(() -> rpcProxy.getListing(null, req),
        res -> {
          if (res.hasDirList()) {
            return PBHelperClient.convert(res.getDirList());
          }
          return null;
        }, DirectoryListing.class);
  }

  @Override
  public BatchedDirectoryListing getBatchedListing(
      String[] srcs, byte[] startAfter, boolean needLocation)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getBatchedListing(srcs, startAfter, needLocation);
    }
    GetBatchedListingRequestProto req = GetBatchedListingRequestProto
        .newBuilder()
        .addAllPaths(Arrays.asList(srcs))
        .setStartAfter(ByteString.copyFrom(startAfter))
        .setNeedLocation(needLocation).build();

    return asyncIpcClient(() -> rpcProxy.getBatchedListing(null, req),
        res -> {
          if (res.getListingsCount() > 0) {
            HdfsPartialListing[] listingArray =
                new HdfsPartialListing[res.getListingsCount()];
            int listingIdx = 0;
            for (BatchedDirectoryListingProto proto : res.getListingsList()) {
              HdfsPartialListing listing;
              if (proto.hasException()) {
                HdfsProtos.RemoteExceptionProto reProto = proto.getException();
                RemoteException ex = new RemoteException(
                    reProto.getClassName(), reProto.getMessage());
                listing = new HdfsPartialListing(proto.getParentIdx(), ex);
              } else {
                List<HdfsFileStatus> statuses =
                    PBHelperClient.convertHdfsFileStatus(
                        proto.getPartialListingList());
                listing = new HdfsPartialListing(proto.getParentIdx(), statuses);
              }
              listingArray[listingIdx++] = listing;
            }
            BatchedDirectoryListing batchedListing =
                new BatchedDirectoryListing(listingArray, res.getHasMore(),
                    res.getStartAfter().toByteArray());
            return batchedListing;
          }
          return null;
        }, BatchedDirectoryListing.class);
  }


  @Override
  public void renewLease(String clientName, List<String> namespaces)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.renewLease(clientName, namespaces);
      return;
    }
    RenewLeaseRequestProto.Builder builder = RenewLeaseRequestProto
        .newBuilder().setClientName(clientName);
    if (namespaces != null && !namespaces.isEmpty()) {
      builder.addAllNamespaces(namespaces);
    }

    asyncIpcClient(() -> rpcProxy.renewLease(null, builder.build()),
        res -> null, Void.class);
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.recoverLease(src, clientName);
    }
    RecoverLeaseRequestProto req = RecoverLeaseRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName).build();

    return asyncIpcClient(() -> rpcProxy.recoverLease(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public long[] getStats() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getStats();
    }

    return asyncIpcClient(() -> rpcProxy.getFsStats(null, VOID_GET_FSSTATUS_REQUEST),
        res -> PBHelperClient.convert(res), long[].class);
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getReplicatedBlockStats();
    }

    return asyncIpcClient(() -> rpcProxy.getFsReplicatedBlockStats(null,
        VOID_GET_FS_REPLICATED_BLOCK_STATS_REQUEST),
        res -> PBHelperClient.convert(res), ReplicatedBlockStats.class);
  }

  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getECBlockGroupStats();
    }

    return asyncIpcClient(() -> rpcProxy.getFsECBlockGroupStats(null,
        VOID_GET_FS_ECBLOCKGROUP_STATS_REQUEST),
        res -> PBHelperClient.convert(res), ECBlockGroupStats.class);
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getDatanodeReport(type);
    }
    GetDatanodeReportRequestProto req = GetDatanodeReportRequestProto
        .newBuilder()
        .setType(PBHelperClient.convert(type)).build();

    return asyncIpcClient(() -> rpcProxy.getDatanodeReport(null, req),
        res -> PBHelperClient.convert(res.getDiList()), DatanodeInfo[].class);
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getDatanodeStorageReport(type);
    }
    final GetDatanodeStorageReportRequestProto req
        = GetDatanodeStorageReportRequestProto.newBuilder()
        .setType(PBHelperClient.convert(type)).build();

    return asyncIpcClient(() -> rpcProxy.getDatanodeStorageReport(null, req),
        res -> PBHelperClient.convertDatanodeStorageReports(
            res.getDatanodeStorageReportsList()), DatanodeStorageReport[].class);
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getPreferredBlockSize(filename);
    }
    GetPreferredBlockSizeRequestProto req = GetPreferredBlockSizeRequestProto
        .newBuilder()
        .setFilename(filename)
        .build();

    return asyncIpcClient(() -> rpcProxy.getPreferredBlockSize(null, req),
        res -> res.getBsize(), Long.class);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.setSafeMode(action, isChecked);
    }
    SetSafeModeRequestProto req = SetSafeModeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action))
        .setChecked(isChecked).build();

    return asyncIpcClient(() -> rpcProxy.setSafeMode(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.saveNamespace(timeWindow, txGap);
    }
    SaveNamespaceRequestProto req = SaveNamespaceRequestProto.newBuilder()
        .setTimeWindow(timeWindow).setTxGap(txGap).build();

    return asyncIpcClient(() -> rpcProxy.saveNamespace(null, req),
        res -> res.getSaved(), Boolean.class);
  }

  @Override
  public long rollEdits() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.rollEdits();
    }
    return asyncIpcClient(() -> rpcProxy.rollEdits(null, VOID_ROLLEDITS_REQUEST),
        res -> res.getNewSegmentTxId(), Long.class);
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException{
    if (!Client.isAsynchronousMode()) {
      return super.restoreFailedStorage(arg);
    }
    RestoreFailedStorageRequestProto req = RestoreFailedStorageRequestProto
        .newBuilder()
        .setArg(arg).build();

    return asyncIpcClient(() -> rpcProxy.restoreFailedStorage(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public void refreshNodes() throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.refreshNodes();
      return;
    }
    asyncIpcClient(() -> rpcProxy.refreshNodes(null, VOID_REFRESH_NODES_REQUEST),
        res -> null, Void.class);
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.finalizeUpgrade();
      return;
    }
    asyncIpcClient(() -> rpcProxy.finalizeUpgrade(null, VOID_FINALIZE_UPGRADE_REQUEST),
        res -> null, Void.class);
  }

  @Override
  public boolean upgradeStatus() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.upgradeStatus();
    }
    return asyncIpcClient(() -> rpcProxy.upgradeStatus(null, VOID_UPGRADE_STATUS_REQUEST),
        res -> res.getUpgradeFinalized(), Boolean.class);
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.rollingUpgrade(action);
    }
    final RollingUpgradeRequestProto r = RollingUpgradeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action)).build();

    return asyncIpcClient(() -> rpcProxy.rollingUpgrade(null, r),
        res -> PBHelperClient.convert(res.getRollingUpgradeInfo()),
        RollingUpgradeInfo.class);
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.listCorruptFileBlocks(path, cookie);
    }
    ListCorruptFileBlocksRequestProto.Builder req =
        ListCorruptFileBlocksRequestProto.newBuilder().setPath(path);
    if (cookie != null) {
      req.setCookie(cookie);
    }

    return asyncIpcClient(() -> rpcProxy.listCorruptFileBlocks(null, req.build()),
        res ->PBHelperClient.convert(res.getCorrupt()), CorruptFileBlocks.class);
  }

  @Override
  public void metaSave(String filename) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.metaSave(filename);
      return;
    }
    MetaSaveRequestProto req = MetaSaveRequestProto.newBuilder()
        .setFilename(filename).build();

    asyncIpcClient(() -> rpcProxy.metaSave(null, req),
        res -> null, Void.class);
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getFileInfo(src);
    }
    GetFileInfoRequestProto req = GetFileInfoRequestProto.newBuilder()
        .setSrc(src)
        .build();

    return asyncIpcClient(() -> rpcProxy.getFileInfo(null, req),
        res -> res.hasFs() ? PBHelperClient.convert(res.getFs()) : null,
        HdfsFileStatus.class);
  }

  @Override
  public HdfsLocatedFileStatus getLocatedFileInfo(
      String src, boolean needBlockToken) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getLocatedFileInfo(src, needBlockToken);
    }
    GetLocatedFileInfoRequestProto req =
        GetLocatedFileInfoRequestProto.newBuilder()
            .setSrc(src)
            .setNeedBlockToken(needBlockToken)
            .build();

    return asyncIpcClient(() -> rpcProxy.getLocatedFileInfo(null, req),
        res -> (HdfsLocatedFileStatus) (res.hasFs() ? PBHelperClient.convert(res.getFs()) : null),
        HdfsLocatedFileStatus.class);
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getFileLinkInfo(src);
    }
    GetFileLinkInfoRequestProto req = GetFileLinkInfoRequestProto.newBuilder()
        .setSrc(src).build();

    return asyncIpcClient(() -> rpcProxy.getFileLinkInfo(null, req),
        res -> res.hasFs() ? PBHelperClient.convert(res.getFs()) : null,
        HdfsFileStatus.class);
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getContentSummary(path);
    }
    GetContentSummaryRequestProto req = GetContentSummaryRequestProto
        .newBuilder()
        .setPath(path)
        .build();

    return asyncIpcClient(() -> rpcProxy.getContentSummary(null, req),
        res -> PBHelperClient.convert(res.getSummary()), ContentSummary.class);
  }

  @Override
  public void setQuota(
      String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setQuota(path, namespaceQuota, storagespaceQuota, type);
      return;
    }
    final SetQuotaRequestProto.Builder builder
        = SetQuotaRequestProto.newBuilder()
        .setPath(path)
        .setNamespaceQuota(namespaceQuota)
        .setStoragespaceQuota(storagespaceQuota);
    if (type != null) {
      builder.setStorageType(PBHelperClient.convertStorageType(type));
    }
    final SetQuotaRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.setQuota(null, req),
        res -> null, Void.class);
  }

  @Override
  public void fsync(
      String src, long fileId, String client, long lastBlockLength) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.fsync(src, fileId, client, lastBlockLength);
      return;
    }
    FsyncRequestProto req = FsyncRequestProto.newBuilder().setSrc(src)
        .setClient(client).setLastBlockLength(lastBlockLength)
        .setFileId(fileId).build();

    asyncIpcClient(() -> rpcProxy.fsync(null, req),
        res -> null, Void.class);
  }

  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setTimes(src, mtime, atime);
      return;
    }
    SetTimesRequestProto req = SetTimesRequestProto.newBuilder()
        .setSrc(src)
        .setMtime(mtime)
        .setAtime(atime)
        .build();

    asyncIpcClient(() -> rpcProxy.setTimes(null, req),
        res -> null, Void.class);
  }

  @Override
  public void createSymlink(
      String target, String link, FsPermission dirPerm,
      boolean createParent) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.createSymlink(target, link, dirPerm, createParent);
      return;
    }
    CreateSymlinkRequestProto req = CreateSymlinkRequestProto.newBuilder()
        .setTarget(target)
        .setLink(link)
        .setDirPerm(PBHelperClient.convert(dirPerm))
        .setCreateParent(createParent)
        .build();

    asyncIpcClient(() -> rpcProxy.createSymlink(null, req),
        res -> null, Void.class);
  }

  @Override
  public String getLinkTarget(String path) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getLinkTarget(path);
    }
    GetLinkTargetRequestProto req = GetLinkTargetRequestProto.newBuilder()
        .setPath(path).build();

    return asyncIpcClient(() -> rpcProxy.getLinkTarget(null, req),
        res -> res.hasTargetPath() ? res.getTargetPath() : null,
        String.class);
  }

  @Override
  public LocatedBlock updateBlockForPipeline(
      ExtendedBlock block, String clientName) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.updateBlockForPipeline(block, clientName);
    }
    UpdateBlockForPipelineRequestProto req = UpdateBlockForPipelineRequestProto
        .newBuilder()
        .setBlock(PBHelperClient.convert(block))
        .setClientName(clientName)
        .build();

    return asyncIpcClient(() -> rpcProxy.updateBlockForPipeline(null, req),
        res -> PBHelperClient.convertLocatedBlockProto(res.getBlock()),
        LocatedBlock.class);
  }

  @Override
  public void updatePipeline(
      String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock,
      DatanodeID[] newNodes, String[] storageIDs) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.updatePipeline(clientName, oldBlock, newBlock, newNodes, storageIDs);
      return;
    }
    UpdatePipelineRequestProto req = UpdatePipelineRequestProto.newBuilder()
        .setClientName(clientName)
        .setOldBlock(PBHelperClient.convert(oldBlock))
        .setNewBlock(PBHelperClient.convert(newBlock))
        .addAllNewNodes(Arrays.asList(PBHelperClient.convert(newNodes)))
        .addAllStorageIDs(storageIDs == null ? null : Arrays.asList(storageIDs))
        .build();

    asyncIpcClient(() -> rpcProxy.updatePipeline(null, req),
        res -> null, Void.class);
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(
      Text renewer) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getDelegationToken(renewer);
    }
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer == null ? "" : renewer.toString())
        .build();

    return asyncIpcClient(() -> rpcProxy.getDelegationToken(null, req),
        res -> res.hasToken() ?
            PBHelperClient.convertDelegationToken(res.getToken()) : null, Token.class);
  }

  @Override
  public long renewDelegationToken(
      Token<DelegationTokenIdentifier> token) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.renewDelegationToken(token);
    }
    RenewDelegationTokenRequestProto req =
        RenewDelegationTokenRequestProto.newBuilder().
            setToken(PBHelperClient.convert(token)).
            build();

    return asyncIpcClient(() -> rpcProxy.renewDelegationToken(null, req),
        res -> res.getNewExpiryTime(), Long.class);
  }

  @Override
  public void cancelDelegationToken(
      Token<DelegationTokenIdentifier> token) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.cancelDelegationToken(token);
      return;
    }
    CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto
        .newBuilder()
        .setToken(PBHelperClient.convert(token))
        .build();

    asyncIpcClient(() -> rpcProxy.cancelDelegationToken(null, req),
        res -> null, Void.class);
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setBalancerBandwidth(bandwidth);
      return;
    }
    SetBalancerBandwidthRequestProto req =
        SetBalancerBandwidthRequestProto.newBuilder()
            .setBandwidth(bandwidth)
            .build();

    asyncIpcClient(() -> rpcProxy.setBalancerBandwidth(null, req),
        res -> null, Void.class);
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getDataEncryptionKey();
    }
    return asyncIpcClient(() -> rpcProxy.getDataEncryptionKey(null,
        VOID_GET_DATA_ENCRYPTIONKEY_REQUEST),
        res -> res.hasDataEncryptionKey() ?
            PBHelperClient.convert(res.getDataEncryptionKey()) : null,
        DataEncryptionKey.class);
  }


  @Override
  public boolean isFileClosed(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.isFileClosed(src);
    }
    IsFileClosedRequestProto req = IsFileClosedRequestProto.newBuilder()
        .setSrc(src).build();

    return asyncIpcClient(() -> rpcProxy.isFileClosed(null, req),
        res -> res.getResult(), Boolean.class);
  }

  @Override
  public String createSnapshot(
      String snapshotRoot, String snapshotName) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.createSnapshot(snapshotRoot, snapshotName);
    }
    final CreateSnapshotRequestProto.Builder builder
        = CreateSnapshotRequestProto.newBuilder().setSnapshotRoot(snapshotRoot);
    if (snapshotName != null) {
      builder.setSnapshotName(snapshotName);
    }
    final CreateSnapshotRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.createSnapshot(null, req),
        res -> res.getSnapshotPath(), String.class);
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.deleteSnapshot(snapshotRoot, snapshotName);
      return;
    }
    DeleteSnapshotRequestProto req = DeleteSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotName(snapshotName).build();

    asyncIpcClient(() -> rpcProxy.deleteSnapshot(null, req),
        res -> null, Void.class);
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.allowSnapshot(snapshotRoot);
      return;
    }
    AllowSnapshotRequestProto req = AllowSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).build();

    asyncIpcClient(() -> rpcProxy.allowSnapshot(null, req),
        res -> null, Void.class);
  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.disallowSnapshot(snapshotRoot);
      return;
    }
    DisallowSnapshotRequestProto req = DisallowSnapshotRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot).build();

    asyncIpcClient(() -> rpcProxy.disallowSnapshot(null, req),
        res -> null, Void.class);
  }

  @Override
  public void renameSnapshot(
      String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.renameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName);
      return;
    }
    RenameSnapshotRequestProto req = RenameSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotOldName(snapshotOldName)
        .setSnapshotNewName(snapshotNewName).build();

    asyncIpcClient(() -> rpcProxy.renameSnapshot(null, req),
        res -> null, Void.class);
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getSnapshottableDirListing();
    }
    GetSnapshottableDirListingRequestProto req =
        GetSnapshottableDirListingRequestProto.newBuilder().build();

    return asyncIpcClient(() -> rpcProxy.getSnapshottableDirListing(null, req),
        res -> {
          if (res.hasSnapshottableDirList()) {
            return PBHelperClient.convert(res.getSnapshottableDirList());
          }
          return null;
        }, SnapshottableDirectoryStatus[].class);
  }

  @Override
  public SnapshotStatus[] getSnapshotListing(String path) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getSnapshotListing(path);
    }
    GetSnapshotListingRequestProto req =
        GetSnapshotListingRequestProto.newBuilder()
            .setSnapshotRoot(path).build();

    return asyncIpcClient(() -> rpcProxy.getSnapshotListing(null, req),
        res -> {
          if (res.hasSnapshotList()) {
            return PBHelperClient.convert(res.getSnapshotList());
          }
          return null;
        }, SnapshotStatus[].class);
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(
      String snapshotRoot, String fromSnapshot, String toSnapshot) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getSnapshotDiffReport(snapshotRoot, fromSnapshot, toSnapshot);
    }
    GetSnapshotDiffReportRequestProto req = GetSnapshotDiffReportRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot)
        .setFromSnapshot(fromSnapshot).setToSnapshot(toSnapshot).build();

    return asyncIpcClient(() -> rpcProxy.getSnapshotDiffReport(null, req),
        res -> PBHelperClient.convert(res.getDiffReport()), SnapshotDiffReport.class);
  }

  @Override
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String fromSnapshot, String toSnapshot,
      byte[] startPath, int index) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getSnapshotDiffReportListing(snapshotRoot, fromSnapshot,
          toSnapshot, startPath, index);
    }
    GetSnapshotDiffReportListingRequestProto req =
        GetSnapshotDiffReportListingRequestProto.newBuilder()
            .setSnapshotRoot(snapshotRoot).setFromSnapshot(fromSnapshot)
            .setToSnapshot(toSnapshot).setCursor(
                HdfsProtos.SnapshotDiffReportCursorProto.newBuilder()
                    .setStartPath(PBHelperClient.getByteString(startPath))
                    .setIndex(index).build()).build();

    return asyncIpcClient(() -> rpcProxy.getSnapshotDiffReportListing(null, req),
        res -> PBHelperClient.convert(res.getDiffReport()),
        SnapshotDiffReportListing.class);
  }

  @Override
  public long addCacheDirective(
      CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.addCacheDirective(directive, flags);
    }
    AddCacheDirectiveRequestProto.Builder builder =
        AddCacheDirectiveRequestProto.newBuilder().
            setInfo(PBHelperClient.convert(directive));
    if (!flags.isEmpty()) {
      builder.setCacheFlags(PBHelperClient.convertCacheFlags(flags));
    }

    return asyncIpcClient(() -> rpcProxy.addCacheDirective(null, builder.build()),
        res -> res.getId(), Long.class);
  }

  @Override
  public void modifyCacheDirective(
      CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.modifyCacheDirective(directive, flags);
      return;
    }
    ModifyCacheDirectiveRequestProto.Builder builder =
        ModifyCacheDirectiveRequestProto.newBuilder().
            setInfo(PBHelperClient.convert(directive));
    if (!flags.isEmpty()) {
      builder.setCacheFlags(PBHelperClient.convertCacheFlags(flags));
    }

    asyncIpcClient(() -> rpcProxy.modifyCacheDirective(null, builder.build()),
        res -> null, Void.class);
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.removeCacheDirective(id);
      return;
    }

    asyncIpcClient(() -> rpcProxy.removeCacheDirective(null,
        RemoveCacheDirectiveRequestProto.newBuilder().
            setId(id).build()),
        res -> null, Void.class);
  }

  @Override
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.listCacheDirectives(prevId, filter);
    }
    if (filter == null) {
      filter = new CacheDirectiveInfo.Builder().build();
    }
    CacheDirectiveInfo f = filter;

    return asyncIpcClient(() -> rpcProxy.listCacheDirectives(null,
        ListCacheDirectivesRequestProto.newBuilder().
            setPrevId(prevId).
            setFilter(PBHelperClient.convert(f)).
            build()),
        res -> new BatchedCacheEntries(res), BatchedEntries.class);
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.addCachePool(info);
      return;
    }
    AddCachePoolRequestProto.Builder builder =
        AddCachePoolRequestProto.newBuilder();
    builder.setInfo(PBHelperClient.convert(info));

    asyncIpcClient(() -> rpcProxy.addCachePool(null, builder.build()),
        res -> null, Void.class);
  }

  @Override
  public void modifyCachePool(CachePoolInfo req) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.modifyCachePool(req);
      return;
    }
    ModifyCachePoolRequestProto.Builder builder =
        ModifyCachePoolRequestProto.newBuilder();
    builder.setInfo(PBHelperClient.convert(req));

    asyncIpcClient(() -> rpcProxy.modifyCachePool(null, builder.build()),
        res -> null, Void.class);
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.removeCachePool(cachePoolName);
      return;
    }

    asyncIpcClient(() -> rpcProxy.removeCachePool(null,
            RemoveCachePoolRequestProto.newBuilder().
                setPoolName(cachePoolName).build()),
        res -> null, Void.class);
  }

  @Override
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.listCachePools(prevKey);
    }

    return asyncIpcClient(() -> rpcProxy.listCachePools(null,
            ListCachePoolsRequestProto.newBuilder().setPrevPoolName(prevKey).build()),
        res -> new BatchedCachePoolEntries(res), BatchedEntries.class);
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.modifyAclEntries(src, aclSpec);
      return;
    }
    ModifyAclEntriesRequestProto req = ModifyAclEntriesRequestProto
        .newBuilder().setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec)).build();

    asyncIpcClient(() -> rpcProxy.modifyAclEntries(null, req),
        res -> null, Void.class);
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.removeAclEntries(src, aclSpec);
      return;
    }
    RemoveAclEntriesRequestProto req = RemoveAclEntriesRequestProto
        .newBuilder().setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec)).build();

    asyncIpcClient(() -> rpcProxy.removeAclEntries(null, req),
        res -> null, Void.class);
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.removeDefaultAcl(src);
      return;
    }
    RemoveDefaultAclRequestProto req = RemoveDefaultAclRequestProto
        .newBuilder().setSrc(src).build();

    asyncIpcClient(() -> rpcProxy.removeDefaultAcl(null, req),
        res -> null, Void.class);
  }

  @Override
  public void removeAcl(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.removeAcl(src);
      return;
    }
    RemoveAclRequestProto req = RemoveAclRequestProto.newBuilder()
        .setSrc(src).build();

    asyncIpcClient(() -> rpcProxy.removeAcl(null, req),
        res -> null, Void.class);
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setAcl(src, aclSpec);
      return;
    }
    SetAclRequestProto req = SetAclRequestProto.newBuilder()
        .setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec))
        .build();

    asyncIpcClient(() -> rpcProxy.setAcl(null, req),
        res -> null, Void.class);
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getAclStatus(src);
    }
    GetAclStatusRequestProto req = GetAclStatusRequestProto.newBuilder()
        .setSrc(src).build();

    return asyncIpcClient(() -> rpcProxy.getAclStatus(null, req),
        res -> PBHelperClient.convert(res), AclStatus.class);
  }

  @Override
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.createEncryptionZone(src, keyName);
      return;
    }
    final CreateEncryptionZoneRequestProto.Builder builder =
        CreateEncryptionZoneRequestProto.newBuilder();
    builder.setSrc(src);
    if (keyName != null && !keyName.isEmpty()) {
      builder.setKeyName(keyName);
    }
    CreateEncryptionZoneRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.createEncryptionZone(null, req),
        res -> null, Void.class);
  }

  @Override
  public EncryptionZone getEZForPath(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getEZForPath(src);
    }
    final GetEZForPathRequestProto.Builder builder =
        GetEZForPathRequestProto.newBuilder();
    builder.setSrc(src);
    final GetEZForPathRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.getEZForPath(null, req),
        res -> {
          if (res.hasZone()) {
            return PBHelperClient.convert(res.getZone());
          } else {
            return null;
          }
        }, EncryptionZone.class);
  }

  @Override
  public BatchedEntries<EncryptionZone> listEncryptionZones(long id)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.listEncryptionZones(id);
    }
    final ListEncryptionZonesRequestProto req =
        ListEncryptionZonesRequestProto.newBuilder()
            .setId(id)
            .build();

    return asyncIpcClient(() -> rpcProxy.listEncryptionZones(null, req),
        res -> {
          List<EncryptionZone> elements =
              Lists.newArrayListWithCapacity(res.getZonesCount());
          for (EncryptionZoneProto p : res.getZonesList()) {
            elements.add(PBHelperClient.convert(p));
          }
          return new BatchedListEntries<>(elements, res.getHasMore());
        }, BatchedEntries.class);
  }

  @Override
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setErasureCodingPolicy(src, ecPolicyName);
      return;
    }
    final SetErasureCodingPolicyRequestProto.Builder builder =
        SetErasureCodingPolicyRequestProto.newBuilder();
    builder.setSrc(src);
    if (ecPolicyName != null) {
      builder.setEcPolicyName(ecPolicyName);
    }
    SetErasureCodingPolicyRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.setErasureCodingPolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public void unsetErasureCodingPolicy(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.unsetErasureCodingPolicy(src);
      return;
    }
    final UnsetErasureCodingPolicyRequestProto.Builder builder =
        UnsetErasureCodingPolicyRequestProto.newBuilder();
    builder.setSrc(src);
    UnsetErasureCodingPolicyRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.unsetErasureCodingPolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      final String... policyNames) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getECTopologyResultForPolicies(policyNames);
    }
    final GetECTopologyResultForPoliciesRequestProto.Builder builder =
        GetECTopologyResultForPoliciesRequestProto.newBuilder();
    builder.addAllPolicies(Arrays.asList(policyNames));
    GetECTopologyResultForPoliciesRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.getECTopologyResultForPolicies(null, req),
        res -> PBHelperClient.convertECTopologyVerifierResultProto(res.getResponse()),
        ECTopologyVerifierResult.class);
  }

  @Override
  public void reencryptEncryptionZone(String zone, ReencryptAction action)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.reencryptEncryptionZone(zone, action);
      return;
    }
    final ReencryptEncryptionZoneRequestProto.Builder builder =
        ReencryptEncryptionZoneRequestProto.newBuilder();
    builder.setZone(zone).setAction(PBHelperClient.convert(action));
    ReencryptEncryptionZoneRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.reencryptEncryptionZone(null, req),
        res -> null, Void.class);
  }

  @Override
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(long id)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.listReencryptionStatus(id);
    }
    final ListReencryptionStatusRequestProto req =
        ListReencryptionStatusRequestProto.newBuilder().setId(id).build();

    return asyncIpcClient(() -> rpcProxy.listReencryptionStatus(null, req),
        res -> {
          List<ZoneReencryptionStatus> elements =
              Lists.newArrayListWithCapacity(res.getStatusesCount());
          for (ZoneReencryptionStatusProto p : res.getStatusesList()) {
            elements.add(PBHelperClient.convert(p));
          }
          return new BatchedListEntries<>(elements, res.getHasMore());
        }, BatchedEntries.class);
  }

  @Override
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setXAttr(src, xAttr, flag);
      return;
    }
    SetXAttrRequestProto req = SetXAttrRequestProto.newBuilder()
        .setSrc(src)
        .setXAttr(PBHelperClient.convertXAttrProto(xAttr))
        .setFlag(PBHelperClient.convert(flag))
        .build();

    asyncIpcClient(() -> rpcProxy.setXAttr(null, req),
        res -> null, Void.class);
  }

  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getXAttrs(src, xAttrs);
    }
    GetXAttrsRequestProto.Builder builder = GetXAttrsRequestProto.newBuilder();
    builder.setSrc(src);
    if (xAttrs != null) {
      builder.addAllXAttrs(PBHelperClient.convertXAttrProto(xAttrs));
    }
    GetXAttrsRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.getXAttrs(null, req),
        res -> PBHelperClient.convert(res), List.class);
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.listXAttrs(src);
    }
    ListXAttrsRequestProto.Builder builder =
        ListXAttrsRequestProto.newBuilder();
    builder.setSrc(src);
    ListXAttrsRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.listXAttrs(null, req),
        res -> PBHelperClient.convert(res), List.class);
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.removeXAttr(src, xAttr);
      return;
    }
    RemoveXAttrRequestProto req = RemoveXAttrRequestProto
        .newBuilder().setSrc(src)
        .setXAttr(PBHelperClient.convertXAttrProto(xAttr)).build();

    asyncIpcClient(() -> rpcProxy.removeXAttr(null, req),
        res -> null, Void.class);
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.checkAccess(path, mode);
      return;
    }
    CheckAccessRequestProto req = CheckAccessRequestProto.newBuilder()
        .setPath(path).setMode(PBHelperClient.convert(mode)).build();

    asyncIpcClient(() -> rpcProxy.checkAccess(null, req),
        res -> null, Void.class);
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setStoragePolicy(src, policyName);
      return;
    }
    SetStoragePolicyRequestProto req = SetStoragePolicyRequestProto
        .newBuilder().setSrc(src).setPolicyName(policyName).build();

    asyncIpcClient(() -> rpcProxy.setStoragePolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public void unsetStoragePolicy(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.unsetStoragePolicy(src);
      return;
    }
    UnsetStoragePolicyRequestProto req = UnsetStoragePolicyRequestProto
        .newBuilder().setSrc(src).build();

    asyncIpcClient(() -> rpcProxy.unsetStoragePolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getStoragePolicy(path);
    }
    GetStoragePolicyRequestProto request = GetStoragePolicyRequestProto
        .newBuilder().setPath(path).build();

    return asyncIpcClient(() -> rpcProxy.getStoragePolicy(null, request),
        res -> PBHelperClient.convert(res.getStoragePolicy()),
        BlockStoragePolicy.class);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getStoragePolicies();
    }

    return asyncIpcClient(() -> rpcProxy.getStoragePolicies(null,
            VOID_GET_STORAGE_POLICIES_REQUEST),
        res -> PBHelperClient.convertStoragePolicies(res.getPoliciesList()),
        BlockStoragePolicy[].class);
  }

  public long getCurrentEditLogTxid() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getCurrentEditLogTxid();
    }
    GetCurrentEditLogTxidRequestProto req = GetCurrentEditLogTxidRequestProto
        .getDefaultInstance();

    return asyncIpcClient(() -> rpcProxy.getCurrentEditLogTxid(null, req),
        res -> res.getTxid(), Long.class);
  }

  @Override
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getEditsFromTxid(txid);
    }
    GetEditsFromTxidRequestProto req = GetEditsFromTxidRequestProto.newBuilder()
        .setTxid(txid).build();

    return asyncIpcClient(() -> rpcProxy.getEditsFromTxid(null, req),
        res -> PBHelperClient.convert(res), EventBatchList.class);
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.addErasureCodingPolicies(policies);
    }
    List<ErasureCodingPolicyProto> protos = Arrays.stream(policies)
        .map(PBHelperClient::convertErasureCodingPolicy)
        .collect(Collectors.toList());
    AddErasureCodingPoliciesRequestProto req =
        AddErasureCodingPoliciesRequestProto.newBuilder()
            .addAllEcPolicies(protos).build();

    return asyncIpcClient(() -> rpcProxy.addErasureCodingPolicies(null, req),
        res -> res.getResponsesList().stream()
            .map(PBHelperClient::convertAddErasureCodingPolicyResponse)
            .toArray(AddErasureCodingPolicyResponse[]::new),
        AddErasureCodingPolicyResponse[].class);
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.removeErasureCodingPolicy(ecPolicyName);
      return;
    }
    RemoveErasureCodingPolicyRequestProto.Builder builder =
        RemoveErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    RemoveErasureCodingPolicyRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.removeErasureCodingPolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.enableErasureCodingPolicy(ecPolicyName);
      return;
    }
    EnableErasureCodingPolicyRequestProto.Builder builder =
        EnableErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    EnableErasureCodingPolicyRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.enableErasureCodingPolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.disableErasureCodingPolicy(ecPolicyName);
      return;
    }
    DisableErasureCodingPolicyRequestProto.Builder builder =
        DisableErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    DisableErasureCodingPolicyRequestProto req = builder.build();

    asyncIpcClient(() -> rpcProxy.disableErasureCodingPolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getErasureCodingPolicies();
    }

    return asyncIpcClient(() -> rpcProxy.getErasureCodingPolicies(
        null, VOID_GET_EC_POLICIES_REQUEST),
        res -> {
          ErasureCodingPolicyInfo[] ecPolicies =
              new ErasureCodingPolicyInfo[res.getEcPoliciesCount()];
          int i = 0;
          for (ErasureCodingPolicyProto proto : res.getEcPoliciesList()) {
            ecPolicies[i++] =
                PBHelperClient.convertErasureCodingPolicyInfo(proto);
          }
          return ecPolicies;
        }, ErasureCodingPolicyInfo[].class);
  }

  @Override
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getErasureCodingCodecs();
    }

    return asyncIpcClient(() -> rpcProxy
            .getErasureCodingCodecs(null, VOID_GET_EC_CODEC_REQUEST),
        res -> {
          Map<String, String> ecCodecs = new HashMap<>();
          for (CodecProto codec : res.getCodecList()) {
            ecCodecs.put(codec.getCodec(), codec.getCoders());
          }
          return ecCodecs;
        }, Map.class);
  }

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getErasureCodingPolicy(src);
    }
    GetErasureCodingPolicyRequestProto req =
        GetErasureCodingPolicyRequestProto.newBuilder().setSrc(src).build();

    return asyncIpcClient(() -> rpcProxy.getErasureCodingPolicy(null, req),
        res -> {
          if (res.hasEcPolicy()) {
            return PBHelperClient.convertErasureCodingPolicy(
                res.getEcPolicy());
          }
          return null;
        }, ErasureCodingPolicy.class);
  }

  @Override
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getQuotaUsage(path);
    }
    GetQuotaUsageRequestProto req =
        GetQuotaUsageRequestProto.newBuilder().setPath(path).build();

    return asyncIpcClient(() -> rpcProxy.getQuotaUsage(null, req),
        res -> PBHelperClient.convert(res.getUsage()), QuotaUsage.class);
  }

  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(
      long prevId, EnumSet<OpenFilesType> openFilesTypes,
      String path) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.listOpenFiles(prevId, openFilesTypes, path);
    }
    ListOpenFilesRequestProto.Builder req =
        ListOpenFilesRequestProto.newBuilder().setId(prevId);
    if (openFilesTypes != null) {
      req.addAllTypes(PBHelperClient.convertOpenFileTypes(openFilesTypes));
    }
    req.setPath(path);

    return asyncIpcClient(() -> rpcProxy.listOpenFiles(null, req.build()),
        res -> {
          List<OpenFileEntry> openFileEntries =
              Lists.newArrayListWithCapacity(res.getEntriesCount());
          for (OpenFilesBatchResponseProto p : res.getEntriesList()) {
            openFileEntries.add(PBHelperClient.convert(p));
          }
          return new BatchedListEntries<>(openFileEntries, res.getHasMore());
        }, BatchedEntries.class);
  }

  @Override
  public void msync() throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.msync();
      return;
    }
    MsyncRequestProto.Builder req = MsyncRequestProto.newBuilder();

    asyncIpcClient(() -> rpcProxy.msync(null, req.build()),
        res -> null, Void.class);
  }

  @Override
  public void satisfyStoragePolicy(String src) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.satisfyStoragePolicy(src);
      return;
    }
    SatisfyStoragePolicyRequestProto req =
        SatisfyStoragePolicyRequestProto.newBuilder().setSrc(src).build();

    asyncIpcClient(() -> rpcProxy.satisfyStoragePolicy(null, req),
        res -> null, Void.class);
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getSlowDatanodeReport();
    }
    GetSlowDatanodeReportRequestProto req =
        GetSlowDatanodeReportRequestProto.newBuilder().build();

    return asyncIpcClient(() -> rpcProxy.getSlowDatanodeReport(null, req),
        res -> PBHelperClient.convert(res.getDatanodeInfoProtoList()),
        DatanodeInfo[].class);
  }

  @Override
  public HAServiceProtocol.HAServiceState getHAServiceState()
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getHAServiceState();
    }
    HAServiceStateRequestProto req =
        HAServiceStateRequestProto.newBuilder().build();

    return asyncIpcClient(() -> rpcProxy.getHAServiceState(null, req),
        res -> {
          switch(res.getState()) {
            case ACTIVE:
              return HAServiceProtocol.HAServiceState.ACTIVE;
            case STANDBY:
              return HAServiceProtocol.HAServiceState.STANDBY;
            case OBSERVER:
              return HAServiceProtocol.HAServiceState.OBSERVER;
            case INITIALIZING:
            default:
              return HAServiceProtocol.HAServiceState.INITIALIZING;
          }
        }, HAServiceProtocol.HAServiceState.class);
  }

  @Override
  public Path getEnclosingRoot(String filename) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getEnclosingRoot(filename);
    }
    final GetEnclosingRootRequestProto.Builder builder =
        GetEnclosingRootRequestProto.newBuilder();
    builder.setFilename(filename);
    final GetEnclosingRootRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.getEnclosingRoot(null, req),
        res -> new Path(res.getEnclosingRootPath()),
        Path.class);
  }
}
