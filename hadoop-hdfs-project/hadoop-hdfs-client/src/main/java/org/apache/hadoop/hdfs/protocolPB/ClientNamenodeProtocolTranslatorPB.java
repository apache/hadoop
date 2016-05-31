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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import com.google.common.collect.Lists;

import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CheckAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetQuotaUsageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2RequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeResponseProto;
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
import org.apache.hadoop.hdfs.protocol.proto.*;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.EncryptionZoneProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.SetErasureCodingPolicyRequestProto;
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
import org.apache.hadoop.io.retry.AsyncCallHandler;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.util.concurrent.AsyncGet;

/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to the
 * new PB types.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientNamenodeProtocolTranslatorPB implements
    ProtocolMetaInterface, ClientProtocol, Closeable, ProtocolTranslator {
  final private ClientNamenodeProtocolPB rpcProxy;

  static final GetServerDefaultsRequestProto VOID_GET_SERVER_DEFAULT_REQUEST =
      GetServerDefaultsRequestProto.newBuilder().build();

  private final static GetFsStatusRequestProto VOID_GET_FSSTATUS_REQUEST =
      GetFsStatusRequestProto.newBuilder().build();

  private final static RollEditsRequestProto VOID_ROLLEDITS_REQUEST =
      RollEditsRequestProto.getDefaultInstance();

  private final static RefreshNodesRequestProto VOID_REFRESH_NODES_REQUEST =
      RefreshNodesRequestProto.newBuilder().build();

  private final static FinalizeUpgradeRequestProto
      VOID_FINALIZE_UPGRADE_REQUEST =
      FinalizeUpgradeRequestProto.newBuilder().build();

  private final static GetDataEncryptionKeyRequestProto
      VOID_GET_DATA_ENCRYPTIONKEY_REQUEST =
      GetDataEncryptionKeyRequestProto.newBuilder().build();

  private final static GetStoragePoliciesRequestProto
      VOID_GET_STORAGE_POLICIES_REQUEST =
      GetStoragePoliciesRequestProto.newBuilder().build();

  private final static GetErasureCodingPoliciesRequestProto
      VOID_GET_EC_POLICIES_REQUEST = GetErasureCodingPoliciesRequestProto
      .newBuilder().build();

  public ClientNamenodeProtocolTranslatorPB(ClientNamenodeProtocolPB proxy) {
    rpcProxy = proxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws IOException {
    GetBlockLocationsRequestProto req = GetBlockLocationsRequestProto
        .newBuilder()
        .setSrc(src)
        .setOffset(offset)
        .setLength(length)
        .build();
    try {
      GetBlockLocationsResponseProto resp = rpcProxy.getBlockLocations(null,
          req);
      return resp.hasLocations() ?
          PBHelperClient.convert(resp.getLocations()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    GetServerDefaultsRequestProto req = VOID_GET_SERVER_DEFAULT_REQUEST;
    try {
      return PBHelperClient
          .convert(rpcProxy.getServerDefaults(null, req).getServerDefaults());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions)
      throws IOException {
    CreateRequestProto.Builder builder = CreateRequestProto.newBuilder()
        .setSrc(src)
        .setMasked(PBHelperClient.convert(masked))
        .setClientName(clientName)
        .setCreateFlag(PBHelperClient.convertCreateFlag(flag))
        .setCreateParent(createParent)
        .setReplication(replication)
        .setBlockSize(blockSize);
    builder.addAllCryptoProtocolVersion(
        PBHelperClient.convert(supportedVersions));
    CreateRequestProto req = builder.build();
    try {
      CreateResponseProto res = rpcProxy.create(null, req);
      return res.hasFs() ? PBHelperClient.convert(res.getFs()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    TruncateRequestProto req = TruncateRequestProto.newBuilder()
        .setSrc(src)
        .setNewLength(newLength)
        .setClientName(clientName)
        .build();
    try {
      return rpcProxy.truncate(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LastBlockWithStatus append(String src, String clientName,
      EnumSetWritable<CreateFlag> flag) throws IOException {
    AppendRequestProto req = AppendRequestProto.newBuilder().setSrc(src)
        .setClientName(clientName).setFlag(
            PBHelperClient.convertCreateFlag(flag))
        .build();
    try {
      AppendResponseProto res = rpcProxy.append(null, req);
      LocatedBlock lastBlock = res.hasBlock() ? PBHelperClient
          .convertLocatedBlockProto(res.getBlock()) : null;
      HdfsFileStatus stat = (res.hasStat()) ?
          PBHelperClient.convert(res.getStat()) : null;
      return new LastBlockWithStatus(lastBlock, stat);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {
    SetReplicationRequestProto req = SetReplicationRequestProto.newBuilder()
        .setSrc(src)
        .setReplication(replication)
        .build();
    try {
      return rpcProxy.setReplication(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws IOException {
    SetPermissionRequestProto req = SetPermissionRequestProto.newBuilder()
        .setSrc(src)
        .setPermission(PBHelperClient.convert(permission))
        .build();
    try {
      if (Client.isAsynchronousMode()) {
        rpcProxy.setPermission(null, req);
        setAsyncReturnValue();
      } else {
        rpcProxy.setPermission(null, req);
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private void setAsyncReturnValue() {
    final AsyncGet<Message, Exception> asyncReturnMessage
        = ProtobufRpcEngine.getAsyncReturnMessage();
    final AsyncGet<Void, Exception> asyncGet
        = new AsyncGet<Void, Exception>() {
      @Override
      public Void get(long timeout, TimeUnit unit) throws Exception {
        asyncReturnMessage.get(timeout, unit);
        return null;
      }

      @Override
      public boolean isDone() {
        return asyncReturnMessage.isDone();
      }
    };
    AsyncCallHandler.setLowerLayerAsyncReturn(asyncGet);
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    SetOwnerRequestProto.Builder req = SetOwnerRequestProto.newBuilder()
        .setSrc(src);
    if (username != null)
      req.setUsername(username);
    if (groupname != null)
      req.setGroupname(groupname);
    try {
      if (Client.isAsynchronousMode()) {
        rpcProxy.setOwner(null, req.build());
        setAsyncReturnValue();
      } else {
        rpcProxy.setOwner(null, req.build());
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
      String holder) throws IOException {
    AbandonBlockRequestProto req = AbandonBlockRequestProto.newBuilder()
        .setB(PBHelperClient.convert(b)).setSrc(src).setHolder(holder)
        .setFileId(fileId).build();
    try {
      rpcProxy.abandonBlock(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
      String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags)
      throws IOException {
    AddBlockRequestProto.Builder req = AddBlockRequestProto.newBuilder()
        .setSrc(src).setClientName(clientName).setFileId(fileId);
    if (previous != null)
      req.setPrevious(PBHelperClient.convert(previous));
    if (excludeNodes != null)
      req.addAllExcludeNodes(PBHelperClient.convert(excludeNodes));
    if (favoredNodes != null) {
      req.addAllFavoredNodes(Arrays.asList(favoredNodes));
    }
    if (addBlockFlags != null) {
      req.addAllFlags(PBHelperClient.convertAddBlockFlags(
          addBlockFlags));
    }
    try {
      return PBHelperClient.convertLocatedBlockProto(
          rpcProxy.addBlock(null, req.build()).getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock getAdditionalDatanode(String src, long fileId,
      ExtendedBlock blk, DatanodeInfo[] existings, String[] existingStorageIDs,
      DatanodeInfo[] excludes, int numAdditionalNodes, String clientName)
      throws IOException {
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
    try {
      return PBHelperClient.convertLocatedBlockProto(
          rpcProxy.getAdditionalDatanode(null, req).getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean complete(String src, String clientName,
      ExtendedBlock last, long fileId) throws IOException {
    CompleteRequestProto.Builder req = CompleteRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName)
        .setFileId(fileId);
    if (last != null)
      req.setLast(PBHelperClient.convert(last));
    try {
      return rpcProxy.complete(null, req.build()).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ReportBadBlocksRequestProto req = ReportBadBlocksRequestProto.newBuilder()
        .addAllBlocks(Arrays.asList(
            PBHelperClient.convertLocatedBlocks(blocks)))
        .build();
    try {
      rpcProxy.reportBadBlocks(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    RenameRequestProto req = RenameRequestProto.newBuilder()
        .setSrc(src)
        .setDst(dst).build();

    try {
      return rpcProxy.rename(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }


  @Override
  public void rename2(String src, String dst, Rename... options)
      throws IOException {
    boolean overwrite = false;
    if (options != null) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    Rename2RequestProto req = Rename2RequestProto.newBuilder().
        setSrc(src).
        setDst(dst).setOverwriteDest(overwrite).
        build();
    try {
      if (Client.isAsynchronousMode()) {
        rpcProxy.rename2(null, req);
        setAsyncReturnValue();
      } else {
        rpcProxy.rename2(null, req);
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException {
    ConcatRequestProto req = ConcatRequestProto.newBuilder().
        setTrg(trg).
        addAllSrcs(Arrays.asList(srcs)).build();
    try {
      rpcProxy.concat(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }


  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    DeleteRequestProto req = DeleteRequestProto.newBuilder().setSrc(src)
        .setRecursive(recursive).build();
    try {
      return rpcProxy.delete(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    MkdirsRequestProto req = MkdirsRequestProto.newBuilder()
        .setSrc(src)
        .setMasked(PBHelperClient.convert(masked))
        .setCreateParent(createParent).build();

    try {
      return rpcProxy.mkdirs(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    GetListingRequestProto req = GetListingRequestProto.newBuilder()
        .setSrc(src)
        .setStartAfter(ByteString.copyFrom(startAfter))
        .setNeedLocation(needLocation).build();
    try {
      GetListingResponseProto result = rpcProxy.getListing(null, req);

      if (result.hasDirList()) {
        return PBHelperClient.convert(result.getDirList());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void renewLease(String clientName) throws IOException {
    RenewLeaseRequestProto req = RenewLeaseRequestProto.newBuilder()
        .setClientName(clientName).build();
    try {
      rpcProxy.renewLease(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    RecoverLeaseRequestProto req = RecoverLeaseRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName).build();
    try {
      return rpcProxy.recoverLease(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long[] getStats() throws IOException {
    try {
      return PBHelperClient.convert(rpcProxy.getFsStats(null,
          VOID_GET_FSSTATUS_REQUEST));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    GetDatanodeReportRequestProto req = GetDatanodeReportRequestProto
        .newBuilder()
        .setType(PBHelperClient.convert(type)).build();
    try {
      return PBHelperClient.convert(
          rpcProxy.getDatanodeReport(null, req).getDiList());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    final GetDatanodeStorageReportRequestProto req
        = GetDatanodeStorageReportRequestProto.newBuilder()
        .setType(PBHelperClient.convert(type)).build();
    try {
      return PBHelperClient.convertDatanodeStorageReports(
          rpcProxy.getDatanodeStorageReport(null, req)
              .getDatanodeStorageReportsList());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException {
    GetPreferredBlockSizeRequestProto req = GetPreferredBlockSizeRequestProto
        .newBuilder()
        .setFilename(filename)
        .build();
    try {
      return rpcProxy.getPreferredBlockSize(null, req).getBsize();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    SetSafeModeRequestProto req = SetSafeModeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action))
        .setChecked(isChecked).build();
    try {
      return rpcProxy.setSafeMode(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    try {
      SaveNamespaceRequestProto req = SaveNamespaceRequestProto.newBuilder()
          .setTimeWindow(timeWindow).setTxGap(txGap).build();
      return rpcProxy.saveNamespace(null, req).getSaved();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long rollEdits() throws IOException {
    try {
      RollEditsResponseProto resp = rpcProxy.rollEdits(null,
          VOID_ROLLEDITS_REQUEST);
      return resp.getNewSegmentTxId();
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException{
    RestoreFailedStorageRequestProto req = RestoreFailedStorageRequestProto
        .newBuilder()
        .setArg(arg).build();
    try {
      return rpcProxy.restoreFailedStorage(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void refreshNodes() throws IOException {
    try {
      rpcProxy.refreshNodes(null, VOID_REFRESH_NODES_REQUEST);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    try {
      rpcProxy.finalizeUpgrade(null, VOID_FINALIZE_UPGRADE_REQUEST);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    final RollingUpgradeRequestProto r = RollingUpgradeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action)).build();
    try {
      final RollingUpgradeResponseProto proto =
          rpcProxy.rollingUpgrade(null, r);
      if (proto.hasRollingUpgradeInfo()) {
        return PBHelperClient.convert(proto.getRollingUpgradeInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    ListCorruptFileBlocksRequestProto.Builder req =
        ListCorruptFileBlocksRequestProto.newBuilder().setPath(path);
    if (cookie != null)
      req.setCookie(cookie);
    try {
      return PBHelperClient.convert(
          rpcProxy.listCorruptFileBlocks(null, req.build()).getCorrupt());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void metaSave(String filename) throws IOException {
    MetaSaveRequestProto req = MetaSaveRequestProto.newBuilder()
        .setFilename(filename).build();
    try {
      rpcProxy.metaSave(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }

  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    GetFileInfoRequestProto req = GetFileInfoRequestProto.newBuilder()
        .setSrc(src).build();
    try {
      GetFileInfoResponseProto res = rpcProxy.getFileInfo(null, req);
      return res.hasFs() ? PBHelperClient.convert(res.getFs()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    GetFileLinkInfoRequestProto req = GetFileLinkInfoRequestProto.newBuilder()
        .setSrc(src).build();
    try {
      GetFileLinkInfoResponseProto result = rpcProxy.getFileLinkInfo(null, req);
      return result.hasFs() ?
          PBHelperClient.convert(rpcProxy.getFileLinkInfo(null, req).getFs()) :
          null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    GetContentSummaryRequestProto req = GetContentSummaryRequestProto
        .newBuilder()
        .setPath(path)
        .build();
    try {
      return PBHelperClient.convert(rpcProxy.getContentSummary(null, req)
          .getSummary());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
      StorageType type) throws IOException {
    final SetQuotaRequestProto.Builder builder
        = SetQuotaRequestProto.newBuilder()
        .setPath(path)
        .setNamespaceQuota(namespaceQuota)
        .setStoragespaceQuota(storagespaceQuota);
    if (type != null) {
      builder.setStorageType(PBHelperClient.convertStorageType(type));
    }
    final SetQuotaRequestProto req = builder.build();
    try {
      rpcProxy.setQuota(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void fsync(String src, long fileId, String client,
      long lastBlockLength) throws IOException {
    FsyncRequestProto req = FsyncRequestProto.newBuilder().setSrc(src)
        .setClient(client).setLastBlockLength(lastBlockLength)
        .setFileId(fileId).build();
    try {
      rpcProxy.fsync(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    SetTimesRequestProto req = SetTimesRequestProto.newBuilder()
        .setSrc(src)
        .setMtime(mtime)
        .setAtime(atime)
        .build();
    try {
      rpcProxy.setTimes(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerm,
      boolean createParent) throws IOException {
    CreateSymlinkRequestProto req = CreateSymlinkRequestProto.newBuilder()
        .setTarget(target)
        .setLink(link)
        .setDirPerm(PBHelperClient.convert(dirPerm))
        .setCreateParent(createParent)
        .build();
    try {
      rpcProxy.createSymlink(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public String getLinkTarget(String path) throws IOException {
    GetLinkTargetRequestProto req = GetLinkTargetRequestProto.newBuilder()
        .setPath(path).build();
    try {
      GetLinkTargetResponseProto rsp = rpcProxy.getLinkTarget(null, req);
      return rsp.hasTargetPath() ? rsp.getTargetPath() : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException {
    UpdateBlockForPipelineRequestProto req = UpdateBlockForPipelineRequestProto
        .newBuilder()
        .setBlock(PBHelperClient.convert(block))
        .setClientName(clientName)
        .build();
    try {
      return PBHelperClient.convertLocatedBlockProto(
          rpcProxy.updateBlockForPipeline(null, req).getBlock());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] storageIDs)
      throws IOException {
    UpdatePipelineRequestProto req = UpdatePipelineRequestProto.newBuilder()
        .setClientName(clientName)
        .setOldBlock(PBHelperClient.convert(oldBlock))
        .setNewBlock(PBHelperClient.convert(newBlock))
        .addAllNewNodes(Arrays.asList(PBHelperClient.convert(newNodes)))
        .addAllStorageIDs(storageIDs == null ? null : Arrays.asList(storageIDs))
        .build();
    try {
      rpcProxy.updatePipeline(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer == null ? "" : renewer.toString())
        .build();
    try {
      GetDelegationTokenResponseProto resp =
          rpcProxy.getDelegationToken(null, req);
      return resp.hasToken() ?
          PBHelperClient.convertDelegationToken(resp.getToken()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    RenewDelegationTokenRequestProto req =
        RenewDelegationTokenRequestProto.newBuilder().
            setToken(PBHelperClient.convert(token)).
            build();
    try {
      return rpcProxy.renewDelegationToken(null, req).getNewExpiryTime();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto
        .newBuilder()
        .setToken(PBHelperClient.convert(token))
        .build();
    try {
      rpcProxy.cancelDelegationToken(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    SetBalancerBandwidthRequestProto req =
        SetBalancerBandwidthRequestProto.newBuilder()
            .setBandwidth(bandwidth)
            .build();
    try {
      rpcProxy.setBalancerBandwidth(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        ClientNamenodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(ClientNamenodeProtocolPB.class), methodName);
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    try {
      GetDataEncryptionKeyResponseProto rsp = rpcProxy.getDataEncryptionKey(
          null, VOID_GET_DATA_ENCRYPTIONKEY_REQUEST);
      return rsp.hasDataEncryptionKey() ?
          PBHelperClient.convert(rsp.getDataEncryptionKey()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }


  @Override
  public boolean isFileClosed(String src) throws IOException {
    IsFileClosedRequestProto req = IsFileClosedRequestProto.newBuilder()
        .setSrc(src).build();
    try {
      return rpcProxy.isFileClosed(null, req).getResult();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    final CreateSnapshotRequestProto.Builder builder
        = CreateSnapshotRequestProto.newBuilder().setSnapshotRoot(snapshotRoot);
    if (snapshotName != null) {
      builder.setSnapshotName(snapshotName);
    }
    final CreateSnapshotRequestProto req = builder.build();
    try {
      return rpcProxy.createSnapshot(null, req).getSnapshotPath();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    DeleteSnapshotRequestProto req = DeleteSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotName(snapshotName).build();
    try {
      rpcProxy.deleteSnapshot(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    AllowSnapshotRequestProto req = AllowSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).build();
    try {
      rpcProxy.allowSnapshot(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    DisallowSnapshotRequestProto req = DisallowSnapshotRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot).build();
    try {
      rpcProxy.disallowSnapshot(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    RenameSnapshotRequestProto req = RenameSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotOldName(snapshotOldName)
        .setSnapshotNewName(snapshotNewName).build();
    try {
      rpcProxy.renameSnapshot(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    GetSnapshottableDirListingRequestProto req =
        GetSnapshottableDirListingRequestProto.newBuilder().build();
    try {
      GetSnapshottableDirListingResponseProto result = rpcProxy
          .getSnapshottableDirListing(null, req);

      if (result.hasSnapshottableDirList()) {
        return PBHelperClient.convert(result.getSnapshottableDirList());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String fromSnapshot, String toSnapshot) throws IOException {
    GetSnapshotDiffReportRequestProto req = GetSnapshotDiffReportRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot)
        .setFromSnapshot(fromSnapshot).setToSnapshot(toSnapshot).build();
    try {
      GetSnapshotDiffReportResponseProto result =
          rpcProxy.getSnapshotDiffReport(null, req);

      return PBHelperClient.convert(result.getDiffReport());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    try {
      AddCacheDirectiveRequestProto.Builder builder =
          AddCacheDirectiveRequestProto.newBuilder().
              setInfo(PBHelperClient.convert(directive));
      if (!flags.isEmpty()) {
        builder.setCacheFlags(PBHelperClient.convertCacheFlags(flags));
      }
      return rpcProxy.addCacheDirective(null, builder.build()).getId();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    try {
      ModifyCacheDirectiveRequestProto.Builder builder =
          ModifyCacheDirectiveRequestProto.newBuilder().
              setInfo(PBHelperClient.convert(directive));
      if (!flags.isEmpty()) {
        builder.setCacheFlags(PBHelperClient.convertCacheFlags(flags));
      }
      rpcProxy.modifyCacheDirective(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void removeCacheDirective(long id)
      throws IOException {
    try {
      rpcProxy.removeCacheDirective(null,
          RemoveCacheDirectiveRequestProto.newBuilder().
              setId(id).build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private static class BatchedCacheEntries
      implements BatchedEntries<CacheDirectiveEntry> {
    private final ListCacheDirectivesResponseProto response;

    BatchedCacheEntries(
        ListCacheDirectivesResponseProto response) {
      this.response = response;
    }

    @Override
    public CacheDirectiveEntry get(int i) {
      return PBHelperClient.convert(response.getElements(i));
    }

    @Override
    public int size() {
      return response.getElementsCount();
    }

    @Override
    public boolean hasMore() {
      return response.getHasMore();
    }
  }

  @Override
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId,
      CacheDirectiveInfo filter) throws IOException {
    if (filter == null) {
      filter = new CacheDirectiveInfo.Builder().build();
    }
    try {
      return new BatchedCacheEntries(
          rpcProxy.listCacheDirectives(null,
              ListCacheDirectivesRequestProto.newBuilder().
                  setPrevId(prevId).
                  setFilter(PBHelperClient.convert(filter)).
                  build()));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    AddCachePoolRequestProto.Builder builder =
        AddCachePoolRequestProto.newBuilder();
    builder.setInfo(PBHelperClient.convert(info));
    try {
      rpcProxy.addCachePool(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void modifyCachePool(CachePoolInfo req) throws IOException {
    ModifyCachePoolRequestProto.Builder builder =
        ModifyCachePoolRequestProto.newBuilder();
    builder.setInfo(PBHelperClient.convert(req));
    try {
      rpcProxy.modifyCachePool(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    try {
      rpcProxy.removeCachePool(null,
          RemoveCachePoolRequestProto.newBuilder().
              setPoolName(cachePoolName).build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private static class BatchedCachePoolEntries
      implements BatchedEntries<CachePoolEntry> {
    private final ListCachePoolsResponseProto proto;

    public BatchedCachePoolEntries(ListCachePoolsResponseProto proto) {
      this.proto = proto;
    }

    @Override
    public CachePoolEntry get(int i) {
      CachePoolEntryProto elem = proto.getEntries(i);
      return PBHelperClient.convert(elem);
    }

    @Override
    public int size() {
      return proto.getEntriesCount();
    }

    @Override
    public boolean hasMore() {
      return proto.getHasMore();
    }
  }

  @Override
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    try {
      return new BatchedCachePoolEntries(
          rpcProxy.listCachePools(null,
              ListCachePoolsRequestProto.newBuilder().
                  setPrevPoolName(prevKey).build()));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    ModifyAclEntriesRequestProto req = ModifyAclEntriesRequestProto
        .newBuilder().setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec)).build();
    try {
      rpcProxy.modifyAclEntries(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    RemoveAclEntriesRequestProto req = RemoveAclEntriesRequestProto
        .newBuilder().setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec)).build();
    try {
      rpcProxy.removeAclEntries(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    RemoveDefaultAclRequestProto req = RemoveDefaultAclRequestProto
        .newBuilder().setSrc(src).build();
    try {
      rpcProxy.removeDefaultAcl(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void removeAcl(String src) throws IOException {
    RemoveAclRequestProto req = RemoveAclRequestProto.newBuilder()
        .setSrc(src).build();
    try {
      rpcProxy.removeAcl(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    SetAclRequestProto req = SetAclRequestProto.newBuilder()
        .setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec))
        .build();
    try {
      if (Client.isAsynchronousMode()) {
        rpcProxy.setAcl(null, req);
        setAsyncReturnValue();
      } else {
        rpcProxy.setAcl(null, req);
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    GetAclStatusRequestProto req = GetAclStatusRequestProto.newBuilder()
        .setSrc(src).build();
    try {
      if (Client.isAsynchronousMode()) {
        rpcProxy.getAclStatus(null, req);
        final AsyncGet<Message, Exception> asyncReturnMessage
            = ProtobufRpcEngine.getAsyncReturnMessage();
        final AsyncGet<AclStatus, Exception> asyncGet
            = new AsyncGet<AclStatus, Exception>() {
          @Override
          public AclStatus get(long timeout, TimeUnit unit) throws Exception {
            return PBHelperClient.convert((GetAclStatusResponseProto)
                asyncReturnMessage.get(timeout, unit));
          }

          @Override
          public boolean isDone() {
            return asyncReturnMessage.isDone();
          }
        };
        AsyncCallHandler.setLowerLayerAsyncReturn(asyncGet);
        return null;
      } else {
        return PBHelperClient.convert(rpcProxy.getAclStatus(null, req));
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    final CreateEncryptionZoneRequestProto.Builder builder =
        CreateEncryptionZoneRequestProto.newBuilder();
    builder.setSrc(src);
    if (keyName != null && !keyName.isEmpty()) {
      builder.setKeyName(keyName);
    }
    CreateEncryptionZoneRequestProto req = builder.build();
    try {
      rpcProxy.createEncryptionZone(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public EncryptionZone getEZForPath(String src) throws IOException {
    final GetEZForPathRequestProto.Builder builder =
        GetEZForPathRequestProto.newBuilder();
    builder.setSrc(src);
    final GetEZForPathRequestProto req = builder.build();
    try {
      final EncryptionZonesProtos.GetEZForPathResponseProto response =
          rpcProxy.getEZForPath(null, req);
      if (response.hasZone()) {
        return PBHelperClient.convert(response.getZone());
      } else {
        return null;
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public BatchedEntries<EncryptionZone> listEncryptionZones(long id)
      throws IOException {
    final ListEncryptionZonesRequestProto req =
        ListEncryptionZonesRequestProto.newBuilder()
            .setId(id)
            .build();
    try {
      EncryptionZonesProtos.ListEncryptionZonesResponseProto response =
          rpcProxy.listEncryptionZones(null, req);
      List<EncryptionZone> elements =
          Lists.newArrayListWithCapacity(response.getZonesCount());
      for (EncryptionZoneProto p : response.getZonesList()) {
        elements.add(PBHelperClient.convert(p));
      }
      return new BatchedListEntries<>(elements, response.getHasMore());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setErasureCodingPolicy(String src, ErasureCodingPolicy ecPolicy)
      throws IOException {
    final SetErasureCodingPolicyRequestProto.Builder builder =
        SetErasureCodingPolicyRequestProto.newBuilder();
    builder.setSrc(src);
    if (ecPolicy != null) {
      builder.setEcPolicy(PBHelperClient.convertErasureCodingPolicy(ecPolicy));
    }
    SetErasureCodingPolicyRequestProto req = builder.build();
    try {
      rpcProxy.setErasureCodingPolicy(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    SetXAttrRequestProto req = SetXAttrRequestProto.newBuilder()
        .setSrc(src)
        .setXAttr(PBHelperClient.convertXAttrProto(xAttr))
        .setFlag(PBHelperClient.convert(flag))
        .build();
    try {
      rpcProxy.setXAttr(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
      throws IOException {
    GetXAttrsRequestProto.Builder builder = GetXAttrsRequestProto.newBuilder();
    builder.setSrc(src);
    if (xAttrs != null) {
      builder.addAllXAttrs(PBHelperClient.convertXAttrProto(xAttrs));
    }
    GetXAttrsRequestProto req = builder.build();
    try {
      return PBHelperClient.convert(rpcProxy.getXAttrs(null, req));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    ListXAttrsRequestProto.Builder builder =
        ListXAttrsRequestProto.newBuilder();
    builder.setSrc(src);
    ListXAttrsRequestProto req = builder.build();
    try {
      return PBHelperClient.convert(rpcProxy.listXAttrs(null, req));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    RemoveXAttrRequestProto req = RemoveXAttrRequestProto
        .newBuilder().setSrc(src)
        .setXAttr(PBHelperClient.convertXAttrProto(xAttr)).build();
    try {
      rpcProxy.removeXAttr(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {
    CheckAccessRequestProto req = CheckAccessRequestProto.newBuilder()
        .setPath(path).setMode(PBHelperClient.convert(mode)).build();
    try {
      rpcProxy.checkAccess(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    SetStoragePolicyRequestProto req = SetStoragePolicyRequestProto
        .newBuilder().setSrc(src).setPolicyName(policyName).build();
    try {
      rpcProxy.setStoragePolicy(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void unsetStoragePolicy(String src) throws IOException {
    UnsetStoragePolicyRequestProto req = UnsetStoragePolicyRequestProto
        .newBuilder().setSrc(src).build();
    try {
      rpcProxy.unsetStoragePolicy(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    GetStoragePolicyRequestProto request = GetStoragePolicyRequestProto
        .newBuilder().setPath(path).build();
    try {
      return PBHelperClient.convert(rpcProxy.getStoragePolicy(null, request)
          .getStoragePolicy());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    try {
      GetStoragePoliciesResponseProto response = rpcProxy
          .getStoragePolicies(null, VOID_GET_STORAGE_POLICIES_REQUEST);
      return PBHelperClient.convertStoragePolicies(response.getPoliciesList());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  public long getCurrentEditLogTxid() throws IOException {
    GetCurrentEditLogTxidRequestProto req = GetCurrentEditLogTxidRequestProto
        .getDefaultInstance();
    try {
      return rpcProxy.getCurrentEditLogTxid(null, req).getTxid();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    GetEditsFromTxidRequestProto req = GetEditsFromTxidRequestProto.newBuilder()
        .setTxid(txid).build();
    try {
      return PBHelperClient.convert(rpcProxy.getEditsFromTxid(null, req));
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ErasureCodingPolicy[] getErasureCodingPolicies() throws IOException {
    try {
      GetErasureCodingPoliciesResponseProto response = rpcProxy
          .getErasureCodingPolicies(null, VOID_GET_EC_POLICIES_REQUEST);
      ErasureCodingPolicy[] ecPolicies =
          new ErasureCodingPolicy[response.getEcPoliciesCount()];
      int i = 0;
      for (ErasureCodingPolicyProto ecPolicyProto :
          response.getEcPoliciesList()) {
        ecPolicies[i++] =
            PBHelperClient.convertErasureCodingPolicy(ecPolicyProto);
      }
      return ecPolicies;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    GetErasureCodingPolicyRequestProto req =
        GetErasureCodingPolicyRequestProto.newBuilder().setSrc(src).build();
    try {
      GetErasureCodingPolicyResponseProto response =
          rpcProxy.getErasureCodingPolicy(null, req);
      if (response.hasEcPolicy()) {
        return PBHelperClient.convertErasureCodingPolicy(
            response.getEcPolicy());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    GetQuotaUsageRequestProto req =
        GetQuotaUsageRequestProto.newBuilder().setPath(path).build();
    try {
      return PBHelperClient.convert(rpcProxy.getQuotaUsage(null, req)
          .getUsage());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
}
