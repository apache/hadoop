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
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
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
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBatchedListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBatchedListingResponseProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLocatedFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLocatedFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetQuotaUsageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSlowDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.HAServiceStateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListOpenFilesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListOpenFilesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.OpenFilesBatchResponseProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpgradeStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpgradeStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SatisfyStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.*;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.EncryptionZoneProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListReencryptionStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListReencryptionStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ZoneReencryptionStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ReencryptEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.AddErasureCodingPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.AddErasureCodingPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.RemoveErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.EnableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.DisableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingCodecsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingCodecsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.SetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.UnsetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.CodecProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BatchedDirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetECTopologyResultForPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto;
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
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.concurrent.AsyncGet;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.getRemoteException;
import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

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

  private final static GetFsReplicatedBlockStatsRequestProto
      VOID_GET_FS_REPLICATED_BLOCK_STATS_REQUEST =
      GetFsReplicatedBlockStatsRequestProto.newBuilder().build();

  private final static GetFsECBlockGroupStatsRequestProto
      VOID_GET_FS_ECBLOCKGROUP_STATS_REQUEST =
      GetFsECBlockGroupStatsRequestProto.newBuilder().build();

  private final static RollEditsRequestProto VOID_ROLLEDITS_REQUEST =
      RollEditsRequestProto.getDefaultInstance();

  private final static RefreshNodesRequestProto VOID_REFRESH_NODES_REQUEST =
      RefreshNodesRequestProto.newBuilder().build();

  private final static FinalizeUpgradeRequestProto
      VOID_FINALIZE_UPGRADE_REQUEST =
      FinalizeUpgradeRequestProto.newBuilder().build();

  private final static UpgradeStatusRequestProto
      VOID_UPGRADE_STATUS_REQUEST =
      UpgradeStatusRequestProto.newBuilder().build();

  private final static GetDataEncryptionKeyRequestProto
      VOID_GET_DATA_ENCRYPTIONKEY_REQUEST =
      GetDataEncryptionKeyRequestProto.newBuilder().build();

  private final static GetStoragePoliciesRequestProto
      VOID_GET_STORAGE_POLICIES_REQUEST =
      GetStoragePoliciesRequestProto.newBuilder().build();

  private final static GetErasureCodingPoliciesRequestProto
      VOID_GET_EC_POLICIES_REQUEST = GetErasureCodingPoliciesRequestProto
      .newBuilder().build();

  private final static GetErasureCodingCodecsRequestProto
      VOID_GET_EC_CODEC_REQUEST = GetErasureCodingCodecsRequestProto
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
    GetBlockLocationsResponseProto resp = ipc(() -> rpcProxy.getBlockLocations(null,
        req));
    return resp.hasLocations() ?
        PBHelperClient.convert(resp.getLocations()) : null;
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    GetServerDefaultsRequestProto req = VOID_GET_SERVER_DEFAULT_REQUEST;
    return PBHelperClient
        .convert(ipc(() -> rpcProxy.getServerDefaults(null, req).getServerDefaults()));
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication, long blockSize,
      CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
      String storagePolicy)
      throws IOException {
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
    CreateResponseProto res = ipc(() -> rpcProxy.create(null, req));
    return res.hasFs() ? PBHelperClient.convert(res.getFs()) : null;
  }

  @Override
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    TruncateRequestProto req = TruncateRequestProto.newBuilder()
        .setSrc(src)
        .setNewLength(newLength)
        .setClientName(clientName)
        .build();
    return ipc(() -> rpcProxy.truncate(null, req).getResult());
  }

  @Override
  public LastBlockWithStatus append(String src, String clientName,
      EnumSetWritable<CreateFlag> flag) throws IOException {
    AppendRequestProto req = AppendRequestProto.newBuilder().setSrc(src)
        .setClientName(clientName).setFlag(
            PBHelperClient.convertCreateFlag(flag))
        .build();
    AppendResponseProto res = ipc(() -> rpcProxy.append(null, req));
    LocatedBlock lastBlock = res.hasBlock() ? PBHelperClient
        .convertLocatedBlockProto(res.getBlock()) : null;
    HdfsFileStatus stat = (res.hasStat()) ?
        PBHelperClient.convert(res.getStat()) : null;
    return new LastBlockWithStatus(lastBlock, stat);
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {
    SetReplicationRequestProto req = SetReplicationRequestProto.newBuilder()
        .setSrc(src)
        .setReplication(replication)
        .build();
    return ipc(() -> rpcProxy.setReplication(null, req)).getResult();
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws IOException {
    SetPermissionRequestProto req = SetPermissionRequestProto.newBuilder()
        .setSrc(src)
        .setPermission(PBHelperClient.convert(permission))
        .build();
    if (Client.isAsynchronousMode()) {
      ipc(() -> rpcProxy.setPermission(null, req));
      setAsyncReturnValue();
    } else {
      ipc(() -> rpcProxy.setPermission(null, req));
    }
  }

  private void setAsyncReturnValue() {
    final AsyncGet<Message, Exception> asyncReturnMessage
        = ProtobufRpcEngine2.getAsyncReturnMessage();
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
    if (Client.isAsynchronousMode()) {
      ipc(() -> rpcProxy.setOwner(null, req.build()));
      setAsyncReturnValue();
    } else {
      ipc(() -> rpcProxy.setOwner(null, req.build()));
    }
  }

  @Override
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
      String holder) throws IOException {
    AbandonBlockRequestProto req = AbandonBlockRequestProto.newBuilder()
        .setB(PBHelperClient.convert(b)).setSrc(src).setHolder(holder)
        .setFileId(fileId).build();
    ipc(() -> rpcProxy.abandonBlock(null, req));
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
    return PBHelperClient.convertLocatedBlockProto(
        ipc(() -> rpcProxy.addBlock(null, req.build())).getBlock());
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
    return PBHelperClient.convertLocatedBlockProto(
        ipc(() -> rpcProxy.getAdditionalDatanode(null, req)).getBlock());
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
    return ipc(() -> rpcProxy.complete(null, req.build())).getResult();
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ReportBadBlocksRequestProto req = ReportBadBlocksRequestProto.newBuilder()
        .addAllBlocks(Arrays.asList(
            PBHelperClient.convertLocatedBlocks(blocks)))
        .build();
    ipc(() -> rpcProxy.reportBadBlocks(null, req));
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    RenameRequestProto req = RenameRequestProto.newBuilder()
        .setSrc(src)
        .setDst(dst).build();

    return ipc(() -> rpcProxy.rename(null, req)).getResult();
  }


  @Override
  public void rename2(String src, String dst, Rename... options)
      throws IOException {
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
    if (Client.isAsynchronousMode()) {
      ipc(() -> rpcProxy.rename2(null, req));
      setAsyncReturnValue();
    } else {
      ipc(() -> rpcProxy.rename2(null, req));
    }
  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException {
    ConcatRequestProto req = ConcatRequestProto.newBuilder().
        setTrg(trg).
        addAllSrcs(Arrays.asList(srcs)).build();
    ipc(() -> rpcProxy.concat(null, req));
  }


  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    DeleteRequestProto req = DeleteRequestProto.newBuilder().setSrc(src)
        .setRecursive(recursive).build();
    return ipc(() -> rpcProxy.delete(null, req).getResult());
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    MkdirsRequestProto.Builder builder = MkdirsRequestProto.newBuilder()
        .setSrc(src)
        .setMasked(PBHelperClient.convert(masked))
        .setCreateParent(createParent);
    FsPermission unmasked = masked.getUnmasked();
    if (unmasked != null) {
      builder.setUnmasked(PBHelperClient.convert(unmasked));
    }
    MkdirsRequestProto req = builder.build();
    return ipc(() -> rpcProxy.mkdirs(null, req)).getResult();
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    GetListingRequestProto req = GetListingRequestProto.newBuilder()
        .setSrc(src)
        .setStartAfter(ByteString.copyFrom(startAfter))
        .setNeedLocation(needLocation).build();
    GetListingResponseProto result = ipc(() -> rpcProxy.getListing(null, req));
    if (result.hasDirList()) {
      return PBHelperClient.convert(result.getDirList());
    }
    return null;
  }

  @Override
  public BatchedDirectoryListing getBatchedListing(
      String[] srcs, byte[] startAfter, boolean needLocation)
      throws IOException {
    GetBatchedListingRequestProto req = GetBatchedListingRequestProto
        .newBuilder()
        .addAllPaths(Arrays.asList(srcs))
        .setStartAfter(ByteString.copyFrom(startAfter))
        .setNeedLocation(needLocation).build();
    GetBatchedListingResponseProto result =
        ipc(() -> rpcProxy.getBatchedListing(null, req));

    if (result.getListingsCount() > 0) {
      HdfsPartialListing[] listingArray =
          new HdfsPartialListing[result.getListingsCount()];
      int listingIdx = 0;
      for (BatchedDirectoryListingProto proto : result.getListingsList()) {
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
          new BatchedDirectoryListing(listingArray, result.getHasMore(),
              result.getStartAfter().toByteArray());
      return batchedListing;
    }
    return null;
  }


  @Override
  public void renewLease(String clientName, List<String> namespaces)
      throws IOException {
    RenewLeaseRequestProto.Builder builder = RenewLeaseRequestProto
        .newBuilder().setClientName(clientName);
    if (namespaces != null && !namespaces.isEmpty()) {
      builder.addAllNamespaces(namespaces);
    }
    ipc(() -> rpcProxy.renewLease(null, builder.build()));
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    RecoverLeaseRequestProto req = RecoverLeaseRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName).build();
    return ipc(() -> rpcProxy.recoverLease(null, req)).getResult();
  }

  @Override
  public long[] getStats() throws IOException {
    return PBHelperClient.convert(ipc(() -> rpcProxy.getFsStats(null,
        VOID_GET_FSSTATUS_REQUEST)));
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    return PBHelperClient.convert(ipc(() -> rpcProxy.getFsReplicatedBlockStats(null,
        VOID_GET_FS_REPLICATED_BLOCK_STATS_REQUEST)));
  }

  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    return PBHelperClient.convert(ipc(() -> rpcProxy.getFsECBlockGroupStats(null,
        VOID_GET_FS_ECBLOCKGROUP_STATS_REQUEST)));
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    GetDatanodeReportRequestProto req = GetDatanodeReportRequestProto
        .newBuilder()
        .setType(PBHelperClient.convert(type)).build();
    return PBHelperClient.convert(
        ipc(() -> rpcProxy.getDatanodeReport(null, req)).getDiList());
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    final GetDatanodeStorageReportRequestProto req
        = GetDatanodeStorageReportRequestProto.newBuilder()
        .setType(PBHelperClient.convert(type)).build();
    return PBHelperClient.convertDatanodeStorageReports(
        ipc(() -> rpcProxy.getDatanodeStorageReport(null, req)
            .getDatanodeStorageReportsList()));
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException {
    GetPreferredBlockSizeRequestProto req = GetPreferredBlockSizeRequestProto
        .newBuilder()
        .setFilename(filename)
        .build();
    return ipc(() -> rpcProxy.getPreferredBlockSize(null, req)).getBsize();
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    SetSafeModeRequestProto req = SetSafeModeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action))
        .setChecked(isChecked).build();
    return ipc(() -> rpcProxy.setSafeMode(null, req)).getResult();
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    SaveNamespaceRequestProto req = SaveNamespaceRequestProto.newBuilder()
        .setTimeWindow(timeWindow).setTxGap(txGap).build();
    return ipc(() -> rpcProxy.saveNamespace(null, req)).getSaved();
  }

  @Override
  public long rollEdits() throws IOException {
    RollEditsResponseProto resp = ipc(() -> rpcProxy.rollEdits(null,
        VOID_ROLLEDITS_REQUEST));
    return resp.getNewSegmentTxId();
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException{
    RestoreFailedStorageRequestProto req = RestoreFailedStorageRequestProto
        .newBuilder()
        .setArg(arg).build();
    return ipc(() -> rpcProxy.restoreFailedStorage(null, req)).getResult();
  }

  @Override
  public void refreshNodes() throws IOException {
    ipc(() -> rpcProxy.refreshNodes(null, VOID_REFRESH_NODES_REQUEST));
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    ipc(() -> rpcProxy.finalizeUpgrade(null, VOID_FINALIZE_UPGRADE_REQUEST));
  }

  @Override
  public boolean upgradeStatus() throws IOException {
    final UpgradeStatusResponseProto proto = ipc(() -> rpcProxy.upgradeStatus(
        null, VOID_UPGRADE_STATUS_REQUEST));
    return proto.getUpgradeFinalized();
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    final RollingUpgradeRequestProto r = RollingUpgradeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action)).build();
    final RollingUpgradeResponseProto proto =
        ipc(() -> rpcProxy.rollingUpgrade(null, r));
    if (proto.hasRollingUpgradeInfo()) {
      return PBHelperClient.convert(proto.getRollingUpgradeInfo());
    }
    return null;
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    ListCorruptFileBlocksRequestProto.Builder req =
        ListCorruptFileBlocksRequestProto.newBuilder().setPath(path);
    if (cookie != null)
      req.setCookie(cookie);
    return PBHelperClient.convert(
        ipc(() -> rpcProxy.listCorruptFileBlocks(null, req.build())).getCorrupt());
  }

  @Override
  public void metaSave(String filename) throws IOException {
    MetaSaveRequestProto req = MetaSaveRequestProto.newBuilder()
        .setFilename(filename).build();
    ipc(() -> rpcProxy.metaSave(null, req));
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    GetFileInfoRequestProto req = GetFileInfoRequestProto.newBuilder()
        .setSrc(src)
        .build();
    GetFileInfoResponseProto res = ipc(() -> rpcProxy.getFileInfo(null, req));
    return res.hasFs() ? PBHelperClient.convert(res.getFs()) : null;
  }

  @Override
  public HdfsLocatedFileStatus getLocatedFileInfo(String src,
      boolean needBlockToken) throws IOException {
    GetLocatedFileInfoRequestProto req =
        GetLocatedFileInfoRequestProto.newBuilder()
            .setSrc(src)
            .setNeedBlockToken(needBlockToken)
            .build();
    GetLocatedFileInfoResponseProto res =
        ipc(() -> rpcProxy.getLocatedFileInfo(null, req));
    return (HdfsLocatedFileStatus) (res.hasFs()
        ? PBHelperClient.convert(res.getFs())
        : null);
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    GetFileLinkInfoRequestProto req = GetFileLinkInfoRequestProto.newBuilder()
        .setSrc(src).build();
    GetFileLinkInfoResponseProto result = ipc(() -> rpcProxy.getFileLinkInfo(null, req));
    return result.hasFs() ? PBHelperClient.convert(result.getFs()) : null;
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    GetContentSummaryRequestProto req = GetContentSummaryRequestProto
        .newBuilder()
        .setPath(path)
        .build();
    return PBHelperClient.convert(ipc(() -> rpcProxy.getContentSummary(null, req))
        .getSummary());
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
    ipc(() -> rpcProxy.setQuota(null, req));
  }

  @Override
  public void fsync(String src, long fileId, String client,
      long lastBlockLength) throws IOException {
    FsyncRequestProto req = FsyncRequestProto.newBuilder().setSrc(src)
        .setClient(client).setLastBlockLength(lastBlockLength)
        .setFileId(fileId).build();
    ipc(() -> rpcProxy.fsync(null, req));
  }

  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    SetTimesRequestProto req = SetTimesRequestProto.newBuilder()
        .setSrc(src)
        .setMtime(mtime)
        .setAtime(atime)
        .build();
    ipc(() -> rpcProxy.setTimes(null, req));
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
    ipc(() -> rpcProxy.createSymlink(null, req));
  }

  @Override
  public String getLinkTarget(String path) throws IOException {
    GetLinkTargetRequestProto req = GetLinkTargetRequestProto.newBuilder()
        .setPath(path).build();
    GetLinkTargetResponseProto rsp = ipc(() -> rpcProxy.getLinkTarget(null, req));
    return rsp.hasTargetPath() ? rsp.getTargetPath() : null;
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException {
    UpdateBlockForPipelineRequestProto req = UpdateBlockForPipelineRequestProto
        .newBuilder()
        .setBlock(PBHelperClient.convert(block))
        .setClientName(clientName)
        .build();
    return PBHelperClient.convertLocatedBlockProto(
        ipc(() -> rpcProxy.updateBlockForPipeline(null, req)).getBlock());
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
    ipc(() -> rpcProxy.updatePipeline(null, req));
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer == null ? "" : renewer.toString())
        .build();
    GetDelegationTokenResponseProto resp =
        ipc(() -> rpcProxy.getDelegationToken(null, req));
    return resp.hasToken() ?
        PBHelperClient.convertDelegationToken(resp.getToken()) : null;
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    RenewDelegationTokenRequestProto req =
        RenewDelegationTokenRequestProto.newBuilder().
            setToken(PBHelperClient.convert(token)).
            build();
    return ipc(() -> rpcProxy.renewDelegationToken(null, req)).getNewExpiryTime();
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto
        .newBuilder()
        .setToken(PBHelperClient.convert(token))
        .build();
    ipc(() -> rpcProxy.cancelDelegationToken(null, req));
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    SetBalancerBandwidthRequestProto req =
        SetBalancerBandwidthRequestProto.newBuilder()
            .setBandwidth(bandwidth)
            .build();
    ipc(() -> rpcProxy.setBalancerBandwidth(null, req));
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        ClientNamenodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(ClientNamenodeProtocolPB.class), methodName);
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    GetDataEncryptionKeyResponseProto rsp = ipc(() -> rpcProxy.getDataEncryptionKey(
        null, VOID_GET_DATA_ENCRYPTIONKEY_REQUEST));
    return rsp.hasDataEncryptionKey() ?
        PBHelperClient.convert(rsp.getDataEncryptionKey()) : null;
  }


  @Override
  public boolean isFileClosed(String src) throws IOException {
    IsFileClosedRequestProto req = IsFileClosedRequestProto.newBuilder()
        .setSrc(src).build();
    return ipc(() -> rpcProxy.isFileClosed(null, req)).getResult();
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
    return ipc(() -> rpcProxy.createSnapshot(null, req)).getSnapshotPath();
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    DeleteSnapshotRequestProto req = DeleteSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotName(snapshotName).build();
    ipc(() -> rpcProxy.deleteSnapshot(null, req));
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    AllowSnapshotRequestProto req = AllowSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).build();
    ipc(() -> rpcProxy.allowSnapshot(null, req));
  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    DisallowSnapshotRequestProto req = DisallowSnapshotRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot).build();
    ipc(() -> rpcProxy.disallowSnapshot(null, req));
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
      String snapshotNewName) throws IOException {
    RenameSnapshotRequestProto req = RenameSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotOldName(snapshotOldName)
        .setSnapshotNewName(snapshotNewName).build();
    ipc(() -> rpcProxy.renameSnapshot(null, req));
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    GetSnapshottableDirListingRequestProto req =
        GetSnapshottableDirListingRequestProto.newBuilder().build();
    GetSnapshottableDirListingResponseProto result = ipc(() -> rpcProxy
        .getSnapshottableDirListing(null, req));

    if (result.hasSnapshottableDirList()) {
      return PBHelperClient.convert(result.getSnapshottableDirList());
    }
    return null;
  }

  @Override
  public SnapshotStatus[] getSnapshotListing(String path)
      throws IOException {
    GetSnapshotListingRequestProto req =
        GetSnapshotListingRequestProto.newBuilder()
            .setSnapshotRoot(path).build();
    GetSnapshotListingResponseProto result = ipc(() -> rpcProxy
        .getSnapshotListing(null, req));

    if (result.hasSnapshotList()) {
      return PBHelperClient.convert(result.getSnapshotList());
    }
    return null;
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String fromSnapshot, String toSnapshot) throws IOException {
    GetSnapshotDiffReportRequestProto req = GetSnapshotDiffReportRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot)
        .setFromSnapshot(fromSnapshot).setToSnapshot(toSnapshot).build();
    GetSnapshotDiffReportResponseProto result =
        ipc(() -> rpcProxy.getSnapshotDiffReport(null, req));

    return PBHelperClient.convert(result.getDiffReport());
  }

  @Override
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String fromSnapshot, String toSnapshot,
      byte[] startPath, int index) throws IOException {
    GetSnapshotDiffReportListingRequestProto req =
        GetSnapshotDiffReportListingRequestProto.newBuilder()
            .setSnapshotRoot(snapshotRoot).setFromSnapshot(fromSnapshot)
            .setToSnapshot(toSnapshot).setCursor(
            HdfsProtos.SnapshotDiffReportCursorProto.newBuilder()
                .setStartPath(PBHelperClient.getByteString(startPath))
                .setIndex(index).build()).build();
    GetSnapshotDiffReportListingResponseProto result =
        ipc(() -> rpcProxy.getSnapshotDiffReportListing(null, req));

    return PBHelperClient.convert(result.getDiffReport());
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    AddCacheDirectiveRequestProto.Builder builder =
        AddCacheDirectiveRequestProto.newBuilder().
            setInfo(PBHelperClient.convert(directive));
    if (!flags.isEmpty()) {
      builder.setCacheFlags(PBHelperClient.convertCacheFlags(flags));
    }
    return ipc(() -> rpcProxy.addCacheDirective(null, builder.build())).getId();
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    ModifyCacheDirectiveRequestProto.Builder builder =
        ModifyCacheDirectiveRequestProto.newBuilder().
            setInfo(PBHelperClient.convert(directive));
    if (!flags.isEmpty()) {
      builder.setCacheFlags(PBHelperClient.convertCacheFlags(flags));
    }
    ipc(() -> rpcProxy.modifyCacheDirective(null, builder.build()));
  }

  @Override
  public void removeCacheDirective(long id)
      throws IOException {
    ipc(() -> rpcProxy.removeCacheDirective(null,
        RemoveCacheDirectiveRequestProto.newBuilder().
            setId(id).build()));
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
    CacheDirectiveInfo f = filter;
    return new BatchedCacheEntries(
        ipc(() -> rpcProxy.listCacheDirectives(null,
            ListCacheDirectivesRequestProto.newBuilder().
                setPrevId(prevId).
                setFilter(PBHelperClient.convert(f)).
                build())));
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    AddCachePoolRequestProto.Builder builder =
        AddCachePoolRequestProto.newBuilder();
    builder.setInfo(PBHelperClient.convert(info));
    ipc(() -> rpcProxy.addCachePool(null, builder.build()));
  }

  @Override
  public void modifyCachePool(CachePoolInfo req) throws IOException {
    ModifyCachePoolRequestProto.Builder builder =
        ModifyCachePoolRequestProto.newBuilder();
    builder.setInfo(PBHelperClient.convert(req));
    ipc(() -> rpcProxy.modifyCachePool(null, builder.build()));
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    ipc(() -> rpcProxy.removeCachePool(null,
        RemoveCachePoolRequestProto.newBuilder().
            setPoolName(cachePoolName).build()));
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
    return new BatchedCachePoolEntries(
        ipc(() -> rpcProxy.listCachePools(null,
            ListCachePoolsRequestProto.newBuilder().
                setPrevPoolName(prevKey).build())));
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    ModifyAclEntriesRequestProto req = ModifyAclEntriesRequestProto
        .newBuilder().setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec)).build();
    ipc(() -> rpcProxy.modifyAclEntries(null, req));
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    RemoveAclEntriesRequestProto req = RemoveAclEntriesRequestProto
        .newBuilder().setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec)).build();
    ipc(() -> rpcProxy.removeAclEntries(null, req));
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    RemoveDefaultAclRequestProto req = RemoveDefaultAclRequestProto
        .newBuilder().setSrc(src).build();
    ipc(() -> rpcProxy.removeDefaultAcl(null, req));
  }

  @Override
  public void removeAcl(String src) throws IOException {
    RemoveAclRequestProto req = RemoveAclRequestProto.newBuilder()
        .setSrc(src).build();
    ipc(() -> rpcProxy.removeAcl(null, req));
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    SetAclRequestProto req = SetAclRequestProto.newBuilder()
        .setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec))
        .build();
    if (Client.isAsynchronousMode()) {
      ipc(() -> rpcProxy.setAcl(null, req));
      setAsyncReturnValue();
    } else {
      ipc(() -> rpcProxy.setAcl(null, req));
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
            = ProtobufRpcEngine2.getAsyncReturnMessage();
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
      throw getRemoteException(e);
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
    ipc(() -> rpcProxy.createEncryptionZone(null, req));
  }

  @Override
  public EncryptionZone getEZForPath(String src) throws IOException {
    final GetEZForPathRequestProto.Builder builder =
        GetEZForPathRequestProto.newBuilder();
    builder.setSrc(src);
    final GetEZForPathRequestProto req = builder.build();
    final EncryptionZonesProtos.GetEZForPathResponseProto response =
        ipc(() -> rpcProxy.getEZForPath(null, req));
    if (response.hasZone()) {
      return PBHelperClient.convert(response.getZone());
    } else {
      return null;
    }
  }

  @Override
  public BatchedEntries<EncryptionZone> listEncryptionZones(long id)
      throws IOException {
    final ListEncryptionZonesRequestProto req =
        ListEncryptionZonesRequestProto.newBuilder()
            .setId(id)
            .build();
    EncryptionZonesProtos.ListEncryptionZonesResponseProto response =
        ipc(() -> rpcProxy.listEncryptionZones(null, req));
    List<EncryptionZone> elements =
        Lists.newArrayListWithCapacity(response.getZonesCount());
    for (EncryptionZoneProto p : response.getZonesList()) {
      elements.add(PBHelperClient.convert(p));
    }
    return new BatchedListEntries<>(elements, response.getHasMore());
  }

  @Override
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    final SetErasureCodingPolicyRequestProto.Builder builder =
        SetErasureCodingPolicyRequestProto.newBuilder();
    builder.setSrc(src);
    if (ecPolicyName != null) {
      builder.setEcPolicyName(ecPolicyName);
    }
    SetErasureCodingPolicyRequestProto req = builder.build();
    ipc(() -> rpcProxy.setErasureCodingPolicy(null, req));
  }

  @Override
  public void unsetErasureCodingPolicy(String src) throws IOException {
    final UnsetErasureCodingPolicyRequestProto.Builder builder =
        UnsetErasureCodingPolicyRequestProto.newBuilder();
    builder.setSrc(src);
    UnsetErasureCodingPolicyRequestProto req = builder.build();
    ipc(() -> rpcProxy.unsetErasureCodingPolicy(null, req));
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      final String... policyNames) throws IOException {
    final GetECTopologyResultForPoliciesRequestProto.Builder builder =
        GetECTopologyResultForPoliciesRequestProto.newBuilder();
    builder.addAllPolicies(Arrays.asList(policyNames));
    GetECTopologyResultForPoliciesRequestProto req = builder.build();
    GetECTopologyResultForPoliciesResponseProto response =
        ipc(() -> rpcProxy.getECTopologyResultForPolicies(null, req));
    return PBHelperClient
        .convertECTopologyVerifierResultProto(response.getResponse());
  }

  @Override
  public void reencryptEncryptionZone(String zone, ReencryptAction action)
      throws IOException {
    final ReencryptEncryptionZoneRequestProto.Builder builder =
        ReencryptEncryptionZoneRequestProto.newBuilder();
    builder.setZone(zone).setAction(PBHelperClient.convert(action));
    ReencryptEncryptionZoneRequestProto req = builder.build();
    ipc(() -> rpcProxy.reencryptEncryptionZone(null, req));
  }

  @Override
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(long id)
      throws IOException {
    final ListReencryptionStatusRequestProto req =
        ListReencryptionStatusRequestProto.newBuilder().setId(id).build();
    ListReencryptionStatusResponseProto response =
        ipc(() -> rpcProxy.listReencryptionStatus(null, req));
    List<ZoneReencryptionStatus> elements =
        Lists.newArrayListWithCapacity(response.getStatusesCount());
    for (ZoneReencryptionStatusProto p : response.getStatusesList()) {
      elements.add(PBHelperClient.convert(p));
    }
    return new BatchedListEntries<>(elements, response.getHasMore());
  }

  @Override
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    SetXAttrRequestProto req = SetXAttrRequestProto.newBuilder()
        .setSrc(src)
        .setXAttr(PBHelperClient.convertXAttrProto(xAttr))
        .setFlag(PBHelperClient.convert(flag))
        .build();
    ipc(() -> rpcProxy.setXAttr(null, req));
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
    return PBHelperClient.convert(ipc(() -> rpcProxy.getXAttrs(null, req)));
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    ListXAttrsRequestProto.Builder builder =
        ListXAttrsRequestProto.newBuilder();
    builder.setSrc(src);
    ListXAttrsRequestProto req = builder.build();
    return PBHelperClient.convert(ipc(() -> rpcProxy.listXAttrs(null, req)));
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    RemoveXAttrRequestProto req = RemoveXAttrRequestProto
        .newBuilder().setSrc(src)
        .setXAttr(PBHelperClient.convertXAttrProto(xAttr)).build();
    ipc(() -> rpcProxy.removeXAttr(null, req));
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {
    CheckAccessRequestProto req = CheckAccessRequestProto.newBuilder()
        .setPath(path).setMode(PBHelperClient.convert(mode)).build();
    ipc(() -> rpcProxy.checkAccess(null, req));
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    SetStoragePolicyRequestProto req = SetStoragePolicyRequestProto
        .newBuilder().setSrc(src).setPolicyName(policyName).build();
    ipc(() -> rpcProxy.setStoragePolicy(null, req));
  }

  @Override
  public void unsetStoragePolicy(String src) throws IOException {
    UnsetStoragePolicyRequestProto req = UnsetStoragePolicyRequestProto
        .newBuilder().setSrc(src).build();
    ipc(() -> rpcProxy.unsetStoragePolicy(null, req));
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    GetStoragePolicyRequestProto request = GetStoragePolicyRequestProto
        .newBuilder().setPath(path).build();
    return PBHelperClient.convert(ipc(() -> rpcProxy.getStoragePolicy(null, request))
        .getStoragePolicy());
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    GetStoragePoliciesResponseProto response = ipc(() -> rpcProxy
        .getStoragePolicies(null, VOID_GET_STORAGE_POLICIES_REQUEST));
    return PBHelperClient.convertStoragePolicies(response.getPoliciesList());
  }

  public long getCurrentEditLogTxid() throws IOException {
    GetCurrentEditLogTxidRequestProto req = GetCurrentEditLogTxidRequestProto
        .getDefaultInstance();
    return ipc(() -> rpcProxy.getCurrentEditLogTxid(null, req)).getTxid();
  }

  @Override
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    GetEditsFromTxidRequestProto req = GetEditsFromTxidRequestProto.newBuilder()
        .setTxid(txid).build();
    return PBHelperClient.convert(ipc(() -> rpcProxy.getEditsFromTxid(null, req)));
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    List<ErasureCodingPolicyProto> protos = Arrays.stream(policies)
        .map(PBHelperClient::convertErasureCodingPolicy)
        .collect(Collectors.toList());
    AddErasureCodingPoliciesRequestProto req =
        AddErasureCodingPoliciesRequestProto.newBuilder()
        .addAllEcPolicies(protos).build();
    AddErasureCodingPoliciesResponseProto rep = ipc(() -> rpcProxy
        .addErasureCodingPolicies(null, req));
    AddErasureCodingPolicyResponse[] responses =
        rep.getResponsesList().stream()
            .map(PBHelperClient::convertAddErasureCodingPolicyResponse)
            .toArray(AddErasureCodingPolicyResponse[]::new);
    return responses;
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    RemoveErasureCodingPolicyRequestProto.Builder builder =
        RemoveErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    RemoveErasureCodingPolicyRequestProto req = builder.build();
    ipc(() -> rpcProxy.removeErasureCodingPolicy(null, req));
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    EnableErasureCodingPolicyRequestProto.Builder builder =
        EnableErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    EnableErasureCodingPolicyRequestProto req = builder.build();
    ipc(() -> rpcProxy.enableErasureCodingPolicy(null, req));
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    DisableErasureCodingPolicyRequestProto.Builder builder =
        DisableErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    DisableErasureCodingPolicyRequestProto req = builder.build();
    ipc(() -> rpcProxy.disableErasureCodingPolicy(null, req));
  }

  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    GetErasureCodingPoliciesResponseProto response = ipc(() -> rpcProxy
        .getErasureCodingPolicies(null, VOID_GET_EC_POLICIES_REQUEST));
    ErasureCodingPolicyInfo[] ecPolicies =
        new ErasureCodingPolicyInfo[response.getEcPoliciesCount()];
    int i = 0;
    for (ErasureCodingPolicyProto proto : response.getEcPoliciesList()) {
      ecPolicies[i++] =
          PBHelperClient.convertErasureCodingPolicyInfo(proto);
    }
    return ecPolicies;
  }

  @Override
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    GetErasureCodingCodecsResponseProto response = ipc(() -> rpcProxy
        .getErasureCodingCodecs(null, VOID_GET_EC_CODEC_REQUEST));
    Map<String, String> ecCodecs = new HashMap<>();
    for (CodecProto codec : response.getCodecList()) {
      ecCodecs.put(codec.getCodec(), codec.getCoders());
    }
    return ecCodecs;
  }

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    GetErasureCodingPolicyRequestProto req =
        GetErasureCodingPolicyRequestProto.newBuilder().setSrc(src).build();
    GetErasureCodingPolicyResponseProto response =
        ipc(() -> rpcProxy.getErasureCodingPolicy(null, req));
    if (response.hasEcPolicy()) {
      return PBHelperClient.convertErasureCodingPolicy(
          response.getEcPolicy());
    }
    return null;
  }

  @Override
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    GetQuotaUsageRequestProto req =
        GetQuotaUsageRequestProto.newBuilder().setPath(path).build();
    return PBHelperClient.convert(ipc(() -> rpcProxy.getQuotaUsage(null, req))
        .getUsage());
  }

  @Deprecated
  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId)
      throws IOException {
    return listOpenFiles(prevId, EnumSet.of(OpenFilesType.ALL_OPEN_FILES),
        OpenFilesIterator.FILTER_PATH_DEFAULT);
  }

  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId,
      EnumSet<OpenFilesType> openFilesTypes, String path) throws IOException {
    ListOpenFilesRequestProto.Builder req =
        ListOpenFilesRequestProto.newBuilder().setId(prevId);
    if (openFilesTypes != null) {
      req.addAllTypes(PBHelperClient.convertOpenFileTypes(openFilesTypes));
    }
    req.setPath(path);

    ListOpenFilesResponseProto response =
        ipc(() -> rpcProxy.listOpenFiles(null, req.build()));
    List<OpenFileEntry> openFileEntries =
        Lists.newArrayListWithCapacity(response.getEntriesCount());
    for (OpenFilesBatchResponseProto p : response.getEntriesList()) {
      openFileEntries.add(PBHelperClient.convert(p));
    }
    return new BatchedListEntries<>(openFileEntries, response.getHasMore());
  }

  @Override
  public void msync() throws IOException {
    MsyncRequestProto.Builder req = MsyncRequestProto.newBuilder();
    ipc(() -> rpcProxy.msync(null, req.build()));
  }

  @Override
  public void satisfyStoragePolicy(String src) throws IOException {
    SatisfyStoragePolicyRequestProto req =
        SatisfyStoragePolicyRequestProto.newBuilder().setSrc(src).build();
    ipc(() -> rpcProxy.satisfyStoragePolicy(null, req));
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    GetSlowDatanodeReportRequestProto req =
        GetSlowDatanodeReportRequestProto.newBuilder().build();
    return PBHelperClient.convert(
        ipc(() -> rpcProxy.getSlowDatanodeReport(null, req)).getDatanodeInfoProtoList());
  }

  @Override
  public HAServiceProtocol.HAServiceState getHAServiceState()
      throws IOException {
    HAServiceStateRequestProto req =
        HAServiceStateRequestProto.newBuilder().build();
    HAServiceStateProto res =
        ipc(() -> rpcProxy.getHAServiceState(null, req)).getState();
    switch(res) {
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
  }

}
