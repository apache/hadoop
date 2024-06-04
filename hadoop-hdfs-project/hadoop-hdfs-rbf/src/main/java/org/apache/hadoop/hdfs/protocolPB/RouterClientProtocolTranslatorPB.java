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
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AllowSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AllowSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CheckAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CheckAccessResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBatchedListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBatchedListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEnclosingRootRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEnclosingRootResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLocatedFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLocatedFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetQuotaUsageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetQuotaUsageResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSlowDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSlowDatanodeReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.HAServiceStateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.HAServiceStateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListOpenFilesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListOpenFilesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MetaSaveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MsyncResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.OpenFilesBatchResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2RequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2ResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameSnapshotRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameSnapshotResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.TruncateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.TruncateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UnsetStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UnsetStoragePolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpgradeStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SatisfyStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SatisfyStoragePolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.EncryptionZoneProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListReencryptionStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListReencryptionStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ZoneReencryptionStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ReencryptEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.AddErasureCodingPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.AddErasureCodingPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.RemoveErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.EnableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.DisableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingCodecsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.SetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.UnsetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.CodecProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BatchedDirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetECTopologyResultForPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.proto.SecurityProtos;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.Response;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpc;
import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncResponse;
import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.getRemoteException;
import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;


/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to the
 * new PB types.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RouterClientProtocolTranslatorPB extends ClientNamenodeProtocolTranslatorPB {

  public RouterClientProtocolTranslatorPB(ClientNamenodeProtocolPB proxy) {
    super(proxy);
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

    AsyncGet<GetBlockLocationsResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.getBlockLocations(null, req));
    asyncResponse(() -> {
      GetBlockLocationsResponseProto resp = asyncGet.get(-1, null);
      return resp.hasLocations() ?
          PBHelperClient.convert(resp.getLocations()) : null;
    });
    return null;
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getServerDefaults();
    }
    GetServerDefaultsRequestProto req = VOID_GET_SERVER_DEFAULT_REQUEST;

    AsyncGet<GetServerDefaultsResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.getServerDefaults(null, req));
    asyncResponse(() ->
        PBHelperClient.convert(asyncGet.get(-1, null).getServerDefaults()));
    return null;
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

    AsyncGet<CreateResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.create(null, req));
    asyncResponse(() -> {
      CreateResponseProto res = asyncGet.get(-1, null);
      return res.hasFs() ? PBHelperClient.convert(res.getFs()) : null;
    });
    return null;
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

    AsyncGet<TruncateResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.truncate(null, req));
    asyncResponse(() -> asyncGet.get(-1, null).getResult());
    return true;
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

    AsyncGet<AppendResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.append(null, req));
    asyncResponse(() -> {
      AppendResponseProto res = asyncGet.get(-1, null);
      LocatedBlock lastBlock = res.hasBlock() ? PBHelperClient
          .convertLocatedBlockProto(res.getBlock()) : null;
      HdfsFileStatus stat = (res.hasStat()) ?
          PBHelperClient.convert(res.getStat()) : null;
      return new LastBlockWithStatus(lastBlock, stat);
    });
    return null;
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

    AsyncGet<SetReplicationResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.setReplication(null, req));
    asyncResponse(() -> asyncGet.get(-1, null).getResult());
    return true;
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

    AsyncGet<SetPermissionResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.setPermission(null, req));
    asyncResponse(() -> {
      asyncGet.get(-1, null);
      return null;
    });
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.setOwner(src, username, groupname);
    }

    SetOwnerRequestProto.Builder req = SetOwnerRequestProto.newBuilder()
        .setSrc(src);
    if (username != null) {
      req.setUsername(username);
    }
    if (groupname != null) {
      req.setGroupname(groupname);
    }

    AsyncGet<SetOwnerResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.setOwner(null, req.build()));
    asyncResponse(() -> asyncGet.get(-1, null));
  }

  @Override
  public void abandonBlock(ExtendedBlock b, long fileId, String src,
                           String holder) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.abandonBlock(b, fileId, src, holder);
    }
    AbandonBlockRequestProto req = AbandonBlockRequestProto.newBuilder()
        .setB(PBHelperClient.convert(b)).setSrc(src).setHolder(holder)
        .setFileId(fileId).build();
    AsyncGet<AbandonBlockResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.abandonBlock(null, req));
    asyncResponse(() -> asyncGet.get(-1, null));
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
    AsyncGet<AddBlockResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.addBlock(null, req.build()));
    asyncResponse(() -> PBHelperClient.convertLocatedBlockProto(
        asyncGet.get(-1, null).getBlock()));
    return null;
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

    AsyncGet<GetAdditionalDatanodeResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.getAdditionalDatanode(null, req));
    asyncResponse(() -> PBHelperClient.convertLocatedBlockProto(
        asyncGet.get(-1, null).getBlock()));
    return null;
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

    AsyncGet<CompleteResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.complete(null, req.build()));
    asyncResponse(() -> asyncGet.get(-1, null).getResult());
    return true;
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

    AsyncGet<ReportBadBlocksResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.reportBadBlocks(null, req));
    asyncResponse(() -> asyncGet.get(-1, null));
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.rename(src, dst);
    }
    RenameRequestProto req = RenameRequestProto.newBuilder()
        .setSrc(src)
        .setDst(dst).build();

    AsyncGet<RenameResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.rename(null, req));
    asyncResponse(() ->
        asyncGet.get(-1, null).getResult());
    return true;
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

    AsyncGet<Rename2ResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.rename2(null, req));
    asyncResponse(() -> asyncGet.get(-1, null));
  }

  @Override
  public void concat(String trg, String[] srcs) throws IOException {
    ConcatRequestProto req = ConcatRequestProto.newBuilder().
        setTrg(trg).
        addAllSrcs(Arrays.asList(srcs)).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<ConcatResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.concat(null, req));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.concat(null, req));
    }
  }


  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    DeleteRequestProto req = DeleteRequestProto.newBuilder().setSrc(src)
        .setRecursive(recursive).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<DeleteResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.delete(null, req));
      asyncResponse(() ->
          asyncGet.get(-1, null).getResult());
      return true;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<MkdirsResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.mkdirs(null, req));
      asyncResponse(() ->
          asyncGet.get(-1, null).getResult());
      return true;
    }
    return ipc(() -> rpcProxy.mkdirs(null, req)).getResult();
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter,
                                     boolean needLocation) throws IOException {
    GetListingRequestProto req = GetListingRequestProto.newBuilder()
        .setSrc(src)
        .setStartAfter(ByteString.copyFrom(startAfter))
        .setNeedLocation(needLocation).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetListingResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getListing(null, req));
      asyncResponse(() -> {
        GetListingResponseProto result = asyncGet.get(-1, null);
        if (result.hasDirList()) {
          return PBHelperClient.convert(result.getDirList());
        }
        return null;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetBatchedListingResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getBatchedListing(null, req));
      asyncResponse(() -> {
        GetBatchedListingResponseProto result = asyncGet.get(-1, null);

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
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<RenewLeaseResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.renewLease(null, builder.build()));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.renewLease(null, builder.build()));
    }
  }

  @Override
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    RecoverLeaseRequestProto req = RecoverLeaseRequestProto.newBuilder()
        .setSrc(src)
        .setClientName(clientName).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<RecoverLeaseResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.recoverLease(null, req));
      asyncResponse(() -> asyncGet.get(-1, null).getResult());
      return true;
    }
    return ipc(() -> rpcProxy.recoverLease(null, req)).getResult();
  }

  @Override
  public long[] getStats() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<GetFsStatsResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getFsStats(null, VOID_GET_FSSTATUS_REQUEST));
      asyncResponse(() -> PBHelperClient.convert(asyncGet.get(-1, null)));
      return null;
    }
    return PBHelperClient.convert(ipc(() -> rpcProxy.getFsStats(null,
        VOID_GET_FSSTATUS_REQUEST)));
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<GetFsReplicatedBlockStatsResponseProto, Exception> asyncGet =
          asyncIpc(() -> rpcProxy.getFsReplicatedBlockStats(null,
              VOID_GET_FS_REPLICATED_BLOCK_STATS_REQUEST));
      asyncResponse(() -> PBHelperClient.convert(
          asyncGet.get(-1, null)));
      return null;
    }
    return PBHelperClient.convert(ipc(() -> rpcProxy.getFsReplicatedBlockStats(null,
        VOID_GET_FS_REPLICATED_BLOCK_STATS_REQUEST)));
  }

  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<GetFsECBlockGroupStatsResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getFsECBlockGroupStats(null,
          VOID_GET_FS_ECBLOCKGROUP_STATS_REQUEST));
      asyncResponse(() -> PBHelperClient.convert(
          asyncGet.get(-1, null)));
      return null;
    }
    return PBHelperClient.convert(ipc(() -> rpcProxy.getFsECBlockGroupStats(null,
        VOID_GET_FS_ECBLOCKGROUP_STATS_REQUEST)));
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    GetDatanodeReportRequestProto req = GetDatanodeReportRequestProto
        .newBuilder()
        .setType(PBHelperClient.convert(type)).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetDatanodeReportResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getDatanodeReport(null, req));
      asyncResponse(() ->
          PBHelperClient.convert(asyncGet.get(-1, null).getDiList()));
      return null;
    }
    return PBHelperClient.convert(
        ipc(() -> rpcProxy.getDatanodeReport(null, req)).getDiList());
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    final GetDatanodeStorageReportRequestProto req
        = GetDatanodeStorageReportRequestProto.newBuilder()
        .setType(PBHelperClient.convert(type)).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetDatanodeStorageReportResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getDatanodeStorageReport(null, req));
      asyncResponse(() ->
          PBHelperClient.convertDatanodeStorageReports(
              asyncGet.get(-1, null).getDatanodeStorageReportsList()));
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetPreferredBlockSizeResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getPreferredBlockSize(null, req));
      asyncResponse(() -> asyncGet.get(-1, null).getBsize());
      return -1;
    }
    return ipc(() -> rpcProxy.getPreferredBlockSize(null, req)).getBsize();
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    SetSafeModeRequestProto req = SetSafeModeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action))
        .setChecked(isChecked).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<SetSafeModeResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setSafeMode(null, req));
      asyncResponse(() -> asyncGet.get(-1, null).getResult());
      return true;
    }
    return ipc(() -> rpcProxy.setSafeMode(null, req)).getResult();
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    SaveNamespaceRequestProto req = SaveNamespaceRequestProto.newBuilder()
        .setTimeWindow(timeWindow).setTxGap(txGap).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<SaveNamespaceResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.saveNamespace(null, req));
      asyncResponse(() -> asyncGet.get(-1, null).getSaved());
      return true;
    }
    return ipc(() -> rpcProxy.saveNamespace(null, req)).getSaved();
  }

  @Override
  public long rollEdits() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<RollEditsResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.rollEdits(null, VOID_ROLLEDITS_REQUEST));
      asyncResponse(() -> asyncGet.get(-1, null).getNewSegmentTxId());
      return -1;
    }
    RollEditsResponseProto resp = ipc(() -> rpcProxy.rollEdits(null,
        VOID_ROLLEDITS_REQUEST));
    return resp.getNewSegmentTxId();
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException{
    RestoreFailedStorageRequestProto req = RestoreFailedStorageRequestProto
        .newBuilder()
        .setArg(arg).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<RestoreFailedStorageResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.restoreFailedStorage(null, req));
      asyncResponse(() -> asyncGet.get(-1, null).getResult());
      return true;
    }
    return ipc(() -> rpcProxy.restoreFailedStorage(null, req)).getResult();
  }

  @Override
  public void refreshNodes() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<RefreshNodesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.refreshNodes(null, VOID_REFRESH_NODES_REQUEST));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.refreshNodes(null, VOID_REFRESH_NODES_REQUEST));
    }
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<FinalizeUpgradeResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.finalizeUpgrade(null, VOID_FINALIZE_UPGRADE_REQUEST));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.finalizeUpgrade(null, VOID_FINALIZE_UPGRADE_REQUEST));
    }
  }

  @Override
  public boolean upgradeStatus() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<UpgradeStatusResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.upgradeStatus(null, VOID_UPGRADE_STATUS_REQUEST));
      asyncResponse(() -> asyncGet.get(-1, null).getUpgradeFinalized());
      return true;
    }
    final UpgradeStatusResponseProto proto = ipc(() -> rpcProxy.upgradeStatus(
        null, VOID_UPGRADE_STATUS_REQUEST));
    return proto.getUpgradeFinalized();
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action)
      throws IOException {
    final RollingUpgradeRequestProto r = RollingUpgradeRequestProto.newBuilder()
        .setAction(PBHelperClient.convert(action)).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<RollingUpgradeResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.rollingUpgrade(null, r));
      asyncResponse(() -> PBHelperClient.convert(
          asyncGet.get(-1, null).getRollingUpgradeInfo()));
      return null;
    }
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
    if (cookie != null) {
      req.setCookie(cookie);
    }

    if (Client.isAsynchronousMode()) {
      AsyncGet<ListCorruptFileBlocksResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.listCorruptFileBlocks(null, req.build()));
      asyncResponse(() -> asyncGet.get(-1, null).getCorrupt());
      return null;
    }
    return PBHelperClient.convert(
        ipc(() -> rpcProxy.listCorruptFileBlocks(null, req.build())).getCorrupt());
  }

  @Override
  public void metaSave(String filename) throws IOException {
    MetaSaveRequestProto req = MetaSaveRequestProto.newBuilder()
        .setFilename(filename).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<MetaSaveResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.metaSave(null, req));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.metaSave(null, req));
    }
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    GetFileInfoRequestProto req = GetFileInfoRequestProto.newBuilder()
        .setSrc(src)
        .build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetFileInfoResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getFileInfo(null, req));
      asyncResponse(() -> {
        GetFileInfoResponseProto res = asyncGet.get(-1, null);
        return res.hasFs() ? PBHelperClient.convert(res.getFs()) : null;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetLocatedFileInfoResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getLocatedFileInfo(null, req));
      asyncResponse((AsyncRpcProtocolPBUtil.Response<Object>) () -> {
        GetLocatedFileInfoResponseProto res = asyncGet.get(-1, null);
        return res.hasFs() ? PBHelperClient.convert(res.getFs()) : null;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetFileLinkInfoResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getFileLinkInfo(null, req));
      asyncResponse(() -> {
        GetFileLinkInfoResponseProto result =  asyncGet.get(-1, null);
        return result.hasFs() ? PBHelperClient.convert(result.getFs()) : null;
      });
      return null;
    }
    GetFileLinkInfoResponseProto result = ipc(() -> rpcProxy.getFileLinkInfo(null, req));
    return result.hasFs() ? PBHelperClient.convert(result.getFs()) : null;
  }

  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    GetContentSummaryRequestProto req = GetContentSummaryRequestProto
        .newBuilder()
        .setPath(path)
        .build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetContentSummaryResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getContentSummary(null, req));
      asyncResponse(() -> PBHelperClient.convert(
          asyncGet.get(-1, null).getSummary()));
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<SetQuotaResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setQuota(null, req));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.setQuota(null, req));
    }
  }

  @Override
  public void fsync(String src, long fileId, String client,
                    long lastBlockLength) throws IOException {
    FsyncRequestProto req = FsyncRequestProto.newBuilder().setSrc(src)
        .setClient(client).setLastBlockLength(lastBlockLength)
        .setFileId(fileId).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<FsyncResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.fsync(null, req));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.fsync(null, req));
    }
  }

  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    SetTimesRequestProto req = SetTimesRequestProto.newBuilder()
        .setSrc(src)
        .setMtime(mtime)
        .setAtime(atime)
        .build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<SetTimesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setTimes(null, req));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.setTimes(null, req));
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<CreateSymlinkResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.createSymlink(null, req));
      asyncResponse(() -> asyncGet.get(-1, null));
    } else {
      ipc(() -> rpcProxy.createSymlink(null, req));
    }
  }

  @Override
  public String getLinkTarget(String path) throws IOException {
    GetLinkTargetRequestProto req = GetLinkTargetRequestProto.newBuilder()
        .setPath(path).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetLinkTargetResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getLinkTarget(null, req));
      asyncResponse(new AsyncRpcProtocolPBUtil.Response<Object>() {
        @Override
        public Object response() throws Exception {
          GetLinkTargetResponseProto rsp = asyncGet.get(-1, null);
          return rsp.hasTargetPath() ? rsp.getTargetPath() : null;
        }
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<UpdateBlockForPipelineResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.updateBlockForPipeline(null, req));
      asyncResponse(
          () -> PBHelperClient.convertLocatedBlockProto(
              asyncGet.get(-1, null).getBlock()));
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<UpdatePipelineResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.updatePipeline(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.updatePipeline(null, req));
    }
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer == null ? "" : renewer.toString())
        .build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetDelegationTokenResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getDelegationToken(null, req));
      asyncResponse(() -> {
        GetDelegationTokenResponseProto resp = asyncGet.get(-1, null);
        return resp.hasToken() ?
            PBHelperClient.convertDelegationToken(resp.getToken()) : null;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<SecurityProtos.RenewDelegationTokenResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.renewDelegationToken(null, req));
      asyncResponse(() ->
          asyncGet.get(-1, null).getNewExpiryTime());
      return -1;
    }
    return ipc(() -> rpcProxy.renewDelegationToken(null, req)).getNewExpiryTime();
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto
        .newBuilder()
        .setToken(PBHelperClient.convert(token))
        .build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<SecurityProtos.CancelDelegationTokenResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.cancelDelegationToken(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.cancelDelegationToken(null, req));
    }
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    SetBalancerBandwidthRequestProto req =
        SetBalancerBandwidthRequestProto.newBuilder()
            .setBandwidth(bandwidth)
            .build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<SetBalancerBandwidthResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setBalancerBandwidth(null, req));
      asyncResponse(new Response<Object>() {
        @Override
        public Object response() throws Exception {
          asyncGet.get(-1, null);
          return null;
        }
      });
    } else {
      ipc(() -> rpcProxy.setBalancerBandwidth(null, req));
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
    if (Client.isAsynchronousMode()) {
      AsyncGet<GetDataEncryptionKeyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getDataEncryptionKey(null,
          VOID_GET_DATA_ENCRYPTIONKEY_REQUEST));
      asyncResponse(() -> {
        GetDataEncryptionKeyResponseProto rsp = asyncGet.get(-1, null);
        return rsp.hasDataEncryptionKey() ?
            PBHelperClient.convert(rsp.getDataEncryptionKey()) : null;
      });
      return null;
    }
    GetDataEncryptionKeyResponseProto rsp = ipc(() -> rpcProxy.getDataEncryptionKey(
        null, VOID_GET_DATA_ENCRYPTIONKEY_REQUEST));
    return rsp.hasDataEncryptionKey() ?
        PBHelperClient.convert(rsp.getDataEncryptionKey()) : null;
  }


  @Override
  public boolean isFileClosed(String src) throws IOException {
    IsFileClosedRequestProto req = IsFileClosedRequestProto.newBuilder()
        .setSrc(src).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<IsFileClosedResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.isFileClosed(null, req));
      asyncResponse(() -> asyncGet.get(-1, null).getResult());
      return true;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<CreateSnapshotResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.createSnapshot(null, req));
      asyncResponse(() ->
          asyncGet.get(-1, null).getSnapshotPath());
      return null;
    }
    return ipc(() -> rpcProxy.createSnapshot(null, req)).getSnapshotPath();
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    DeleteSnapshotRequestProto req = DeleteSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotName(snapshotName).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<DeleteSnapshotResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.deleteSnapshot(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.deleteSnapshot(null, req));
    }
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    AllowSnapshotRequestProto req = AllowSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<AllowSnapshotResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.allowSnapshot(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.allowSnapshot(null, req));
    }
  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    DisallowSnapshotRequestProto req = DisallowSnapshotRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<DisallowSnapshotResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.disallowSnapshot(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.disallowSnapshot(null, req));
    }
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName,
                             String snapshotNewName) throws IOException {
    RenameSnapshotRequestProto req = RenameSnapshotRequestProto.newBuilder()
        .setSnapshotRoot(snapshotRoot).setSnapshotOldName(snapshotOldName)
        .setSnapshotNewName(snapshotNewName).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<RenameSnapshotResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.renameSnapshot(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.renameSnapshot(null, req));
    }
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    GetSnapshottableDirListingRequestProto req =
        GetSnapshottableDirListingRequestProto.newBuilder().build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetSnapshottableDirListingResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getSnapshottableDirListing(null, req));
      asyncResponse(() -> {
        GetSnapshottableDirListingResponseProto result = asyncGet.get(-1, null);
        if (result.hasSnapshottableDirList()) {
          return PBHelperClient.convert(result.getSnapshottableDirList());
        }
        return null;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetSnapshotListingResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getSnapshotListing(null, req));
      asyncResponse(() -> {
        GetSnapshotListingResponseProto result = asyncGet.get(-1, null);
        if (result.hasSnapshotList()) {
          return PBHelperClient.convert(result.getSnapshotList());
        }
        return null;
      });
      return null;
    }
    GetSnapshotListingResponseProto result = ipc(() -> rpcProxy
        .getSnapshotListing(null, req));

    if (result.hasSnapshotList()) {
      return PBHelperClient.convert(result.getSnapshotList());
    }
    return null;
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(
      String snapshotRoot,
      String fromSnapshot, String toSnapshot) throws IOException {
    GetSnapshotDiffReportRequestProto req = GetSnapshotDiffReportRequestProto
        .newBuilder().setSnapshotRoot(snapshotRoot)
        .setFromSnapshot(fromSnapshot).setToSnapshot(toSnapshot).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetSnapshotDiffReportResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getSnapshotDiffReport(null, req));
      asyncResponse(() -> PBHelperClient.convert(asyncGet.get(-1, null)
          .getDiffReport()));
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetSnapshotDiffReportListingResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getSnapshotDiffReportListing(null, req));
      asyncResponse(new Response<Object>() {
        @Override
        public Object response() throws Exception {
          return PBHelperClient.convert(asyncGet.get(-1, null).getDiffReport());
        }
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<AddCacheDirectiveResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.addCacheDirective(null, builder.build()));
      asyncResponse(() -> asyncGet.get(-1, null).getId());
      return -1;
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
    if (Client.isAsynchronousMode()) {
      AsyncGet<ModifyCacheDirectiveResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.modifyCacheDirective(null, builder.build()));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.modifyCacheDirective(null, builder.build()));
    }
  }

  @Override
  public void removeCacheDirective(long id)
      throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<RemoveCacheDirectiveResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.removeCacheDirective(null,
          RemoveCacheDirectiveRequestProto.newBuilder().
              setId(id).build()));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.removeCacheDirective(null,
          RemoveCacheDirectiveRequestProto.newBuilder().
              setId(id).build()));
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
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId,
      CacheDirectiveInfo filter) throws IOException {
    if (filter == null) {
      filter = new CacheDirectiveInfo.Builder().build();
    }
    CacheDirectiveInfo f = filter;

    if (Client.isAsynchronousMode()) {
      AsyncGet<ListCacheDirectivesResponseProto, Exception> asyncGet =
          asyncIpc(() -> rpcProxy.listCacheDirectives(null,
              ListCacheDirectivesRequestProto.newBuilder().
                  setPrevId(prevId).
                  setFilter(PBHelperClient.convert(f)).
                  build()));
      asyncResponse(() -> new BatchedCacheEntries(asyncGet.get(-1, null)));
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<AddCachePoolResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.addCachePool(null, builder.build()));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.addCachePool(null, builder.build()));
    }
  }

  @Override
  public void modifyCachePool(CachePoolInfo req) throws IOException {
    ModifyCachePoolRequestProto.Builder builder =
        ModifyCachePoolRequestProto.newBuilder();
    builder.setInfo(PBHelperClient.convert(req));

    if (Client.isAsynchronousMode()) {
      AsyncGet<ModifyCachePoolResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.modifyCachePool(null, builder.build()));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.modifyCachePool(null, builder.build()));
    }
  }

  @Override
  public void removeCachePool(String cachePoolName) throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<RemoveCachePoolResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.removeCachePool(null,
          RemoveCachePoolRequestProto.newBuilder().
              setPoolName(cachePoolName).build()));

      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.removeCachePool(null,
          RemoveCachePoolRequestProto.newBuilder().
              setPoolName(cachePoolName).build()));
    }
  }

  private static class BatchedCachePoolEntries
      implements BatchedEntries<CachePoolEntry> {
    private final ListCachePoolsResponseProto proto;

    BatchedCachePoolEntries(ListCachePoolsResponseProto proto) {
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
    if (Client.isAsynchronousMode()) {
      AsyncGet<ListCachePoolsResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.listCachePools(null,
          ListCachePoolsRequestProto.newBuilder().
              setPrevPoolName(prevKey).build()));
      asyncResponse((Response<Object>) () ->
          new BatchedCachePoolEntries(asyncGet.get(-1, null)));
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<AclProtos.ModifyAclEntriesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.modifyAclEntries(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.modifyAclEntries(null, req));
    }
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    RemoveAclEntriesRequestProto req = RemoveAclEntriesRequestProto
        .newBuilder().setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec)).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<AclProtos.RemoveAclEntriesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.removeAclEntries(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.removeAclEntries(null, req));
    }
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    RemoveDefaultAclRequestProto req = RemoveDefaultAclRequestProto
        .newBuilder().setSrc(src).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<AclProtos.RemoveDefaultAclResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.removeDefaultAcl(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.removeDefaultAcl(null, req));
    }
  }

  @Override
  public void removeAcl(String src) throws IOException {
    RemoveAclRequestProto req = RemoveAclRequestProto.newBuilder()
        .setSrc(src).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<AclProtos.RemoveAclResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.removeAcl(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.removeAcl(null, req));
    }
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    SetAclRequestProto req = SetAclRequestProto.newBuilder()
        .setSrc(src)
        .addAllAclSpec(PBHelperClient.convertAclEntryProto(aclSpec))
        .build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<AclProtos.SetAclResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setAcl(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
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
        AsyncGet<GetAclStatusResponseProto, Exception> asyncGet =
            asyncIpc(() -> rpcProxy.getAclStatus(null, req));
        asyncResponse((Response<Object>) () ->
            PBHelperClient.convert(asyncGet.get(-1, null)));
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<EncryptionZonesProtos.CreateEncryptionZoneResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.createEncryptionZone(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.createEncryptionZone(null, req));
    }
  }

  @Override
  public EncryptionZone getEZForPath(String src) throws IOException {
    final GetEZForPathRequestProto.Builder builder =
        GetEZForPathRequestProto.newBuilder();
    builder.setSrc(src);
    final GetEZForPathRequestProto req = builder.build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<EncryptionZonesProtos.GetEZForPathResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getEZForPath(null, req));
      asyncResponse((Response<Object>) () -> {
        final EncryptionZonesProtos.GetEZForPathResponseProto response
            = asyncGet.get(-1, null);
        if (response.hasZone()) {
          return PBHelperClient.convert(response.getZone());
        } else {
          return null;
        }
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<EncryptionZonesProtos.ListEncryptionZonesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.listEncryptionZones(null, req));
      asyncResponse((Response<Object>) () -> {
        EncryptionZonesProtos.ListEncryptionZonesResponseProto response
            = asyncGet.get(-1, null);
        List<EncryptionZone> elements =
            Lists.newArrayListWithCapacity(response.getZonesCount());
        for (EncryptionZoneProto p : response.getZonesList()) {
          elements.add(PBHelperClient.convert(p));
        }
        return new BatchedListEntries<>(elements, response.getHasMore());
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<ErasureCodingProtos.SetErasureCodingPolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setErasureCodingPolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.setErasureCodingPolicy(null, req));
    }
  }

  @Override
  public void unsetErasureCodingPolicy(String src) throws IOException {
    final UnsetErasureCodingPolicyRequestProto.Builder builder =
        UnsetErasureCodingPolicyRequestProto.newBuilder();
    builder.setSrc(src);
    UnsetErasureCodingPolicyRequestProto req = builder.build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<ErasureCodingProtos.UnsetErasureCodingPolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.unsetErasureCodingPolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.unsetErasureCodingPolicy(null, req));
    }
  }

  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      final String... policyNames) throws IOException {
    final GetECTopologyResultForPoliciesRequestProto.Builder builder =
        GetECTopologyResultForPoliciesRequestProto.newBuilder();
    builder.addAllPolicies(Arrays.asList(policyNames));
    GetECTopologyResultForPoliciesRequestProto req = builder.build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetECTopologyResultForPoliciesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getECTopologyResultForPolicies(null, req));
      asyncResponse((Response<Object>) () -> PBHelperClient
          .convertECTopologyVerifierResultProto(
              asyncGet.get(-1, null).getResponse()));
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<EncryptionZonesProtos.ReencryptEncryptionZoneResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.reencryptEncryptionZone(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.reencryptEncryptionZone(null, req));
    }
  }

  @Override
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(long id)
      throws IOException {
    final ListReencryptionStatusRequestProto req =
        ListReencryptionStatusRequestProto.newBuilder().setId(id).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<ListReencryptionStatusResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.listReencryptionStatus(null, req));
      asyncResponse((Response<Object>) () -> {
        ListReencryptionStatusResponseProto response = asyncGet.get(-1, null);
        List<ZoneReencryptionStatus> elements =
            Lists.newArrayListWithCapacity(response.getStatusesCount());
        for (ZoneReencryptionStatusProto p : response.getStatusesList()) {
          elements.add(PBHelperClient.convert(p));
        }
        return new BatchedListEntries<>(elements, response.getHasMore());
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<XAttrProtos.SetXAttrResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setXAttr(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.setXAttr(null, req));
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<XAttrProtos.GetXAttrsResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getXAttrs(null, req));
      asyncResponse((Response<Object>) () ->
          PBHelperClient.convert(asyncGet.get(-1, null)));
      return null;
    }
    return PBHelperClient.convert(ipc(() -> rpcProxy.getXAttrs(null, req)));
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    ListXAttrsRequestProto.Builder builder =
        ListXAttrsRequestProto.newBuilder();
    builder.setSrc(src);
    ListXAttrsRequestProto req = builder.build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<XAttrProtos.ListXAttrsResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.listXAttrs(null, req));
      asyncResponse((Response<Object>) () ->
          PBHelperClient.convert(asyncGet.get(-1, null)));
      return null;
    }
    return PBHelperClient.convert(ipc(() -> rpcProxy.listXAttrs(null, req)));
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    RemoveXAttrRequestProto req = RemoveXAttrRequestProto
        .newBuilder().setSrc(src)
        .setXAttr(PBHelperClient.convertXAttrProto(xAttr)).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<XAttrProtos.RemoveXAttrResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.removeXAttr(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.removeXAttr(null, req));
    }
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {
    CheckAccessRequestProto req = CheckAccessRequestProto.newBuilder()
        .setPath(path).setMode(PBHelperClient.convert(mode)).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<CheckAccessResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.checkAccess(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.checkAccess(null, req));
    }
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    SetStoragePolicyRequestProto req = SetStoragePolicyRequestProto
        .newBuilder().setSrc(src).setPolicyName(policyName).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<SetStoragePolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.setStoragePolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.setStoragePolicy(null, req));
    }
  }

  @Override
  public void unsetStoragePolicy(String src) throws IOException {
    UnsetStoragePolicyRequestProto req = UnsetStoragePolicyRequestProto
        .newBuilder().setSrc(src).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<UnsetStoragePolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.unsetStoragePolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.unsetStoragePolicy(null, req));
    }
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    GetStoragePolicyRequestProto request = GetStoragePolicyRequestProto
        .newBuilder().setPath(path).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetStoragePolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getStoragePolicy(null, request));
      asyncResponse((Response<Object>) () ->
          PBHelperClient.convert(asyncGet.get(-1, null).getStoragePolicy()));
      return null;
    }
    return PBHelperClient.convert(ipc(() -> rpcProxy.getStoragePolicy(null, request))
        .getStoragePolicy());
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<GetStoragePoliciesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy
          .getStoragePolicies(null, VOID_GET_STORAGE_POLICIES_REQUEST));
      asyncResponse((Response<Object>) () -> PBHelperClient.convertStoragePolicies(
          asyncGet.get(-1, null).getPoliciesList()));
      return null;
    }
    GetStoragePoliciesResponseProto response = ipc(() -> rpcProxy
        .getStoragePolicies(null, VOID_GET_STORAGE_POLICIES_REQUEST));
    return PBHelperClient.convertStoragePolicies(response.getPoliciesList());
  }

  public long getCurrentEditLogTxid() throws IOException {
    GetCurrentEditLogTxidRequestProto req = GetCurrentEditLogTxidRequestProto
        .getDefaultInstance();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetCurrentEditLogTxidResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getCurrentEditLogTxid(null, req));
      asyncResponse((Response<Object>) () -> asyncGet.get(-1, null).getTxid());
      return -1;
    }
    return ipc(() -> rpcProxy.getCurrentEditLogTxid(null, req)).getTxid();
  }

  @Override
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    GetEditsFromTxidRequestProto req = GetEditsFromTxidRequestProto.newBuilder()
        .setTxid(txid).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetEditsFromTxidResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getEditsFromTxid(null, req));
      asyncResponse((Response<Object>) () -> PBHelperClient.convert(asyncGet.get(-1, null)));
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<AddErasureCodingPoliciesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy
          .addErasureCodingPolicies(null, req));
      asyncResponse((Response<Object>) () -> {
        AddErasureCodingPoliciesResponseProto rep = asyncGet.get(-1, null);
        AddErasureCodingPolicyResponse[] responses =
            rep.getResponsesList().stream()
                .map(PBHelperClient::convertAddErasureCodingPolicyResponse)
                .toArray(AddErasureCodingPolicyResponse[]::new);
        return responses;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<ErasureCodingProtos.RemoveErasureCodingPolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.removeErasureCodingPolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.removeErasureCodingPolicy(null, req));
    }
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    EnableErasureCodingPolicyRequestProto.Builder builder =
        EnableErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    EnableErasureCodingPolicyRequestProto req = builder.build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<ErasureCodingProtos.EnableErasureCodingPolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.enableErasureCodingPolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.enableErasureCodingPolicy(null, req));
    }
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    DisableErasureCodingPolicyRequestProto.Builder builder =
        DisableErasureCodingPolicyRequestProto.newBuilder();
    builder.setEcPolicyName(ecPolicyName);
    DisableErasureCodingPolicyRequestProto req = builder.build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<ErasureCodingProtos.DisableErasureCodingPolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.disableErasureCodingPolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.disableErasureCodingPolicy(null, req));
    }
  }

  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    if (Client.isAsynchronousMode()) {
      AsyncGet<GetErasureCodingPoliciesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy
          .getErasureCodingPolicies(null, VOID_GET_EC_POLICIES_REQUEST));
      asyncResponse((Response<Object>) () -> {
        GetErasureCodingPoliciesResponseProto response = asyncGet.get(-1, null);
        ErasureCodingPolicyInfo[] ecPolicies =
            new ErasureCodingPolicyInfo[response.getEcPoliciesCount()];
        int i = 0;
        for (ErasureCodingPolicyProto proto : response.getEcPoliciesList()) {
          ecPolicies[i++] =
              PBHelperClient.convertErasureCodingPolicyInfo(proto);
        }
        return ecPolicies;
      });
      return null;
    }
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
    if (Client.isAsynchronousMode()) {
      AsyncGet<GetErasureCodingCodecsResponseProto, Exception> asyncGet =
          asyncIpc(() -> rpcProxy
              .getErasureCodingCodecs(null, VOID_GET_EC_CODEC_REQUEST));
      asyncResponse(() -> {
        GetErasureCodingCodecsResponseProto response = asyncGet.get(-1, null);
        Map<String, String> ecCodecs = new HashMap<>();
        for (CodecProto codec : response.getCodecList()) {
          ecCodecs.put(codec.getCodec(), codec.getCoders());
        }
        return ecCodecs;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetErasureCodingPolicyResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getErasureCodingPolicy(null, req));
      asyncResponse((Response<Object>) () -> {
        GetErasureCodingPolicyResponseProto response = asyncGet.get(-1, null);
        if (response.hasEcPolicy()) {
          return PBHelperClient.convertErasureCodingPolicy(
              response.getEcPolicy());
        }
        return null;
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetQuotaUsageResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getQuotaUsage(null, req));
      asyncResponse((Response<Object>) () ->
          PBHelperClient.convert(asyncGet.get(-1, null).getUsage()));
      return null;
    }
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
  public BatchedEntries<OpenFileEntry> listOpenFiles(
      long prevId,
      EnumSet<OpenFilesType> openFilesTypes, String path) throws IOException {
    ListOpenFilesRequestProto.Builder req =
        ListOpenFilesRequestProto.newBuilder().setId(prevId);
    if (openFilesTypes != null) {
      req.addAllTypes(PBHelperClient.convertOpenFileTypes(openFilesTypes));
    }
    req.setPath(path);

    if (Client.isAsynchronousMode()) {
      AsyncGet<ListOpenFilesResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.listOpenFiles(null, req.build()));
      asyncResponse(() -> {
        ListOpenFilesResponseProto response = asyncGet.get(-1, null);
        List<OpenFileEntry> openFileEntries =
            Lists.newArrayListWithCapacity(response.getEntriesCount());
        for (OpenFilesBatchResponseProto p : response.getEntriesList()) {
          openFileEntries.add(PBHelperClient.convert(p));
        }
        return new BatchedListEntries<>(openFileEntries, response.getHasMore());
      });
      return null;
    }
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

    if (Client.isAsynchronousMode()) {
      AsyncGet<MsyncResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.msync(null, req.build()));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.msync(null, req.build()));
    }
  }

  @Override
  public void satisfyStoragePolicy(String src) throws IOException {
    SatisfyStoragePolicyRequestProto req =
        SatisfyStoragePolicyRequestProto.newBuilder().setSrc(src).build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<SatisfyStoragePolicyResponseProto, Exception> asyncGet =
          asyncIpc(() -> rpcProxy.satisfyStoragePolicy(null, req));
      asyncResponse(() -> {
        asyncGet.get(-1, null);
        return null;
      });
    } else {
      ipc(() -> rpcProxy.satisfyStoragePolicy(null, req));
    }
  }

  @Override
  public DatanodeInfo[] getSlowDatanodeReport() throws IOException {
    GetSlowDatanodeReportRequestProto req =
        GetSlowDatanodeReportRequestProto.newBuilder().build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetSlowDatanodeReportResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getSlowDatanodeReport(null, req));
      asyncResponse(() ->
          PBHelperClient.convert(asyncGet.get(-1, null).getDatanodeInfoProtoList()));
      return null;
    }
    return PBHelperClient.convert(
        ipc(() -> rpcProxy.getSlowDatanodeReport(null, req)).getDatanodeInfoProtoList());
  }

  @Override
  public HAServiceProtocol.HAServiceState getHAServiceState()
      throws IOException {
    HAServiceStateRequestProto req =
        HAServiceStateRequestProto.newBuilder().build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<HAServiceStateResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getHAServiceState(null, req));
      asyncResponse(() -> {
        HAServiceStateProto res = asyncGet.get(-1, null).getState();
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
      });
      return null;
    }
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

  @Override
  public Path getEnclosingRoot(String filename) throws IOException {
    final GetEnclosingRootRequestProto.Builder builder =
        GetEnclosingRootRequestProto.newBuilder();
    builder.setFilename(filename);
    final GetEnclosingRootRequestProto req = builder.build();

    if (Client.isAsynchronousMode()) {
      AsyncGet<GetEnclosingRootResponseProto, Exception> asyncGet
          = asyncIpc(() -> rpcProxy.getEnclosingRoot(null, req));
      asyncResponse(() ->
          new Path(asyncGet.get(-1, null).getEnclosingRootPath()));
      return null;
    }
    try {
      final GetEnclosingRootResponseProto response =
          rpcProxy.getEnclosingRoot(null, req);
      return new Path(response.getEnclosingRootPath());
    } catch (ServiceException e) {
      throw getRemoteException(e);
    }
  }
}