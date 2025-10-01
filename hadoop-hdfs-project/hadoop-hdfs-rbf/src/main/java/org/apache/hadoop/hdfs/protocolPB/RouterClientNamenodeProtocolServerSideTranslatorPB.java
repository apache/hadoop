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

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsPartialListing;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatusRequestProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshotListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesRequestProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollEditsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SatisfyStoragePolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SatisfyStoragePolicyResponseProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpgradeStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpgradeStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListReencryptionStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListReencryptionStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ReencryptEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ReencryptEncryptionZoneResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.AddErasureCodingPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.AddErasureCodingPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.DisableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.DisableErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.EnableErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.EnableErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetECTopologyResultForPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingCodecsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingCodecsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPoliciesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPoliciesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.GetErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.RemoveErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.RemoveErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.SetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.SetErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.UnsetErasureCodingPolicyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.UnsetErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrResponseProto;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.ProtocolStringList;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterClientNamenodeProtocolServerSideTranslatorPB
    extends ClientNamenodeProtocolServerSideTranslatorPB {

  private final RouterRpcServer server;

  public RouterClientNamenodeProtocolServerSideTranslatorPB(
      ClientProtocol server) throws IOException {
    super(server);
    this.server = (RouterRpcServer) server;
  }

  @Override
  public GetBlockLocationsResponseProto getBlockLocations(
      RpcController controller, GetBlockLocationsRequestProto req) {
    asyncRouterServer(() -> server.getBlockLocations(req.getSrc(), req.getOffset(),
        req.getLength()),
        b -> {
          GetBlockLocationsResponseProto.Builder builder
              = GetBlockLocationsResponseProto
              .newBuilder();
          if (b != null) {
            builder.setLocations(PBHelperClient.convert(b)).build();
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public GetServerDefaultsResponseProto getServerDefaults(
      RpcController controller, GetServerDefaultsRequestProto req) {
    asyncRouterServer(server::getServerDefaults,
        result -> GetServerDefaultsResponseProto.newBuilder()
            .setServerDefaults(PBHelperClient.convert(result))
            .build());
    return null;
  }


  @Override
  public CreateResponseProto create(RpcController controller, CreateRequestProto req) {
    asyncRouterServer(() -> {
      FsPermission masked = req.hasUnmasked() ?
          FsCreateModes.create(PBHelperClient.convert(req.getMasked()),
              PBHelperClient.convert(req.getUnmasked())) :
          PBHelperClient.convert(req.getMasked());
      return server.create(req.getSrc(),
          masked, req.getClientName(),
          PBHelperClient.convertCreateFlag(req.getCreateFlag()), req.getCreateParent(),
          (short) req.getReplication(), req.getBlockSize(),
          PBHelperClient.convertCryptoProtocolVersions(
              req.getCryptoProtocolVersionList()),
          req.getEcPolicyName(), req.getStoragePolicy());
    }, result -> {
        if (result != null) {
          return CreateResponseProto.newBuilder().setFs(PBHelperClient.convert(result))
              .build();
        }
        return VOID_CREATE_RESPONSE;
      });
    return null;
  }

  @Override
  public AppendResponseProto append(RpcController controller,
      AppendRequestProto req) {
    asyncRouterServer(() -> {
      EnumSetWritable<CreateFlag> flags = req.hasFlag() ?
          PBHelperClient.convertCreateFlag(req.getFlag()) :
          new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND));
      return server.append(req.getSrc(),
          req.getClientName(), flags);
    }, result -> {
        AppendResponseProto.Builder builder =
            AppendResponseProto.newBuilder();
        if (result.getLastBlock() != null) {
          builder.setBlock(PBHelperClient.convertLocatedBlock(
              result.getLastBlock()));
        }
        if (result.getFileStatus() != null) {
          builder.setStat(PBHelperClient.convert(result.getFileStatus()));
        }
        return builder.build();
      });
    return null;
  }

  @Override
  public SetReplicationResponseProto setReplication(
      RpcController controller,
      SetReplicationRequestProto req) {
    asyncRouterServer(() ->
            server.setReplication(req.getSrc(), (short) req.getReplication()),
        result -> SetReplicationResponseProto.newBuilder().setResult(result).build());
    return null;
  }


  @Override
  public SetPermissionResponseProto setPermission(
      RpcController controller,
      SetPermissionRequestProto req) {
    asyncRouterServer(() -> {
      server.setPermission(req.getSrc(), PBHelperClient.convert(req.getPermission()));
      return null;
    }, result -> VOID_SET_PERM_RESPONSE);
    return null;
  }

  @Override
  public SetOwnerResponseProto setOwner(
      RpcController controller,
      SetOwnerRequestProto req) {
    asyncRouterServer(() -> {
      server.setOwner(req.getSrc(),
          req.hasUsername() ? req.getUsername() : null,
          req.hasGroupname() ? req.getGroupname() : null);
      return null;
    }, result -> VOID_SET_OWNER_RESPONSE);
    return null;
  }

  @Override
  public AbandonBlockResponseProto abandonBlock(
      RpcController controller,
      AbandonBlockRequestProto req) {
    asyncRouterServer(() -> {
      server.abandonBlock(PBHelperClient.convert(req.getB()), req.getFileId(),
          req.getSrc(), req.getHolder());
      return null;
    }, result -> VOID_ADD_BLOCK_RESPONSE);
    return null;
  }

  @Override
  public AddBlockResponseProto addBlock(
      RpcController controller,
      AddBlockRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.DatanodeInfoProto> excl = req.getExcludeNodesList();
      List<String> favor = req.getFavoredNodesList();
      EnumSet<AddBlockFlag> flags =
          PBHelperClient.convertAddBlockFlags(req.getFlagsList());
      return server.addBlock(
          req.getSrc(),
          req.getClientName(),
          req.hasPrevious() ? PBHelperClient.convert(req.getPrevious()) : null,
          (excl == null || excl.size() == 0) ? null : PBHelperClient.convert(excl
              .toArray(new HdfsProtos.DatanodeInfoProto[excl.size()])), req.getFileId(),
          (favor == null || favor.size() == 0) ? null : favor
              .toArray(new String[favor.size()]),
          flags);
    }, result -> AddBlockResponseProto.newBuilder()
        .setBlock(PBHelperClient.convertLocatedBlock(result)).build());
    return null;
  }

  @Override
  public GetAdditionalDatanodeResponseProto getAdditionalDatanode(
      RpcController controller, GetAdditionalDatanodeRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.DatanodeInfoProto> existingList = req.getExistingsList();
      List<String> existingStorageIDsList = req.getExistingStorageUuidsList();
      List<HdfsProtos.DatanodeInfoProto> excludesList = req.getExcludesList();
      LocatedBlock result = server.getAdditionalDatanode(req.getSrc(),
          req.getFileId(), PBHelperClient.convert(req.getBlk()),
          PBHelperClient.convert(existingList.toArray(
              new HdfsProtos.DatanodeInfoProto[existingList.size()])),
          existingStorageIDsList.toArray(
              new String[existingStorageIDsList.size()]),
          PBHelperClient.convert(excludesList.toArray(
              new HdfsProtos.DatanodeInfoProto[excludesList.size()])),
          req.getNumAdditionalNodes(), req.getClientName());
      return result;
    }, result -> GetAdditionalDatanodeResponseProto.newBuilder()
        .setBlock(
            PBHelperClient.convertLocatedBlock(result))
        .build());
    return null;
  }

  @Override
  public CompleteResponseProto complete(
      RpcController controller,
      CompleteRequestProto req) {
    asyncRouterServer(() -> {
      boolean result =
          server.complete(req.getSrc(), req.getClientName(),
              req.hasLast() ? PBHelperClient.convert(req.getLast()) : null,
              req.hasFileId() ? req.getFileId() : HdfsConstants.GRANDFATHER_INODE_ID);
      return result;
    }, result -> CompleteResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public ReportBadBlocksResponseProto reportBadBlocks(
      RpcController controller,
      ReportBadBlocksRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.LocatedBlockProto> bl = req.getBlocksList();
      server.reportBadBlocks(PBHelperClient.convertLocatedBlocks(
          bl.toArray(new HdfsProtos.LocatedBlockProto[bl.size()])));
      return null;
    }, result -> VOID_REP_BAD_BLOCK_RESPONSE);
    return null;
  }

  @Override
  public ConcatResponseProto concat(
      RpcController controller,
      ConcatRequestProto req) {
    asyncRouterServer(() -> {
      List<String> srcs = req.getSrcsList();
      server.concat(req.getTrg(), srcs.toArray(new String[srcs.size()]));
      return null;
    }, result -> VOID_CONCAT_RESPONSE);
    return null;
  }

  @Override
  public RenameResponseProto rename(
      RpcController controller,
      RenameRequestProto req) {
    asyncRouterServer(() -> {
      return server.rename(req.getSrc(), req.getDst());
    }, result -> RenameResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public Rename2ResponseProto rename2(
      RpcController controller,
      Rename2RequestProto req) {
    asyncRouterServer(() -> {
      // resolve rename options
      ArrayList<Options.Rename> optionList = new ArrayList<Options.Rename>();
      if (req.getOverwriteDest()) {
        optionList.add(Options.Rename.OVERWRITE);
      }
      if (req.hasMoveToTrash() && req.getMoveToTrash()) {
        optionList.add(Options.Rename.TO_TRASH);
      }

      if (optionList.isEmpty()) {
        optionList.add(Options.Rename.NONE);
      }
      server.rename2(req.getSrc(), req.getDst(),
          optionList.toArray(new Options.Rename[optionList.size()]));
      return null;
    }, result -> VOID_RENAME2_RESPONSE);
    return null;
  }

  @Override
  public TruncateResponseProto truncate(
      RpcController controller,
      TruncateRequestProto req) {
    asyncRouterServer(() -> server.truncate(req.getSrc(), req.getNewLength(),
        req.getClientName()),
        result -> TruncateResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public DeleteResponseProto delete(
      RpcController controller,
      DeleteRequestProto req) {
    asyncRouterServer(() -> server.delete(req.getSrc(), req.getRecursive()),
        result -> DeleteResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public MkdirsResponseProto mkdirs(
      RpcController controller,
      MkdirsRequestProto req) {
    asyncRouterServer(() -> {
      FsPermission masked = req.hasUnmasked() ?
          FsCreateModes.create(PBHelperClient.convert(req.getMasked()),
              PBHelperClient.convert(req.getUnmasked())) :
          PBHelperClient.convert(req.getMasked());
      boolean result = server.mkdirs(req.getSrc(), masked,
          req.getCreateParent());
      return result;
    }, result -> MkdirsResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public GetListingResponseProto getListing(
      RpcController controller,
      GetListingRequestProto req) {
    asyncRouterServer(() -> {
      DirectoryListing result = server.getListing(
          req.getSrc(), req.getStartAfter().toByteArray(),
          req.getNeedLocation());
      return result;
    }, result -> {
        if (result != null) {
          return GetListingResponseProto.newBuilder().setDirList(
                  PBHelperClient.convert(result)).build();
        } else {
          return VOID_GETLISTING_RESPONSE;
        }
      });
    return null;
  }

  @Override
  public GetBatchedListingResponseProto getBatchedListing(
      RpcController controller,
      GetBatchedListingRequestProto request) {
    asyncRouterServer(() -> {
      BatchedDirectoryListing result = server.getBatchedListing(
          request.getPathsList().toArray(new String[]{}),
          request.getStartAfter().toByteArray(),
          request.getNeedLocation());
      return result;
    }, result -> {
        if (result != null) {
          GetBatchedListingResponseProto.Builder builder =
              GetBatchedListingResponseProto.newBuilder();
          for (HdfsPartialListing partialListing : result.getListings()) {
            HdfsProtos.BatchedDirectoryListingProto.Builder listingBuilder =
                HdfsProtos.BatchedDirectoryListingProto.newBuilder();
            if (partialListing.getException() != null) {
              RemoteException ex = partialListing.getException();
              HdfsProtos.RemoteExceptionProto.Builder rexBuilder =
                  HdfsProtos.RemoteExceptionProto.newBuilder();
              rexBuilder.setClassName(ex.getClassName());
              if (ex.getMessage() != null) {
                rexBuilder.setMessage(ex.getMessage());
              }
              listingBuilder.setException(rexBuilder.build());
            } else {
              for (HdfsFileStatus f : partialListing.getPartialListing()) {
                listingBuilder.addPartialListing(PBHelperClient.convert(f));
              }
            }
            listingBuilder.setParentIdx(partialListing.getParentIdx());
            builder.addListings(listingBuilder);
          }
          builder.setHasMore(result.hasMore());
          builder.setStartAfter(ByteString.copyFrom(result.getStartAfter()));
          return builder.build();
        } else {
          return VOID_GETBATCHEDLISTING_RESPONSE;
        }
      });
    return null;
  }

  @Override
  public RenewLeaseResponseProto renewLease(
      RpcController controller,
      RenewLeaseRequestProto req) {
    asyncRouterServer(() -> {
      server.renewLease(req.getClientName(), req.getNamespacesList());
      return null;
    }, result -> VOID_RENEWLEASE_RESPONSE);
    return null;
  }

  @Override
  public RecoverLeaseResponseProto recoverLease(
      RpcController controller,
      RecoverLeaseRequestProto req) {
    asyncRouterServer(() -> server.recoverLease(req.getSrc(), req.getClientName()),
        result -> RecoverLeaseResponseProto.newBuilder()
            .setResult(result).build());
    return null;
  }

  @Override
  public RestoreFailedStorageResponseProto restoreFailedStorage(
      RpcController controller, RestoreFailedStorageRequestProto req) {
    asyncRouterServer(() -> server.restoreFailedStorage(req.getArg()),
        result -> RestoreFailedStorageResponseProto.newBuilder().setResult(result)
            .build());
    return null;
  }

  @Override
  public GetFsStatsResponseProto getFsStats(
      RpcController controller,
      GetFsStatusRequestProto req) {
    asyncRouterServer(server::getStats, PBHelperClient::convert);
    return null;
  }

  @Override
  public GetFsReplicatedBlockStatsResponseProto getFsReplicatedBlockStats(
      RpcController controller, GetFsReplicatedBlockStatsRequestProto request) {
    asyncRouterServer(server::getReplicatedBlockStats, PBHelperClient::convert);
    return null;
  }

  @Override
  public GetFsECBlockGroupStatsResponseProto getFsECBlockGroupStats(
      RpcController controller, GetFsECBlockGroupStatsRequestProto request) {
    asyncRouterServer(server::getECBlockGroupStats, PBHelperClient::convert);
    return null;
  }

  @Override
  public GetDatanodeReportResponseProto getDatanodeReport(
      RpcController controller, GetDatanodeReportRequestProto req) {
    asyncRouterServer(() -> server.getDatanodeReport(PBHelperClient.convert(req.getType())),
        result -> {
          List<? extends HdfsProtos.DatanodeInfoProto> re = PBHelperClient.convert(result);
          return GetDatanodeReportResponseProto.newBuilder()
              .addAllDi(re).build();
        });
    return null;
  }

  @Override
  public GetDatanodeStorageReportResponseProto getDatanodeStorageReport(
      RpcController controller, GetDatanodeStorageReportRequestProto req) {
    asyncRouterServer(() -> server.getDatanodeStorageReport(PBHelperClient.convert(req.getType())),
        result -> {
          List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> reports =
              PBHelperClient.convertDatanodeStorageReports(result);
          return GetDatanodeStorageReportResponseProto.newBuilder()
              .addAllDatanodeStorageReports(reports)
              .build();
        });
    return null;
  }

  @Override
  public GetPreferredBlockSizeResponseProto getPreferredBlockSize(
      RpcController controller, GetPreferredBlockSizeRequestProto req) {
    asyncRouterServer(() -> server.getPreferredBlockSize(req.getFilename()),
        result -> GetPreferredBlockSizeResponseProto.newBuilder().setBsize(result)
            .build());
    return null;
  }

  @Override
  public SetSafeModeResponseProto setSafeMode(
      RpcController controller,
      SetSafeModeRequestProto req) {
    asyncRouterServer(() -> server.setSafeMode(PBHelperClient.convert(req.getAction()),
        req.getChecked()),
        result -> SetSafeModeResponseProto.newBuilder().setResult(result).build());
    return null;
  }

  @Override
  public SaveNamespaceResponseProto saveNamespace(
      RpcController controller,
      SaveNamespaceRequestProto req) {
    asyncRouterServer(() -> {
      final long timeWindow = req.hasTimeWindow() ? req.getTimeWindow() : 0;
      final long txGap = req.hasTxGap() ? req.getTxGap() : 0;
      return server.saveNamespace(timeWindow, txGap);
    }, result -> SaveNamespaceResponseProto.newBuilder().setSaved(result).build());
    return null;
  }

  @Override
  public RollEditsResponseProto rollEdits(
      RpcController controller,
      RollEditsRequestProto request) {
    asyncRouterServer(server::rollEdits,
        txid -> RollEditsResponseProto.newBuilder()
            .setNewSegmentTxId(txid)
            .build());
    return null;
  }


  @Override
  public RefreshNodesResponseProto refreshNodes(
      RpcController controller,
      RefreshNodesRequestProto req) {
    asyncRouterServer(() -> {
      server.refreshNodes();
      return null;
    }, result -> VOID_REFRESHNODES_RESPONSE);
    return null;
  }

  @Override
  public FinalizeUpgradeResponseProto finalizeUpgrade(
      RpcController controller,
      FinalizeUpgradeRequestProto req) {
    asyncRouterServer(() -> {
      server.finalizeUpgrade();
      return null;
    }, result -> VOID_REFRESHNODES_RESPONSE);
    return null;
  }

  @Override
  public UpgradeStatusResponseProto upgradeStatus(
      RpcController controller, UpgradeStatusRequestProto req) {
    asyncRouterServer(server::upgradeStatus,
        result -> {
          UpgradeStatusResponseProto.Builder b =
              UpgradeStatusResponseProto.newBuilder();
          b.setUpgradeFinalized(result);
          return b.build();
        });
    return null;
  }

  @Override
  public RollingUpgradeResponseProto rollingUpgrade(
      RpcController controller,
      RollingUpgradeRequestProto req) {
    asyncRouterServer(() ->
            server.rollingUpgrade(PBHelperClient.convert(req.getAction())),
        info -> {
          final RollingUpgradeResponseProto.Builder b =
              RollingUpgradeResponseProto.newBuilder();
          if (info != null) {
            b.setRollingUpgradeInfo(PBHelperClient.convert(info));
          }
          return b.build();
        });
    return null;
  }

  @Override
  public ListCorruptFileBlocksResponseProto listCorruptFileBlocks(
      RpcController controller, ListCorruptFileBlocksRequestProto req) {
    asyncRouterServer(() -> server.listCorruptFileBlocks(
        req.getPath(), req.hasCookie() ? req.getCookie(): null),
        result -> ListCorruptFileBlocksResponseProto.newBuilder()
            .setCorrupt(PBHelperClient.convert(result))
            .build());
    return null;
  }

  @Override
  public MetaSaveResponseProto metaSave(
      RpcController controller,
      MetaSaveRequestProto req) {
    asyncRouterServer(() -> {
      server.metaSave(req.getFilename());
      return null;
    }, result -> VOID_METASAVE_RESPONSE);
    return null;
  }

  @Override
  public GetFileInfoResponseProto getFileInfo(
      RpcController controller,
      GetFileInfoRequestProto req) {
    asyncRouterServer(() -> server.getFileInfo(req.getSrc()),
        result -> {
          if (result != null) {
            return GetFileInfoResponseProto.newBuilder().setFs(
                PBHelperClient.convert(result)).build();
          }
          return VOID_GETFILEINFO_RESPONSE;
        });
    return null;
  }

  @Override
  public GetLocatedFileInfoResponseProto getLocatedFileInfo(
      RpcController controller, GetLocatedFileInfoRequestProto req) {
    asyncRouterServer(() -> server.getLocatedFileInfo(req.getSrc(),
        req.getNeedBlockToken()),
        result -> {
          if (result != null) {
            return GetLocatedFileInfoResponseProto.newBuilder().setFs(
                PBHelperClient.convert(result)).build();
          }
          return VOID_GETLOCATEDFILEINFO_RESPONSE;
        });
    return null;
  }

  @Override
  public GetFileLinkInfoResponseProto getFileLinkInfo(
      RpcController controller,
      GetFileLinkInfoRequestProto req) {
    asyncRouterServer(() -> server.getFileLinkInfo(req.getSrc()),
        result -> {
          if (result != null) {
            return GetFileLinkInfoResponseProto.newBuilder().setFs(
                PBHelperClient.convert(result)).build();
          } else {
            return VOID_GETFILELINKINFO_RESPONSE;
          }
        });
    return null;
  }

  @Override
  public GetContentSummaryResponseProto getContentSummary(
      RpcController controller, GetContentSummaryRequestProto req) {
    asyncRouterServer(() -> server.getContentSummary(req.getPath()),
        result -> GetContentSummaryResponseProto.newBuilder()
            .setSummary(PBHelperClient.convert(result)).build());
    return null;
  }

  @Override
  public SetQuotaResponseProto setQuota(
      RpcController controller,
      SetQuotaRequestProto req) {
    asyncRouterServer(() -> {
      server.setQuota(req.getPath(), req.getNamespaceQuota(),
          req.getStoragespaceQuota(),
          req.hasStorageType() ?
              PBHelperClient.convertStorageType(req.getStorageType()): null);
      return null;
    }, result -> VOID_SETQUOTA_RESPONSE);
    return null;
  }

  @Override
  public FsyncResponseProto fsync(
      RpcController controller,
      FsyncRequestProto req) {
    asyncRouterServer(() -> {
      server.fsync(req.getSrc(), req.getFileId(),
          req.getClient(), req.getLastBlockLength());
      return null;
    }, result -> VOID_FSYNC_RESPONSE);
    return null;
  }

  @Override
  public SetTimesResponseProto setTimes(
      RpcController controller,
      SetTimesRequestProto req) {
    asyncRouterServer(() -> {
      server.setTimes(req.getSrc(), req.getMtime(), req.getAtime());
      return null;
    }, result -> VOID_SETTIMES_RESPONSE);
    return null;
  }

  @Override
  public CreateSymlinkResponseProto createSymlink(
      RpcController controller,
      CreateSymlinkRequestProto req) {
    asyncRouterServer(() -> {
      server.createSymlink(req.getTarget(), req.getLink(),
          PBHelperClient.convert(req.getDirPerm()), req.getCreateParent());
      return null;
    }, result -> VOID_CREATESYMLINK_RESPONSE);
    return null;
  }

  @Override
  public GetLinkTargetResponseProto getLinkTarget(
      RpcController controller,
      GetLinkTargetRequestProto req) {
    asyncRouterServer(() -> server.getLinkTarget(req.getPath()),
        result -> {
          GetLinkTargetResponseProto.Builder builder =
              GetLinkTargetResponseProto
                  .newBuilder();
          if (result != null) {
            builder.setTargetPath(result);
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public UpdateBlockForPipelineResponseProto updateBlockForPipeline(
      RpcController controller, UpdateBlockForPipelineRequestProto req) {
    asyncRouterServer(() -> server.updateBlockForPipeline(PBHelperClient.convert(req.getBlock()),
        req.getClientName()),
        result -> {
          HdfsProtos.LocatedBlockProto res = PBHelperClient.convertLocatedBlock(result);
          return UpdateBlockForPipelineResponseProto.newBuilder().setBlock(res)
              .build();
        });
    return null;
  }

  @Override
  public UpdatePipelineResponseProto updatePipeline(
      RpcController controller,
      UpdatePipelineRequestProto req) {
    asyncRouterServer(() -> {
      List<HdfsProtos.DatanodeIDProto> newNodes = req.getNewNodesList();
      List<String> newStorageIDs = req.getStorageIDsList();
      server.updatePipeline(req.getClientName(),
          PBHelperClient.convert(req.getOldBlock()),
          PBHelperClient.convert(req.getNewBlock()),
          PBHelperClient.convert(newNodes.toArray(new HdfsProtos.DatanodeIDProto[newNodes.size()])),
          newStorageIDs.toArray(new String[newStorageIDs.size()]));
      return null;
    }, result -> VOID_UPDATEPIPELINE_RESPONSE);
    return null;
  }

  @Override
  public GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller, GetDelegationTokenRequestProto req) {
    asyncRouterServer(() -> server
            .getDelegationToken(new Text(req.getRenewer())),
        token -> {
          GetDelegationTokenResponseProto.Builder rspBuilder =
              GetDelegationTokenResponseProto.newBuilder();
          if (token != null) {
            rspBuilder.setToken(PBHelperClient.convert(token));
          }
          return rspBuilder.build();
        });
    return null;
  }

  @Override
  public RenewDelegationTokenResponseProto renewDelegationToken(
      RpcController controller, RenewDelegationTokenRequestProto req) {
    asyncRouterServer(() -> server.renewDelegationToken(PBHelperClient
            .convertDelegationToken(req.getToken())),
        result -> RenewDelegationTokenResponseProto.newBuilder()
            .setNewExpiryTime(result).build());
    return null;
  }

  @Override
  public CancelDelegationTokenResponseProto cancelDelegationToken(
      RpcController controller, CancelDelegationTokenRequestProto req) {
    asyncRouterServer(() -> {
      server.cancelDelegationToken(PBHelperClient.convertDelegationToken(req
          .getToken()));
      return null;
    }, result -> VOID_CANCELDELEGATIONTOKEN_RESPONSE);
    return null;
  }

  @Override
  public SetBalancerBandwidthResponseProto setBalancerBandwidth(
      RpcController controller, SetBalancerBandwidthRequestProto req) {
    asyncRouterServer(() -> {
      server.setBalancerBandwidth(req.getBandwidth());
      return null;
    }, result -> VOID_SETBALANCERBANDWIDTH_RESPONSE);
    return null;
  }

  @Override
  public GetDataEncryptionKeyResponseProto getDataEncryptionKey(
      RpcController controller, GetDataEncryptionKeyRequestProto request) {
    asyncRouterServer(server::getDataEncryptionKey, encryptionKey -> {
      GetDataEncryptionKeyResponseProto.Builder builder =
          GetDataEncryptionKeyResponseProto.newBuilder();
      if (encryptionKey != null) {
        builder.setDataEncryptionKey(PBHelperClient.convert(encryptionKey));
      }
      return builder.build();
    });
    return null;
  }

  @Override
  public CreateSnapshotResponseProto createSnapshot(
      RpcController controller,
      CreateSnapshotRequestProto req) throws ServiceException {
    asyncRouterServer(() -> server.createSnapshot(req.getSnapshotRoot(),
        req.hasSnapshotName()? req.getSnapshotName(): null),
        snapshotPath -> {
          final CreateSnapshotResponseProto.Builder builder
              = CreateSnapshotResponseProto.newBuilder();
          if (snapshotPath != null) {
            builder.setSnapshotPath(snapshotPath);
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public DeleteSnapshotResponseProto deleteSnapshot(
      RpcController controller,
      DeleteSnapshotRequestProto req) {
    asyncRouterServer(() -> {
      server.deleteSnapshot(req.getSnapshotRoot(), req.getSnapshotName());
      return null;
    }, result -> VOID_DELETE_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public AllowSnapshotResponseProto allowSnapshot(
      RpcController controller,
      AllowSnapshotRequestProto req) {
    asyncRouterServer(() -> {
      server.allowSnapshot(req.getSnapshotRoot());
      return null;
    }, result -> VOID_ALLOW_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public DisallowSnapshotResponseProto disallowSnapshot(
      RpcController controller,
      DisallowSnapshotRequestProto req) {
    asyncRouterServer(() -> {
      server.disallowSnapshot(req.getSnapshotRoot());
      return null;
    }, result -> VOID_DISALLOW_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public RenameSnapshotResponseProto renameSnapshot(
      RpcController controller,
      RenameSnapshotRequestProto request) {
    asyncRouterServer(() -> {
      server.renameSnapshot(request.getSnapshotRoot(),
          request.getSnapshotOldName(), request.getSnapshotNewName());
      return null;
    }, result -> VOID_RENAME_SNAPSHOT_RESPONSE);
    return null;
  }

  @Override
  public GetSnapshottableDirListingResponseProto getSnapshottableDirListing(
      RpcController controller, GetSnapshottableDirListingRequestProto request) {
    asyncRouterServer(server::getSnapshottableDirListing,
        result -> {
          if (result != null) {
            return GetSnapshottableDirListingResponseProto.newBuilder().
                setSnapshottableDirList(PBHelperClient.convert(result)).build();
          } else {
            return NULL_GET_SNAPSHOTTABLE_DIR_LISTING_RESPONSE;
          }
        });
    return null;
  }

  @Override
  public GetSnapshotListingResponseProto getSnapshotListing(
      RpcController controller, GetSnapshotListingRequestProto request) {
    asyncRouterServer(() -> server
            .getSnapshotListing(request.getSnapshotRoot()),
        result -> {
          if (result != null) {
            return GetSnapshotListingResponseProto.newBuilder().
                setSnapshotList(PBHelperClient.convert(result)).build();
          } else {
            return NULL_GET_SNAPSHOT_LISTING_RESPONSE;
          }
        });
    return null;
  }

  @Override
  public GetSnapshotDiffReportResponseProto getSnapshotDiffReport(
      RpcController controller, GetSnapshotDiffReportRequestProto request) {
    asyncRouterServer(() -> server.getSnapshotDiffReport(
        request.getSnapshotRoot(), request.getFromSnapshot(),
        request.getToSnapshot()),
        report -> GetSnapshotDiffReportResponseProto.newBuilder()
            .setDiffReport(PBHelperClient.convert(report)).build());
    return null;
  }

  @Override
  public GetSnapshotDiffReportListingResponseProto getSnapshotDiffReportListing(
      RpcController controller,
      GetSnapshotDiffReportListingRequestProto request) {
    asyncRouterServer(() -> server
            .getSnapshotDiffReportListing(request.getSnapshotRoot(),
                request.getFromSnapshot(), request.getToSnapshot(),
                request.getCursor().getStartPath().toByteArray(),
                request.getCursor().getIndex()),
        report -> GetSnapshotDiffReportListingResponseProto.newBuilder()
            .setDiffReport(PBHelperClient.convert(report)).build());
    return null;
  }

  @Override
  public IsFileClosedResponseProto isFileClosed(
      RpcController controller, IsFileClosedRequestProto request) {
    asyncRouterServer(() -> server.isFileClosed(request.getSrc()),
        result -> IsFileClosedResponseProto.newBuilder()
            .setResult(result).build());
    return null;
  }

  @Override
  public AddCacheDirectiveResponseProto addCacheDirective(
      RpcController controller, AddCacheDirectiveRequestProto request) {
    asyncRouterServer(() -> server.addCacheDirective(
        PBHelperClient.convert(request.getInfo()),
        PBHelperClient.convertCacheFlags(request.getCacheFlags())),
        id -> AddCacheDirectiveResponseProto.newBuilder().
            setId(id).build());
    return null;
  }

  @Override
  public ModifyCacheDirectiveResponseProto modifyCacheDirective(
      RpcController controller, ModifyCacheDirectiveRequestProto request) {
    asyncRouterServer(() -> {
      server.modifyCacheDirective(
          PBHelperClient.convert(request.getInfo()),
          PBHelperClient.convertCacheFlags(request.getCacheFlags()));
      return null;
    }, result -> ModifyCacheDirectiveResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public RemoveCacheDirectiveResponseProto removeCacheDirective(
      RpcController controller,
      RemoveCacheDirectiveRequestProto request) {
    asyncRouterServer(() -> {
      server.removeCacheDirective(request.getId());
      return null;
    }, result -> RemoveCacheDirectiveResponseProto.
        newBuilder().build());
    return null;
  }

  @Override
  public ListCacheDirectivesResponseProto listCacheDirectives(
      RpcController controller, ListCacheDirectivesRequestProto request) {
    asyncRouterServer(() -> {
      CacheDirectiveInfo filter =
          PBHelperClient.convert(request.getFilter());
      return  server.listCacheDirectives(request.getPrevId(), filter);
    }, entries -> {
        ListCacheDirectivesResponseProto.Builder builder =
            ListCacheDirectivesResponseProto.newBuilder();
        builder.setHasMore(entries.hasMore());
        for (int i=0, n=entries.size(); i<n; i++) {
          builder.addElements(PBHelperClient.convert(entries.get(i)));
        }
        return builder.build();
      });
    return null;
  }

  @Override
  public AddCachePoolResponseProto addCachePool(
      RpcController controller,
      AddCachePoolRequestProto request) {
    asyncRouterServer(() -> {
      server.addCachePool(PBHelperClient.convert(request.getInfo()));
      return null;
    }, result -> AddCachePoolResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ModifyCachePoolResponseProto modifyCachePool(
      RpcController controller,
      ModifyCachePoolRequestProto request) {
    asyncRouterServer(() -> {
      server.modifyCachePool(PBHelperClient.convert(request.getInfo()));
      return null;
    }, result -> ModifyCachePoolResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public RemoveCachePoolResponseProto removeCachePool(
      RpcController controller,
      RemoveCachePoolRequestProto request) {
    asyncRouterServer(() -> {
      server.removeCachePool(request.getPoolName());
      return null;
    }, result -> RemoveCachePoolResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public ListCachePoolsResponseProto listCachePools(
      RpcController controller,
      ListCachePoolsRequestProto request) {
    asyncRouterServer(() -> server.listCachePools(request.getPrevPoolName()),
        entries -> {
          ListCachePoolsResponseProto.Builder responseBuilder =
              ListCachePoolsResponseProto.newBuilder();
          responseBuilder.setHasMore(entries.hasMore());
          for (int i=0, n=entries.size(); i<n; i++) {
            responseBuilder.addEntries(PBHelperClient.convert(entries.get(i)));
          }
          return responseBuilder.build();
        });
    return null;
  }

  @Override
  public ModifyAclEntriesResponseProto modifyAclEntries(
      RpcController controller, ModifyAclEntriesRequestProto req) {
    asyncRouterServer(() -> {
      server.modifyAclEntries(req.getSrc(), PBHelperClient.convertAclEntry(req.getAclSpecList()));
      return null;
    }, vo -> VOID_MODIFYACLENTRIES_RESPONSE);
    return null;
  }

  @Override
  public RemoveAclEntriesResponseProto removeAclEntries(
      RpcController controller, RemoveAclEntriesRequestProto req) {
    asyncRouterServer(() -> {
      server.removeAclEntries(req.getSrc(),
          PBHelperClient.convertAclEntry(req.getAclSpecList()));
      return null;
    }, vo -> VOID_REMOVEACLENTRIES_RESPONSE);
    return null;
  }

  @Override
  public RemoveDefaultAclResponseProto removeDefaultAcl(
      RpcController controller, RemoveDefaultAclRequestProto req) {
    asyncRouterServer(() -> {
      server.removeDefaultAcl(req.getSrc());
      return null;
    }, vo -> VOID_REMOVEDEFAULTACL_RESPONSE);
    return null;
  }

  @Override
  public RemoveAclResponseProto removeAcl(
      RpcController controller,
      RemoveAclRequestProto req) {
    asyncRouterServer(() -> {
      server.removeAcl(req.getSrc());
      return null;
    }, vo -> VOID_REMOVEACL_RESPONSE);
    return null;
  }

  @Override
  public SetAclResponseProto setAcl(
      RpcController controller,
      SetAclRequestProto req) {
    asyncRouterServer(() -> {
      server.setAcl(req.getSrc(), PBHelperClient.convertAclEntry(req.getAclSpecList()));
      return null;
    }, vo -> VOID_SETACL_RESPONSE);
    return null;
  }

  @Override
  public GetAclStatusResponseProto getAclStatus(
      RpcController controller,
      GetAclStatusRequestProto req) {
    asyncRouterServer(() -> server.getAclStatus(req.getSrc()),
        PBHelperClient::convert);
    return null;
  }

  @Override
  public CreateEncryptionZoneResponseProto createEncryptionZone(
      RpcController controller, CreateEncryptionZoneRequestProto req) {
    asyncRouterServer(() -> {
      server.createEncryptionZone(req.getSrc(), req.getKeyName());
      return null;
    }, vo -> CreateEncryptionZoneResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public GetEZForPathResponseProto getEZForPath(
      RpcController controller, GetEZForPathRequestProto req) {
    asyncRouterServer(() -> server.getEZForPath(req.getSrc()),
        ret -> {
          GetEZForPathResponseProto.Builder builder =
              GetEZForPathResponseProto.newBuilder();
          if (ret != null) {
            builder.setZone(PBHelperClient.convert(ret));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ListEncryptionZonesResponseProto listEncryptionZones(
      RpcController controller, ListEncryptionZonesRequestProto req) {
    asyncRouterServer(() -> server.listEncryptionZones(req.getId()),
        entries -> {
          ListEncryptionZonesResponseProto.Builder builder =
              ListEncryptionZonesResponseProto.newBuilder();
          builder.setHasMore(entries.hasMore());
          for (int i=0; i<entries.size(); i++) {
            builder.addZones(PBHelperClient.convert(entries.get(i)));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ReencryptEncryptionZoneResponseProto reencryptEncryptionZone(
      RpcController controller, ReencryptEncryptionZoneRequestProto req) {
    asyncRouterServer(() -> {
      server.reencryptEncryptionZone(req.getZone(),
          PBHelperClient.convert(req.getAction()));
      return null;
    }, vo -> ReencryptEncryptionZoneResponseProto.newBuilder().build());
    return null;
  }

  public ListReencryptionStatusResponseProto listReencryptionStatus(
      RpcController controller, ListReencryptionStatusRequestProto req) {
    asyncRouterServer(() -> server.listReencryptionStatus(req.getId()),
        entries -> {
          ListReencryptionStatusResponseProto.Builder builder =
              ListReencryptionStatusResponseProto.newBuilder();
          builder.setHasMore(entries.hasMore());
          for (int i=0; i<entries.size(); i++) {
            builder.addStatuses(PBHelperClient.convert(entries.get(i)));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public SetErasureCodingPolicyResponseProto setErasureCodingPolicy(
      RpcController controller, SetErasureCodingPolicyRequestProto req) {
    asyncRouterServer(() -> {
      String ecPolicyName = req.hasEcPolicyName() ?
          req.getEcPolicyName() : null;
      server.setErasureCodingPolicy(req.getSrc(), ecPolicyName);
      return null;
    }, vo -> SetErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public UnsetErasureCodingPolicyResponseProto unsetErasureCodingPolicy(
      RpcController controller, UnsetErasureCodingPolicyRequestProto req) {
    asyncRouterServer(() -> {
      server.unsetErasureCodingPolicy(req.getSrc());
      return null;
    }, vo -> UnsetErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public GetECTopologyResultForPoliciesResponseProto getECTopologyResultForPolicies(
      RpcController controller, GetECTopologyResultForPoliciesRequestProto req) {
    asyncRouterServer(() -> {
      ProtocolStringList policies = req.getPoliciesList();
      return server.getECTopologyResultForPolicies(
          policies.toArray(policies.toArray(new String[policies.size()])));
    }, result -> {
        GetECTopologyResultForPoliciesResponseProto.Builder builder =
            GetECTopologyResultForPoliciesResponseProto.newBuilder();
        builder
            .setResponse(PBHelperClient.convertECTopologyVerifierResult(result));
        return builder.build();
      });
    return null;
  }

  @Override
  public SetXAttrResponseProto setXAttr(
      RpcController controller,
      SetXAttrRequestProto req) {
    asyncRouterServer(() -> {
      server.setXAttr(req.getSrc(), PBHelperClient.convertXAttr(req.getXAttr()),
          PBHelperClient.convert(req.getFlag()));
      return null;
    }, vo -> VOID_SETXATTR_RESPONSE);
    return null;
  }

  @Override
  public GetXAttrsResponseProto getXAttrs(
      RpcController controller,
      GetXAttrsRequestProto req) {
    asyncRouterServer(() -> server.getXAttrs(req.getSrc(),
        PBHelperClient.convertXAttrs(req.getXAttrsList())),
        PBHelperClient::convertXAttrsResponse);
    return null;
  }

  @Override
  public ListXAttrsResponseProto listXAttrs(
      RpcController controller,
      ListXAttrsRequestProto req) {
    asyncRouterServer(() -> server.listXAttrs(req.getSrc()),
        PBHelperClient::convertListXAttrsResponse);
    return null;
  }

  @Override
  public RemoveXAttrResponseProto removeXAttr(
      RpcController controller,
      RemoveXAttrRequestProto req) {
    asyncRouterServer(() -> {
      server.removeXAttr(req.getSrc(), PBHelperClient.convertXAttr(req.getXAttr()));
      return null;
    }, vo -> VOID_REMOVEXATTR_RESPONSE);
    return null;
  }

  @Override
  public CheckAccessResponseProto checkAccess(
      RpcController controller,
      CheckAccessRequestProto req) {
    asyncRouterServer(() -> {
      server.checkAccess(req.getPath(), PBHelperClient.convert(req.getMode()));
      return null;
    }, vo -> VOID_CHECKACCESS_RESPONSE);
    return null;
  }

  @Override
  public SetStoragePolicyResponseProto setStoragePolicy(
      RpcController controller, SetStoragePolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.setStoragePolicy(request.getSrc(), request.getPolicyName());
      return null;
    }, vo -> VOID_SET_STORAGE_POLICY_RESPONSE);
    return null;
  }

  @Override
  public UnsetStoragePolicyResponseProto unsetStoragePolicy(
      RpcController controller, UnsetStoragePolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.unsetStoragePolicy(request.getSrc());
      return null;
    }, vo -> VOID_UNSET_STORAGE_POLICY_RESPONSE);
    return null;
  }

  @Override
  public GetStoragePolicyResponseProto getStoragePolicy(
      RpcController controller, GetStoragePolicyRequestProto request) {
    asyncRouterServer(() -> server.getStoragePolicy(request.getPath()),
        result -> {
          HdfsProtos.BlockStoragePolicyProto policy = PBHelperClient.convert(result);
          return GetStoragePolicyResponseProto.newBuilder()
              .setStoragePolicy(policy).build();
        });
    return null;
  }

  @Override
  public GetStoragePoliciesResponseProto getStoragePolicies(
      RpcController controller, GetStoragePoliciesRequestProto request) {
    asyncRouterServer(server::getStoragePolicies,
        policies -> {
          GetStoragePoliciesResponseProto.Builder builder =
              GetStoragePoliciesResponseProto.newBuilder();
          if (policies == null) {
            return builder.build();
          }
          for (BlockStoragePolicy policy : policies) {
            builder.addPolicies(PBHelperClient.convert(policy));
          }
          return builder.build();
        });
    return null;
  }

  public GetCurrentEditLogTxidResponseProto getCurrentEditLogTxid(
      RpcController controller,
      GetCurrentEditLogTxidRequestProto req) {
    asyncRouterServer(server::getCurrentEditLogTxid,
        result -> GetCurrentEditLogTxidResponseProto.newBuilder()
            .setTxid(result).build());
    return null;
  }

  @Override
  public GetEditsFromTxidResponseProto getEditsFromTxid(
      RpcController controller,
      GetEditsFromTxidRequestProto req) {
    asyncRouterServer(() -> server.getEditsFromTxid(req.getTxid()),
        PBHelperClient::convertEditsResponse);
    return null;
  }

  @Override
  public GetErasureCodingPoliciesResponseProto getErasureCodingPolicies(
      RpcController controller,
      GetErasureCodingPoliciesRequestProto request) {
    asyncRouterServer(server::getErasureCodingPolicies,
        ecpInfos -> {
          GetErasureCodingPoliciesResponseProto.Builder resBuilder =
              GetErasureCodingPoliciesResponseProto
                  .newBuilder();
          for (ErasureCodingPolicyInfo info : ecpInfos) {
            resBuilder.addEcPolicies(
                PBHelperClient.convertErasureCodingPolicy(info));
          }
          return resBuilder.build();
        });
    return null;
  }

  @Override
  public GetErasureCodingCodecsResponseProto getErasureCodingCodecs(
      RpcController controller, GetErasureCodingCodecsRequestProto request) {
    asyncRouterServer(server::getErasureCodingCodecs,
        codecs -> {
          GetErasureCodingCodecsResponseProto.Builder resBuilder =
              GetErasureCodingCodecsResponseProto.newBuilder();
          for (Map.Entry<String, String> codec : codecs.entrySet()) {
            resBuilder.addCodec(
                PBHelperClient.convertErasureCodingCodec(
                    codec.getKey(), codec.getValue()));
          }
          return resBuilder.build();
        });
    return null;
  }

  @Override
  public AddErasureCodingPoliciesResponseProto addErasureCodingPolicies(
      RpcController controller, AddErasureCodingPoliciesRequestProto request) {
    asyncRouterServer(() -> {
      ErasureCodingPolicy[] policies = request.getEcPoliciesList().stream()
          .map(PBHelperClient::convertErasureCodingPolicy)
          .toArray(ErasureCodingPolicy[]::new);
      return server
          .addErasureCodingPolicies(policies);
    }, result -> {
        List<HdfsProtos.AddErasureCodingPolicyResponseProto> responseProtos =
            Arrays.stream(result)
                .map(PBHelperClient::convertAddErasureCodingPolicyResponse)
                .collect(Collectors.toList());
        AddErasureCodingPoliciesResponseProto response =
            AddErasureCodingPoliciesResponseProto.newBuilder()
                .addAllResponses(responseProtos).build();
        return response;
      });
    return null;
  }

  @Override
  public RemoveErasureCodingPolicyResponseProto removeErasureCodingPolicy(
      RpcController controller, RemoveErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.removeErasureCodingPolicy(request.getEcPolicyName());
      return null;
    }, vo -> RemoveErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public EnableErasureCodingPolicyResponseProto enableErasureCodingPolicy(
      RpcController controller, EnableErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.enableErasureCodingPolicy(request.getEcPolicyName());
      return null;
    }, vo -> EnableErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public DisableErasureCodingPolicyResponseProto disableErasureCodingPolicy(
      RpcController controller, DisableErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.disableErasureCodingPolicy(request.getEcPolicyName());
      return null;
    }, vo -> DisableErasureCodingPolicyResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public GetErasureCodingPolicyResponseProto getErasureCodingPolicy(
      RpcController controller,
      GetErasureCodingPolicyRequestProto request) {
    asyncRouterServer(() -> server.getErasureCodingPolicy(request.getSrc()),
        ecPolicy -> {
          GetErasureCodingPolicyResponseProto.Builder builder =
              GetErasureCodingPolicyResponseProto.newBuilder();
          if (ecPolicy != null) {
            builder.setEcPolicy(PBHelperClient.convertErasureCodingPolicy(ecPolicy));
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public GetQuotaUsageResponseProto getQuotaUsage(
      RpcController controller, GetQuotaUsageRequestProto req) {
    asyncRouterServer(() -> server.getQuotaUsage(req.getPath()),
        result -> GetQuotaUsageResponseProto.newBuilder()
            .setUsage(PBHelperClient.convert(result)).build());
    return null;
  }

  @Override
  public ListOpenFilesResponseProto listOpenFiles(
      RpcController controller,
      ListOpenFilesRequestProto req) {
    asyncRouterServer(() -> {
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes =
          PBHelperClient.convertOpenFileTypes(req.getTypesList());
      return server.listOpenFiles(req.getId(),
          openFilesTypes, req.getPath());
    }, entries -> {
        ListOpenFilesResponseProto.Builder builder =
            ListOpenFilesResponseProto.newBuilder();
        builder.setHasMore(entries.hasMore());
        for (int i = 0; i < entries.size(); i++) {
          builder.addEntries(PBHelperClient.convert(entries.get(i)));
        }
        builder.addAllTypes(req.getTypesList());
        return builder.build();
      });
    return null;
  }

  @Override
  public MsyncResponseProto msync(RpcController controller, MsyncRequestProto req) {
    asyncRouterServer(() -> {
      server.msync();
      return null;
    }, vo -> MsyncResponseProto.newBuilder().build());
    return null;
  }

  @Override
  public SatisfyStoragePolicyResponseProto satisfyStoragePolicy(
      RpcController controller, SatisfyStoragePolicyRequestProto request) {
    asyncRouterServer(() -> {
      server.satisfyStoragePolicy(request.getSrc());
      return null;
    }, vo -> VOID_SATISFYSTORAGEPOLICY_RESPONSE);
    return null;
  }

  @Override
  public HAServiceStateResponseProto getHAServiceState(
      RpcController controller,
      HAServiceStateRequestProto request) {
    asyncRouterServer(server::getHAServiceState,
        state -> {
          HAServiceProtocolProtos.HAServiceStateProto retState;
          switch (state) {
            case ACTIVE:
              retState = HAServiceProtocolProtos.HAServiceStateProto.ACTIVE;
              break;
            case STANDBY:
              retState = HAServiceProtocolProtos.HAServiceStateProto.STANDBY;
              break;
            case OBSERVER:
              retState = HAServiceProtocolProtos.HAServiceStateProto.OBSERVER;
              break;
            case INITIALIZING:
            default:
              retState = HAServiceProtocolProtos.HAServiceStateProto.INITIALIZING;
              break;
          }
          HAServiceStateResponseProto.Builder builder =
              HAServiceStateResponseProto.newBuilder();
          builder.setState(retState);
          return builder.build();
        });
    return null;
  }

  @Override
  public GetSlowDatanodeReportResponseProto getSlowDatanodeReport(
      RpcController controller,
      GetSlowDatanodeReportRequestProto request) {
    asyncRouterServer(server::getSlowDatanodeReport,
        res -> {
          List<? extends HdfsProtos.DatanodeInfoProto> result =
              PBHelperClient.convert(res);
          return GetSlowDatanodeReportResponseProto.newBuilder()
              .addAllDatanodeInfoProto(result)
              .build();
        });
    return null;
  }

  @Override
  public GetEnclosingRootResponseProto getEnclosingRoot(
      RpcController controller, GetEnclosingRootRequestProto req) {
    asyncRouterServer(() -> server.getEnclosingRoot(req.getFilename()),
        enclosingRootPath -> ClientNamenodeProtocolProtos
            .GetEnclosingRootResponseProto.newBuilder()
            .setEnclosingRootPath(enclosingRootPath.toUri().toString())
            .build());
    return null;
  }
}
