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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos
    .EncryptionZoneProto;
import static org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CipherSuiteProto;
import static org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CryptoProtocolVersionProto;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.AclEntryScopeProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.AclEntryTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.FsActionProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveStatsProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheFlagProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolStatsProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateFlagProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DatanodeReportTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DatanodeStorageReportProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeActionProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SafeModeActionProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmIdProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmSlotProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BalancerBandwidthCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockIdCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockRecoveryCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.FinalizeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.KeyUpdateCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.NNHAStatusHeartbeatProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.VolumeFailureSummaryProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportContextProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockStoragePolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlocksWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CheckpointCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CheckpointSignatureProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CipherOptionProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ContentSummaryProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CorruptFileBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DataEncryptionKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto.AdminState;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfosProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeLocalInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeStorageProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeStorageProto.StorageState;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExportedBlockKeysProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.FsPermissionProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.FsServerDefaultsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto.FileType;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RecoveringBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogManifestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReplicaStateProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RollingUpgradeStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotDiffReportEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotDiffReportProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshottableDirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshottableDirectoryStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageReportProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypesProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageUuidsProto;
import org.apache.hadoop.hdfs.protocol.proto.InotifyProtos;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrProto.XAttrNamespaceProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrSetFlagProto;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockIdCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.hdfs.util.ExactSizeInputStream;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Shorts;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;

/**
 * Utilities for converting protobuf classes to and from implementation classes
 * and other helper utilities to help in dealing with protobuf.
 * 
 * Note that when converting from an internal type to protobuf type, the
 * converter never return null for protobuf type. The check for internal type
 * being null must be done before calling the convert() method.
 */
public class PBHelper {
  private static final RegisterCommandProto REG_CMD_PROTO = 
      RegisterCommandProto.newBuilder().build();
  private static final RegisterCommand REG_CMD = new RegisterCommand();

  private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES =
      AclEntryScope.values();
  private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES =
      AclEntryType.values();
  private static final FsAction[] FSACTION_VALUES =
      FsAction.values();
  private static final XAttr.NameSpace[] XATTR_NAMESPACE_VALUES = 
      XAttr.NameSpace.values();

  private PBHelper() {
    /** Hidden constructor */
  }

  public static ByteString getByteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  private static <T extends Enum<T>, U extends Enum<U>> U castEnum(T from, U[] to) {
    return to[from.ordinal()];
  }

  public static NamenodeRole convert(NamenodeRoleProto role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRole.NAMENODE;
    case BACKUP:
      return NamenodeRole.BACKUP;
    case CHECKPOINT:
      return NamenodeRole.CHECKPOINT;
    }
    return null;
  }

  public static NamenodeRoleProto convert(NamenodeRole role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRoleProto.NAMENODE;
    case BACKUP:
      return NamenodeRoleProto.BACKUP;
    case CHECKPOINT:
      return NamenodeRoleProto.CHECKPOINT;
    }
    return null;
  }

  public static BlockStoragePolicy[] convertStoragePolicies(
      List<BlockStoragePolicyProto> policyProtos) {
    if (policyProtos == null || policyProtos.size() == 0) {
      return new BlockStoragePolicy[0];
    }
    BlockStoragePolicy[] policies = new BlockStoragePolicy[policyProtos.size()];
    int i = 0;
    for (BlockStoragePolicyProto proto : policyProtos) {
      policies[i++] = convert(proto);
    }
    return policies;
  }

  public static BlockStoragePolicy convert(BlockStoragePolicyProto proto) {
    List<StorageTypeProto> cList = proto.getCreationPolicy()
        .getStorageTypesList();
    StorageType[] creationTypes = convertStorageTypes(cList, cList.size());
    List<StorageTypeProto> cfList = proto.hasCreationFallbackPolicy() ? proto
        .getCreationFallbackPolicy().getStorageTypesList() : null;
    StorageType[] creationFallbackTypes = cfList == null ? StorageType
        .EMPTY_ARRAY : convertStorageTypes(cfList, cfList.size());
    List<StorageTypeProto> rfList = proto.hasReplicationFallbackPolicy() ?
        proto.getReplicationFallbackPolicy().getStorageTypesList() : null;
    StorageType[] replicationFallbackTypes = rfList == null ? StorageType
        .EMPTY_ARRAY : convertStorageTypes(rfList, rfList.size());
    return new BlockStoragePolicy((byte) proto.getPolicyId(), proto.getName(),
        creationTypes, creationFallbackTypes, replicationFallbackTypes);
  }

  public static BlockStoragePolicyProto convert(BlockStoragePolicy policy) {
    BlockStoragePolicyProto.Builder builder = BlockStoragePolicyProto
        .newBuilder().setPolicyId(policy.getId()).setName(policy.getName());
    // creation storage types
    StorageTypesProto creationProto = convert(policy.getStorageTypes());
    Preconditions.checkArgument(creationProto != null);
    builder.setCreationPolicy(creationProto);
    // creation fallback
    StorageTypesProto creationFallbackProto = convert(
        policy.getCreationFallbacks());
    if (creationFallbackProto != null) {
      builder.setCreationFallbackPolicy(creationFallbackProto);
    }
    // replication fallback
    StorageTypesProto replicationFallbackProto = convert(
        policy.getReplicationFallbacks());
    if (replicationFallbackProto != null) {
      builder.setReplicationFallbackPolicy(replicationFallbackProto);
    }
    return builder.build();
  }

  public static StorageTypesProto convert(StorageType[] types) {
    if (types == null || types.length == 0) {
      return null;
    }
    List<StorageTypeProto> list = convertStorageTypes(types);
    return StorageTypesProto.newBuilder().addAllStorageTypes(list).build();
  }

  public static StorageInfoProto convert(StorageInfo info) {
    return StorageInfoProto.newBuilder().setClusterID(info.getClusterID())
        .setCTime(info.getCTime()).setLayoutVersion(info.getLayoutVersion())
        .setNamespceID(info.getNamespaceID()).build();
  }

  public static StorageInfo convert(StorageInfoProto info, NodeType type) {
    return new StorageInfo(info.getLayoutVersion(), info.getNamespceID(),
        info.getClusterID(), info.getCTime(), type);
  }

  public static NamenodeRegistrationProto convert(NamenodeRegistration reg) {
    return NamenodeRegistrationProto.newBuilder()
        .setHttpAddress(reg.getHttpAddress()).setRole(convert(reg.getRole()))
        .setRpcAddress(reg.getAddress())
        .setStorageInfo(convert((StorageInfo) reg)).build();
  }

  public static NamenodeRegistration convert(NamenodeRegistrationProto reg) {
    StorageInfo si = convert(reg.getStorageInfo(), NodeType.NAME_NODE);
    return new NamenodeRegistration(reg.getRpcAddress(), reg.getHttpAddress(),
        si, convert(reg.getRole()));
  }

  // DatanodeId
  public static DatanodeID convert(DatanodeIDProto dn) {
    return new DatanodeID(dn.getIpAddr(), dn.getHostName(), dn.getDatanodeUuid(),
        dn.getXferPort(), dn.getInfoPort(), dn.hasInfoSecurePort() ? dn
        .getInfoSecurePort() : 0, dn.getIpcPort());
  }

  public static DatanodeIDProto convert(DatanodeID dn) {
    // For wire compatibility with older versions we transmit the StorageID
    // which is the same as the DatanodeUuid. Since StorageID is a required
    // field we pass the empty string if the DatanodeUuid is not yet known.
    return DatanodeIDProto.newBuilder()
        .setIpAddr(dn.getIpAddr())
        .setHostName(dn.getHostName())
        .setXferPort(dn.getXferPort())
        .setDatanodeUuid(dn.getDatanodeUuid() != null ? dn.getDatanodeUuid() : "")
        .setInfoPort(dn.getInfoPort())
        .setInfoSecurePort(dn.getInfoSecurePort())
        .setIpcPort(dn.getIpcPort()).build();
  }

  // Arrays of DatanodeId
  public static DatanodeIDProto[] convert(DatanodeID[] did) {
    if (did == null)
      return null;
    final int len = did.length;
    DatanodeIDProto[] result = new DatanodeIDProto[len];
    for (int i = 0; i < len; ++i) {
      result[i] = convert(did[i]);
    }
    return result;
  }
  
  public static DatanodeID[] convert(DatanodeIDProto[] did) {
    if (did == null) return null;
    final int len = did.length;
    DatanodeID[] result = new DatanodeID[len];
    for (int i = 0; i < len; ++i) {
      result[i] = convert(did[i]);
    }
    return result;
  }
  
  // Block
  public static BlockProto convert(Block b) {
    return BlockProto.newBuilder().setBlockId(b.getBlockId())
        .setGenStamp(b.getGenerationStamp()).setNumBytes(b.getNumBytes())
        .build();
  }

  public static Block convert(BlockProto b) {
    return new Block(b.getBlockId(), b.getNumBytes(), b.getGenStamp());
  }

  public static BlockWithLocationsProto convert(BlockWithLocations blk) {
    return BlockWithLocationsProto.newBuilder()
        .setBlock(convert(blk.getBlock()))
        .addAllDatanodeUuids(Arrays.asList(blk.getDatanodeUuids()))
        .addAllStorageUuids(Arrays.asList(blk.getStorageIDs()))
        .addAllStorageTypes(convertStorageTypes(blk.getStorageTypes()))
        .build();
  }

  public static BlockWithLocations convert(BlockWithLocationsProto b) {
    final List<String> datanodeUuids = b.getDatanodeUuidsList();
    final List<String> storageUuids = b.getStorageUuidsList();
    final List<StorageTypeProto> storageTypes = b.getStorageTypesList();
    return new BlockWithLocations(convert(b.getBlock()),
        datanodeUuids.toArray(new String[datanodeUuids.size()]),
        storageUuids.toArray(new String[storageUuids.size()]),
        convertStorageTypes(storageTypes, storageUuids.size()));
  }

  public static BlocksWithLocationsProto convert(BlocksWithLocations blks) {
    BlocksWithLocationsProto.Builder builder = BlocksWithLocationsProto
        .newBuilder();
    for (BlockWithLocations b : blks.getBlocks()) {
      builder.addBlocks(convert(b));
    }
    return builder.build();
  }

  public static BlocksWithLocations convert(BlocksWithLocationsProto blocks) {
    List<BlockWithLocationsProto> b = blocks.getBlocksList();
    BlockWithLocations[] ret = new BlockWithLocations[b.size()];
    int i = 0;
    for (BlockWithLocationsProto entry : b) {
      ret[i++] = convert(entry);
    }
    return new BlocksWithLocations(ret);
  }

  public static BlockKeyProto convert(BlockKey key) {
    byte[] encodedKey = key.getEncodedKey();
    ByteString keyBytes = ByteString.copyFrom(encodedKey == null ? 
        DFSUtil.EMPTY_BYTES : encodedKey);
    return BlockKeyProto.newBuilder().setKeyId(key.getKeyId())
        .setKeyBytes(keyBytes).setExpiryDate(key.getExpiryDate()).build();
  }

  public static BlockKey convert(BlockKeyProto k) {
    return new BlockKey(k.getKeyId(), k.getExpiryDate(), k.getKeyBytes()
        .toByteArray());
  }

  public static ExportedBlockKeysProto convert(ExportedBlockKeys keys) {
    ExportedBlockKeysProto.Builder builder = ExportedBlockKeysProto
        .newBuilder();
    builder.setIsBlockTokenEnabled(keys.isBlockTokenEnabled())
        .setKeyUpdateInterval(keys.getKeyUpdateInterval())
        .setTokenLifeTime(keys.getTokenLifetime())
        .setCurrentKey(convert(keys.getCurrentKey()));
    for (BlockKey k : keys.getAllKeys()) {
      builder.addAllKeys(convert(k));
    }
    return builder.build();
  }

  public static ExportedBlockKeys convert(ExportedBlockKeysProto keys) {
    return new ExportedBlockKeys(keys.getIsBlockTokenEnabled(),
        keys.getKeyUpdateInterval(), keys.getTokenLifeTime(),
        convert(keys.getCurrentKey()), convertBlockKeys(keys.getAllKeysList()));
  }

  public static CheckpointSignatureProto convert(CheckpointSignature s) {
    return CheckpointSignatureProto.newBuilder()
        .setBlockPoolId(s.getBlockpoolID())
        .setCurSegmentTxId(s.getCurSegmentTxId())
        .setMostRecentCheckpointTxId(s.getMostRecentCheckpointTxId())
        .setStorageInfo(PBHelper.convert((StorageInfo) s)).build();
  }

  public static CheckpointSignature convert(CheckpointSignatureProto s) {
    StorageInfo si = PBHelper.convert(s.getStorageInfo(), NodeType.NAME_NODE);
    return new CheckpointSignature(si, s.getBlockPoolId(),
        s.getMostRecentCheckpointTxId(), s.getCurSegmentTxId());
  }

  public static RemoteEditLogProto convert(RemoteEditLog log) {
    return RemoteEditLogProto.newBuilder()
        .setStartTxId(log.getStartTxId())
        .setEndTxId(log.getEndTxId())
        .setIsInProgress(log.isInProgress()).build();
  }

  public static RemoteEditLog convert(RemoteEditLogProto l) {
    return new RemoteEditLog(l.getStartTxId(), l.getEndTxId(),
        l.getIsInProgress());
  }

  public static RemoteEditLogManifestProto convert(
      RemoteEditLogManifest manifest) {
    RemoteEditLogManifestProto.Builder builder = RemoteEditLogManifestProto
        .newBuilder();
    for (RemoteEditLog log : manifest.getLogs()) {
      builder.addLogs(convert(log));
    }
    return builder.build();
  }

  public static RemoteEditLogManifest convert(
      RemoteEditLogManifestProto manifest) {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>(manifest
        .getLogsList().size());
    for (RemoteEditLogProto l : manifest.getLogsList()) {
      logs.add(convert(l));
    }
    return new RemoteEditLogManifest(logs);
  }

  public static CheckpointCommandProto convert(CheckpointCommand cmd) {
    return CheckpointCommandProto.newBuilder()
        .setSignature(convert(cmd.getSignature()))
        .setNeedToReturnImage(cmd.needToReturnImage()).build();
  }

  public static NamenodeCommandProto convert(NamenodeCommand cmd) {
    if (cmd instanceof CheckpointCommand) {
      return NamenodeCommandProto.newBuilder().setAction(cmd.getAction())
          .setType(NamenodeCommandProto.Type.CheckPointCommand)
          .setCheckpointCmd(convert((CheckpointCommand) cmd)).build();
    }
    return NamenodeCommandProto.newBuilder()
        .setType(NamenodeCommandProto.Type.NamenodeCommand)
        .setAction(cmd.getAction()).build();
  }

  public static BlockKey[] convertBlockKeys(List<BlockKeyProto> list) {
    BlockKey[] ret = new BlockKey[list.size()];
    int i = 0;
    for (BlockKeyProto k : list) {
      ret[i++] = convert(k);
    }
    return ret;
  }

  public static NamespaceInfo convert(NamespaceInfoProto info) {
    StorageInfoProto storage = info.getStorageInfo();
    return new NamespaceInfo(storage.getNamespceID(), storage.getClusterID(),
        info.getBlockPoolID(), storage.getCTime(), info.getBuildVersion(),
        info.getSoftwareVersion(), info.getCapabilities());
  }

  public static NamenodeCommand convert(NamenodeCommandProto cmd) {
    if (cmd == null) return null;
    switch (cmd.getType()) {
    case CheckPointCommand:
      CheckpointCommandProto chkPt = cmd.getCheckpointCmd();
      return new CheckpointCommand(PBHelper.convert(chkPt.getSignature()),
          chkPt.getNeedToReturnImage());
    default:
      return new NamenodeCommand(cmd.getAction());
    }
  }
  
  public static ExtendedBlock convert(ExtendedBlockProto eb) {
    if (eb == null) return null;
    return new ExtendedBlock( eb.getPoolId(),  eb.getBlockId(),   eb.getNumBytes(),
       eb.getGenerationStamp());
  }
  
  public static ExtendedBlockProto convert(final ExtendedBlock b) {
    if (b == null) return null;
   return ExtendedBlockProto.newBuilder().
      setPoolId(b.getBlockPoolId()).
      setBlockId(b.getBlockId()).
      setNumBytes(b.getNumBytes()).
      setGenerationStamp(b.getGenerationStamp()).
      build();
  }
  
  public static RecoveringBlockProto convert(RecoveringBlock b) {
    if (b == null) {
      return null;
    }
    LocatedBlockProto lb = PBHelper.convert((LocatedBlock)b);
    RecoveringBlockProto.Builder builder = RecoveringBlockProto.newBuilder();
    builder.setBlock(lb).setNewGenStamp(b.getNewGenerationStamp());
    if(b.getNewBlock() != null)
      builder.setTruncateBlock(PBHelper.convert(b.getNewBlock()));
    return builder.build();
  }

  public static RecoveringBlock convert(RecoveringBlockProto b) {
    ExtendedBlock block = convert(b.getBlock().getB());
    DatanodeInfo[] locs = convert(b.getBlock().getLocsList());
    return (b.hasTruncateBlock()) ?
        new RecoveringBlock(block, locs, PBHelper.convert(b.getTruncateBlock())) :
        new RecoveringBlock(block, locs, b.getNewGenStamp());
  }
  
  public static DatanodeInfoProto.AdminState convert(
      final DatanodeInfo.AdminStates inAs) {
    switch (inAs) {
    case NORMAL: return  DatanodeInfoProto.AdminState.NORMAL;
    case DECOMMISSION_INPROGRESS: 
        return DatanodeInfoProto.AdminState.DECOMMISSION_INPROGRESS;
    case DECOMMISSIONED: return DatanodeInfoProto.AdminState.DECOMMISSIONED;
    default: return DatanodeInfoProto.AdminState.NORMAL;
    }
  }
  
  static public DatanodeInfo convert(DatanodeInfoProto di) {
    if (di == null) return null;
    return new DatanodeInfo(
        PBHelper.convert(di.getId()),
        di.hasLocation() ? di.getLocation() : null , 
        di.getCapacity(),  di.getDfsUsed(),  di.getRemaining(),
        di.getBlockPoolUsed(), di.getCacheCapacity(), di.getCacheUsed(),
        di.getLastUpdate(), di.getLastUpdateMonotonic(),
        di.getXceiverCount(), PBHelper.convert(di.getAdminState()));
  }
  
  static public DatanodeInfoProto convertDatanodeInfo(DatanodeInfo di) {
    if (di == null) return null;
    return convert(di);
  }
  
  
  static public DatanodeInfo[] convert(DatanodeInfoProto di[]) {
    if (di == null) return null;
    DatanodeInfo[] result = new DatanodeInfo[di.length];
    for (int i = 0; i < di.length; i++) {
      result[i] = convert(di[i]);
    }    
    return result;
  }

  public static List<? extends HdfsProtos.DatanodeInfoProto> convert(
      DatanodeInfo[] dnInfos) {
    return convert(dnInfos, 0);
  }
  
  /**
   * Copy from {@code dnInfos} to a target of list of same size starting at
   * {@code startIdx}.
   */
  public static List<? extends HdfsProtos.DatanodeInfoProto> convert(
      DatanodeInfo[] dnInfos, int startIdx) {
    if (dnInfos == null)
      return null;
    ArrayList<HdfsProtos.DatanodeInfoProto> protos = Lists
        .newArrayListWithCapacity(dnInfos.length);
    for (int i = startIdx; i < dnInfos.length; i++) {
      protos.add(convert(dnInfos[i]));
    }
    return protos;
  }

  public static DatanodeInfo[] convert(List<DatanodeInfoProto> list) {
    DatanodeInfo[] info = new DatanodeInfo[list.size()];
    for (int i = 0; i < info.length; i++) {
      info[i] = convert(list.get(i));
    }
    return info;
  }
  
  public static DatanodeInfoProto convert(DatanodeInfo info) {
    DatanodeInfoProto.Builder builder = DatanodeInfoProto.newBuilder();
    if (info.getNetworkLocation() != null) {
      builder.setLocation(info.getNetworkLocation());
    }
    builder
        .setId(PBHelper.convert((DatanodeID)info))
        .setCapacity(info.getCapacity())
        .setDfsUsed(info.getDfsUsed())
        .setRemaining(info.getRemaining())
        .setBlockPoolUsed(info.getBlockPoolUsed())
        .setCacheCapacity(info.getCacheCapacity())
        .setCacheUsed(info.getCacheUsed())
        .setLastUpdate(info.getLastUpdate())
        .setLastUpdateMonotonic(info.getLastUpdateMonotonic())
        .setXceiverCount(info.getXceiverCount())
        .setAdminState(PBHelper.convert(info.getAdminState()))
        .build();
    return builder.build();
  }

  public static DatanodeStorageReportProto convertDatanodeStorageReport(
      DatanodeStorageReport report) {
    return DatanodeStorageReportProto.newBuilder()
        .setDatanodeInfo(convert(report.getDatanodeInfo()))
        .addAllStorageReports(convertStorageReports(report.getStorageReports()))
        .build();
  }

  public static List<DatanodeStorageReportProto> convertDatanodeStorageReports(
      DatanodeStorageReport[] reports) {
    final List<DatanodeStorageReportProto> protos
        = new ArrayList<DatanodeStorageReportProto>(reports.length);
    for(int i = 0; i < reports.length; i++) {
      protos.add(convertDatanodeStorageReport(reports[i]));
    }
    return protos;
  }

  public static DatanodeStorageReport convertDatanodeStorageReport(
      DatanodeStorageReportProto proto) {
    return new DatanodeStorageReport(
        convert(proto.getDatanodeInfo()),
        convertStorageReports(proto.getStorageReportsList()));
  }

  public static DatanodeStorageReport[] convertDatanodeStorageReports(
      List<DatanodeStorageReportProto> protos) {
    final DatanodeStorageReport[] reports
        = new DatanodeStorageReport[protos.size()];
    for(int i = 0; i < reports.length; i++) {
      reports[i] = convertDatanodeStorageReport(protos.get(i));
    }
    return reports;
  }

  public static AdminStates convert(AdminState adminState) {
    switch(adminState) {
    case DECOMMISSION_INPROGRESS:
      return AdminStates.DECOMMISSION_INPROGRESS;
    case DECOMMISSIONED:
      return AdminStates.DECOMMISSIONED;
    case NORMAL:
    default:
      return AdminStates.NORMAL;
    }
  }
  
  public static LocatedBlockProto convert(LocatedBlock b) {
    if (b == null) return null;
    Builder builder = LocatedBlockProto.newBuilder();
    DatanodeInfo[] locs = b.getLocations();
    List<DatanodeInfo> cachedLocs =
        Lists.newLinkedList(Arrays.asList(b.getCachedLocations()));
    for (int i = 0; i < locs.length; i++) {
      DatanodeInfo loc = locs[i];
      builder.addLocs(i, PBHelper.convert(loc));
      boolean locIsCached = cachedLocs.contains(loc);
      builder.addIsCached(locIsCached);
      if (locIsCached) {
        cachedLocs.remove(loc);
      }
    }
    Preconditions.checkArgument(cachedLocs.size() == 0,
        "Found additional cached replica locations that are not in the set of"
        + " storage-backed locations!");

    StorageType[] storageTypes = b.getStorageTypes();
    if (storageTypes != null) {
      for (int i = 0; i < storageTypes.length; ++i) {
        builder.addStorageTypes(PBHelper.convertStorageType(storageTypes[i]));
      }
    }
    final String[] storageIDs = b.getStorageIDs();
    if (storageIDs != null) {
      builder.addAllStorageIDs(Arrays.asList(storageIDs));
    }

    return builder.setB(PBHelper.convert(b.getBlock()))
        .setBlockToken(PBHelper.convert(b.getBlockToken()))
        .setCorrupt(b.isCorrupt()).setOffset(b.getStartOffset()).build();
  }
  
  public static LocatedBlock convert(LocatedBlockProto proto) {
    if (proto == null) return null;
    List<DatanodeInfoProto> locs = proto.getLocsList();
    DatanodeInfo[] targets = new DatanodeInfo[locs.size()];
    for (int i = 0; i < locs.size(); i++) {
      targets[i] = PBHelper.convert(locs.get(i));
    }

    final StorageType[] storageTypes = convertStorageTypes(
        proto.getStorageTypesList(), locs.size());

    final int storageIDsCount = proto.getStorageIDsCount();
    final String[] storageIDs;
    if (storageIDsCount == 0) {
      storageIDs = null;
    } else {
      Preconditions.checkState(storageIDsCount == locs.size());
      storageIDs = proto.getStorageIDsList().toArray(new String[storageIDsCount]);
    }

    // Set values from the isCached list, re-using references from loc
    List<DatanodeInfo> cachedLocs = new ArrayList<DatanodeInfo>(locs.size());
    List<Boolean> isCachedList = proto.getIsCachedList();
    for (int i=0; i<isCachedList.size(); i++) {
      if (isCachedList.get(i)) {
        cachedLocs.add(targets[i]);
      }
    }

    LocatedBlock lb = new LocatedBlock(PBHelper.convert(proto.getB()), targets,
        storageIDs, storageTypes, proto.getOffset(), proto.getCorrupt(),
        cachedLocs.toArray(new DatanodeInfo[0]));
    lb.setBlockToken(PBHelper.convert(proto.getBlockToken()));

    return lb;
  }

  public static TokenProto convert(Token<?> tok) {
    return TokenProto.newBuilder().
              setIdentifier(ByteString.copyFrom(tok.getIdentifier())).
              setPassword(ByteString.copyFrom(tok.getPassword())).
              setKind(tok.getKind().toString()).
              setService(tok.getService().toString()).build(); 
  }
  
  public static Token<BlockTokenIdentifier> convert(
      TokenProto blockToken) {
    return new Token<BlockTokenIdentifier>(blockToken.getIdentifier()
        .toByteArray(), blockToken.getPassword().toByteArray(), new Text(
        blockToken.getKind()), new Text(blockToken.getService()));
  }

  
  public static Token<DelegationTokenIdentifier> convertDelegationToken(
      TokenProto blockToken) {
    return new Token<DelegationTokenIdentifier>(blockToken.getIdentifier()
        .toByteArray(), blockToken.getPassword().toByteArray(), new Text(
        blockToken.getKind()), new Text(blockToken.getService()));
  }

  public static ReplicaState convert(ReplicaStateProto state) {
    switch (state) {
    case RBW:
      return ReplicaState.RBW;
    case RUR:
      return ReplicaState.RUR;
    case RWR:
      return ReplicaState.RWR;
    case TEMPORARY:
      return ReplicaState.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaState.FINALIZED;
    }
  }

  public static ReplicaStateProto convert(ReplicaState state) {
    switch (state) {
    case RBW:
      return ReplicaStateProto.RBW;
    case RUR:
      return ReplicaStateProto.RUR;
    case RWR:
      return ReplicaStateProto.RWR;
    case TEMPORARY:
      return ReplicaStateProto.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaStateProto.FINALIZED;
    }
  }
  
  public static DatanodeRegistrationProto convert(
      DatanodeRegistration registration) {
    DatanodeRegistrationProto.Builder builder = DatanodeRegistrationProto
        .newBuilder();
    return builder.setDatanodeID(PBHelper.convert((DatanodeID) registration))
        .setStorageInfo(PBHelper.convert(registration.getStorageInfo()))
        .setKeys(PBHelper.convert(registration.getExportedKeys()))
        .setSoftwareVersion(registration.getSoftwareVersion()).build();
  }

  public static DatanodeRegistration convert(DatanodeRegistrationProto proto) {
    StorageInfo si = convert(proto.getStorageInfo(), NodeType.DATA_NODE);
    return new DatanodeRegistration(PBHelper.convert(proto.getDatanodeID()),
        si, PBHelper.convert(proto.getKeys()), proto.getSoftwareVersion());
  }

  public static DatanodeCommand convert(DatanodeCommandProto proto) {
    switch (proto.getCmdType()) {
    case BalancerBandwidthCommand:
      return PBHelper.convert(proto.getBalancerCmd());
    case BlockCommand:
      return PBHelper.convert(proto.getBlkCmd());
    case BlockRecoveryCommand:
      return PBHelper.convert(proto.getRecoveryCmd());
    case FinalizeCommand:
      return PBHelper.convert(proto.getFinalizeCmd());
    case KeyUpdateCommand:
      return PBHelper.convert(proto.getKeyUpdateCmd());
    case RegisterCommand:
      return REG_CMD;
    case BlockIdCommand:
      return PBHelper.convert(proto.getBlkIdCmd());
    default:
      return null;
    }
  }
  
  public static BalancerBandwidthCommandProto convert(
      BalancerBandwidthCommand bbCmd) {
    return BalancerBandwidthCommandProto.newBuilder()
        .setBandwidth(bbCmd.getBalancerBandwidthValue()).build();
  }

  public static KeyUpdateCommandProto convert(KeyUpdateCommand cmd) {
    return KeyUpdateCommandProto.newBuilder()
        .setKeys(PBHelper.convert(cmd.getExportedKeys())).build();
  }

  public static BlockRecoveryCommandProto convert(BlockRecoveryCommand cmd) {
    BlockRecoveryCommandProto.Builder builder = BlockRecoveryCommandProto
        .newBuilder();
    for (RecoveringBlock b : cmd.getRecoveringBlocks()) {
      builder.addBlocks(PBHelper.convert(b));
    }
    return builder.build();
  }

  public static FinalizeCommandProto convert(FinalizeCommand cmd) {
    return FinalizeCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId()).build();
  }

  public static BlockCommandProto convert(BlockCommand cmd) {
    BlockCommandProto.Builder builder = BlockCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId());
    switch (cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      builder.setAction(BlockCommandProto.Action.TRANSFER);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      builder.setAction(BlockCommandProto.Action.INVALIDATE);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      builder.setAction(BlockCommandProto.Action.SHUTDOWN);
      break;
    default:
      throw new AssertionError("Invalid action");
    }
    Block[] blocks = cmd.getBlocks();
    for (int i = 0; i < blocks.length; i++) {
      builder.addBlocks(PBHelper.convert(blocks[i]));
    }
    builder.addAllTargets(convert(cmd.getTargets()))
           .addAllTargetStorageUuids(convert(cmd.getTargetStorageIDs()));
    StorageType[][] types = cmd.getTargetStorageTypes();
    if (types != null) {
      builder.addAllTargetStorageTypes(convert(types));
    }
    return builder.build();
  }

  private static List<StorageTypesProto> convert(StorageType[][] types) {
    List<StorageTypesProto> list = Lists.newArrayList();
    if (types != null) {
      for (StorageType[] ts : types) {
        StorageTypesProto.Builder builder = StorageTypesProto.newBuilder();
        builder.addAllStorageTypes(convertStorageTypes(ts));
        list.add(builder.build());
      }
    }
    return list;
  }

  public static BlockIdCommandProto convert(BlockIdCommand cmd) {
    BlockIdCommandProto.Builder builder = BlockIdCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId());
    switch (cmd.getAction()) {
    case DatanodeProtocol.DNA_CACHE:
      builder.setAction(BlockIdCommandProto.Action.CACHE);
      break;
    case DatanodeProtocol.DNA_UNCACHE:
      builder.setAction(BlockIdCommandProto.Action.UNCACHE);
      break;
    default:
      throw new AssertionError("Invalid action");
    }
    long[] blockIds = cmd.getBlockIds();
    for (int i = 0; i < blockIds.length; i++) {
      builder.addBlockIds(blockIds[i]);
    }
    return builder.build();
  }

  private static List<DatanodeInfosProto> convert(DatanodeInfo[][] targets) {
    DatanodeInfosProto[] ret = new DatanodeInfosProto[targets.length];
    for (int i = 0; i < targets.length; i++) {
      ret[i] = DatanodeInfosProto.newBuilder()
          .addAllDatanodes(PBHelper.convert(targets[i])).build();
    }
    return Arrays.asList(ret);
  }

  private static List<StorageUuidsProto> convert(String[][] targetStorageUuids) {
    StorageUuidsProto[] ret = new StorageUuidsProto[targetStorageUuids.length];
    for (int i = 0; i < targetStorageUuids.length; i++) {
      ret[i] = StorageUuidsProto.newBuilder()
          .addAllStorageUuids(Arrays.asList(targetStorageUuids[i])).build();
    }
    return Arrays.asList(ret);
  }

  public static DatanodeCommandProto convert(DatanodeCommand datanodeCommand) {
    DatanodeCommandProto.Builder builder = DatanodeCommandProto.newBuilder();
    if (datanodeCommand == null) {
      return builder.setCmdType(DatanodeCommandProto.Type.NullDatanodeCommand)
          .build();
    }
    switch (datanodeCommand.getAction()) {
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
      builder.setCmdType(DatanodeCommandProto.Type.BalancerBandwidthCommand)
          .setBalancerCmd(
              PBHelper.convert((BalancerBandwidthCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      builder
          .setCmdType(DatanodeCommandProto.Type.KeyUpdateCommand)
          .setKeyUpdateCmd(PBHelper.convert((KeyUpdateCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      builder.setCmdType(DatanodeCommandProto.Type.BlockRecoveryCommand)
          .setRecoveryCmd(
              PBHelper.convert((BlockRecoveryCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      builder.setCmdType(DatanodeCommandProto.Type.FinalizeCommand)
          .setFinalizeCmd(PBHelper.convert((FinalizeCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_REGISTER:
      builder.setCmdType(DatanodeCommandProto.Type.RegisterCommand)
          .setRegisterCmd(REG_CMD_PROTO);
      break;
    case DatanodeProtocol.DNA_TRANSFER:
    case DatanodeProtocol.DNA_INVALIDATE:
    case DatanodeProtocol.DNA_SHUTDOWN:
      builder.setCmdType(DatanodeCommandProto.Type.BlockCommand).
        setBlkCmd(PBHelper.convert((BlockCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_CACHE:
    case DatanodeProtocol.DNA_UNCACHE:
      builder.setCmdType(DatanodeCommandProto.Type.BlockIdCommand).
        setBlkIdCmd(PBHelper.convert((BlockIdCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_UNKNOWN: //Not expected
    default:
      builder.setCmdType(DatanodeCommandProto.Type.NullDatanodeCommand);
    }
    return builder.build();
  }

  public static KeyUpdateCommand convert(KeyUpdateCommandProto keyUpdateCmd) {
    return new KeyUpdateCommand(PBHelper.convert(keyUpdateCmd.getKeys()));
  }

  public static FinalizeCommand convert(FinalizeCommandProto finalizeCmd) {
    return new FinalizeCommand(finalizeCmd.getBlockPoolId());
  }

  public static BlockRecoveryCommand convert(
      BlockRecoveryCommandProto recoveryCmd) {
    List<RecoveringBlockProto> list = recoveryCmd.getBlocksList();
    List<RecoveringBlock> recoveringBlocks = new ArrayList<RecoveringBlock>(
        list.size());
    
    for (RecoveringBlockProto rbp : list) {
      recoveringBlocks.add(PBHelper.convert(rbp));
    }
    return new BlockRecoveryCommand(recoveringBlocks);
  }

  public static BlockCommand convert(BlockCommandProto blkCmd) {
    List<BlockProto> blockProtoList = blkCmd.getBlocksList();
    Block[] blocks = new Block[blockProtoList.size()];
    for (int i = 0; i < blockProtoList.size(); i++) {
      blocks[i] = PBHelper.convert(blockProtoList.get(i));
    }
    List<DatanodeInfosProto> targetList = blkCmd.getTargetsList();
    DatanodeInfo[][] targets = new DatanodeInfo[targetList.size()][];
    for (int i = 0; i < targetList.size(); i++) {
      targets[i] = PBHelper.convert(targetList.get(i));
    }

    StorageType[][] targetStorageTypes = new StorageType[targetList.size()][];
    List<StorageTypesProto> targetStorageTypesList = blkCmd.getTargetStorageTypesList();
    if (targetStorageTypesList.isEmpty()) { // missing storage types
      for(int i = 0; i < targetStorageTypes.length; i++) {
        targetStorageTypes[i] = new StorageType[targets[i].length];
        Arrays.fill(targetStorageTypes[i], StorageType.DEFAULT);
      }
    } else {
      for(int i = 0; i < targetStorageTypes.length; i++) {
        List<StorageTypeProto> p = targetStorageTypesList.get(i).getStorageTypesList();
        targetStorageTypes[i] = convertStorageTypes(p, targets[i].length);
      }
    }

    List<StorageUuidsProto> targetStorageUuidsList = blkCmd.getTargetStorageUuidsList();
    String[][] targetStorageIDs = new String[targetStorageUuidsList.size()][];
    for(int i = 0; i < targetStorageIDs.length; i++) {
      List<String> storageIDs = targetStorageUuidsList.get(i).getStorageUuidsList();
      targetStorageIDs[i] = storageIDs.toArray(new String[storageIDs.size()]);
    }

    int action = DatanodeProtocol.DNA_UNKNOWN;
    switch (blkCmd.getAction()) {
    case TRANSFER:
      action = DatanodeProtocol.DNA_TRANSFER;
      break;
    case INVALIDATE:
      action = DatanodeProtocol.DNA_INVALIDATE;
      break;
    case SHUTDOWN:
      action = DatanodeProtocol.DNA_SHUTDOWN;
      break;
    default:
      throw new AssertionError("Unknown action type: " + blkCmd.getAction());
    }
    return new BlockCommand(action, blkCmd.getBlockPoolId(), blocks, targets,
        targetStorageTypes, targetStorageIDs);
  }

  public static BlockIdCommand convert(BlockIdCommandProto blkIdCmd) {
    int numBlockIds = blkIdCmd.getBlockIdsCount();
    long blockIds[] = new long[numBlockIds];
    for (int i = 0; i < numBlockIds; i++) {
      blockIds[i] = blkIdCmd.getBlockIds(i);
    }
    int action = DatanodeProtocol.DNA_UNKNOWN;
    switch (blkIdCmd.getAction()) {
    case CACHE:
      action = DatanodeProtocol.DNA_CACHE;
      break;
    case UNCACHE:
      action = DatanodeProtocol.DNA_UNCACHE;
      break;
    default:
      throw new AssertionError("Unknown action type: " + blkIdCmd.getAction());
    }
    return new BlockIdCommand(action, blkIdCmd.getBlockPoolId(), blockIds);
  }

  public static DatanodeInfo[] convert(DatanodeInfosProto datanodeInfosProto) {
    List<DatanodeInfoProto> proto = datanodeInfosProto.getDatanodesList();
    DatanodeInfo[] infos = new DatanodeInfo[proto.size()];
    for (int i = 0; i < infos.length; i++) {
      infos[i] = PBHelper.convert(proto.get(i));
    }
    return infos;
  }

  public static BalancerBandwidthCommand convert(
      BalancerBandwidthCommandProto balancerCmd) {
    return new BalancerBandwidthCommand(balancerCmd.getBandwidth());
  }

  public static ReceivedDeletedBlockInfoProto convert(
      ReceivedDeletedBlockInfo receivedDeletedBlockInfo) {
    ReceivedDeletedBlockInfoProto.Builder builder = 
        ReceivedDeletedBlockInfoProto.newBuilder();
    
    ReceivedDeletedBlockInfoProto.BlockStatus status;
    switch (receivedDeletedBlockInfo.getStatus()) {
    case RECEIVING_BLOCK:
      status = ReceivedDeletedBlockInfoProto.BlockStatus.RECEIVING;
      break;
    case RECEIVED_BLOCK:
      status = ReceivedDeletedBlockInfoProto.BlockStatus.RECEIVED;
      break;
    case DELETED_BLOCK:
      status = ReceivedDeletedBlockInfoProto.BlockStatus.DELETED;
      break;
    default:
      throw new IllegalArgumentException("Bad status: " +
          receivedDeletedBlockInfo.getStatus());
    }
    builder.setStatus(status);
    
    if (receivedDeletedBlockInfo.getDelHints() != null) {
      builder.setDeleteHint(receivedDeletedBlockInfo.getDelHints());
    }
    return builder.setBlock(PBHelper.convert(receivedDeletedBlockInfo.getBlock()))
        .build();
  }

  public static ReceivedDeletedBlockInfo convert(
      ReceivedDeletedBlockInfoProto proto) {
    ReceivedDeletedBlockInfo.BlockStatus status = null;
    switch (proto.getStatus()) {
    case RECEIVING:
      status = BlockStatus.RECEIVING_BLOCK;
      break;
    case RECEIVED:
      status = BlockStatus.RECEIVED_BLOCK;
      break;
    case DELETED:
      status = BlockStatus.DELETED_BLOCK;
      break;
    }
    return new ReceivedDeletedBlockInfo(
        PBHelper.convert(proto.getBlock()),
        status,
        proto.hasDeleteHint() ? proto.getDeleteHint() : null);
  }
  
  public static NamespaceInfoProto convert(NamespaceInfo info) {
    return NamespaceInfoProto.newBuilder()
        .setBlockPoolID(info.getBlockPoolID())
        .setBuildVersion(info.getBuildVersion())
        .setUnused(0)
        .setStorageInfo(PBHelper.convert((StorageInfo)info))
        .setSoftwareVersion(info.getSoftwareVersion())
        .setCapabilities(info.getCapabilities())
        .build();
  }
  
  // Located Block Arrays and Lists
  public static LocatedBlockProto[] convertLocatedBlock(LocatedBlock[] lb) {
    if (lb == null) return null;
    return convertLocatedBlock2(Arrays.asList(lb)).toArray(
        new LocatedBlockProto[lb.length]);
  }
  
  public static LocatedBlock[] convertLocatedBlock(LocatedBlockProto[] lb) {
    if (lb == null) return null;
    return convertLocatedBlock(Arrays.asList(lb)).toArray(
        new LocatedBlock[lb.length]);
  }
  
  public static List<LocatedBlock> convertLocatedBlock(
      List<LocatedBlockProto> lb) {
    if (lb == null) return null;
    final int len = lb.size();
    List<LocatedBlock> result = 
        new ArrayList<LocatedBlock>(len);
    for (int i = 0; i < len; ++i) {
      result.add(PBHelper.convert(lb.get(i)));
    }
    return result;
  }
  
  public static List<LocatedBlockProto> convertLocatedBlock2(List<LocatedBlock> lb) {
    if (lb == null) return null;
    final int len = lb.size();
    List<LocatedBlockProto> result = new ArrayList<LocatedBlockProto>(len);
    for (int i = 0; i < len; ++i) {
      result.add(PBHelper.convert(lb.get(i)));
    }
    return result;
  }
  
  
  // LocatedBlocks
  public static LocatedBlocks convert(LocatedBlocksProto lb) {
    return new LocatedBlocks(
        lb.getFileLength(), lb.getUnderConstruction(),
        PBHelper.convertLocatedBlock(lb.getBlocksList()),
        lb.hasLastBlock() ? PBHelper.convert(lb.getLastBlock()) : null,
        lb.getIsLastBlockComplete(),
        lb.hasFileEncryptionInfo() ? convert(lb.getFileEncryptionInfo()) :
            null);
  }
  
  public static LocatedBlocksProto convert(LocatedBlocks lb) {
    if (lb == null) {
      return null;
    }
    LocatedBlocksProto.Builder builder = 
        LocatedBlocksProto.newBuilder();
    if (lb.getLastLocatedBlock() != null) {
      builder.setLastBlock(PBHelper.convert(lb.getLastLocatedBlock()));
    }
    if (lb.getFileEncryptionInfo() != null) {
      builder.setFileEncryptionInfo(convert(lb.getFileEncryptionInfo()));
    }
    return builder.setFileLength(lb.getFileLength())
        .setUnderConstruction(lb.isUnderConstruction())
        .addAllBlocks(PBHelper.convertLocatedBlock2(lb.getLocatedBlocks()))
        .setIsLastBlockComplete(lb.isLastBlockComplete()).build();
  }
  
  // DataEncryptionKey
  public static DataEncryptionKey convert(DataEncryptionKeyProto bet) {
    String encryptionAlgorithm = bet.getEncryptionAlgorithm();
    return new DataEncryptionKey(bet.getKeyId(),
        bet.getBlockPoolId(),
        bet.getNonce().toByteArray(),
        bet.getEncryptionKey().toByteArray(),
        bet.getExpiryDate(),
        encryptionAlgorithm.isEmpty() ? null : encryptionAlgorithm);
  }
  
  public static DataEncryptionKeyProto convert(DataEncryptionKey bet) {
    DataEncryptionKeyProto.Builder b = DataEncryptionKeyProto.newBuilder()
        .setKeyId(bet.keyId)
        .setBlockPoolId(bet.blockPoolId)
        .setNonce(ByteString.copyFrom(bet.nonce))
        .setEncryptionKey(ByteString.copyFrom(bet.encryptionKey))
        .setExpiryDate(bet.expiryDate);
    if (bet.encryptionAlgorithm != null) {
      b.setEncryptionAlgorithm(bet.encryptionAlgorithm);
    }
    return b.build();
  }
  
  public static FsServerDefaults convert(FsServerDefaultsProto fs) {
    if (fs == null) return null;
    return new FsServerDefaults(
        fs.getBlockSize(), fs.getBytesPerChecksum(), 
        fs.getWritePacketSize(), (short) fs.getReplication(),
        fs.getFileBufferSize(),
        fs.getEncryptDataTransfer(),
        fs.getTrashInterval(),
        PBHelper.convert(fs.getChecksumType()));
  }
  
  public static FsServerDefaultsProto convert(FsServerDefaults fs) {
    if (fs == null) return null;
    return FsServerDefaultsProto.newBuilder().
      setBlockSize(fs.getBlockSize()).
      setBytesPerChecksum(fs.getBytesPerChecksum()).
      setWritePacketSize(fs.getWritePacketSize())
      .setReplication(fs.getReplication())
      .setFileBufferSize(fs.getFileBufferSize())
      .setEncryptDataTransfer(fs.getEncryptDataTransfer())
      .setTrashInterval(fs.getTrashInterval())
      .setChecksumType(PBHelper.convert(fs.getChecksumType()))
      .build();
  }
  
  public static FsPermissionProto convert(FsPermission p) {
    return FsPermissionProto.newBuilder().setPerm(p.toExtendedShort()).build();
  }
  
  public static FsPermission convert(FsPermissionProto p) {
    return new FsPermissionExtension((short)p.getPerm());
  }
  
  
  // The creatFlag field in PB is a bitmask whose values are the same a the 
  // emum values of CreateFlag
  public static int convertCreateFlag(EnumSetWritable<CreateFlag> flag) {
    int value = 0;
    if (flag.contains(CreateFlag.APPEND)) {
      value |= CreateFlagProto.APPEND.getNumber();
    }
    if (flag.contains(CreateFlag.CREATE)) {
      value |= CreateFlagProto.CREATE.getNumber();
    }
    if (flag.contains(CreateFlag.OVERWRITE)) {
      value |= CreateFlagProto.OVERWRITE.getNumber();
    }
    if (flag.contains(CreateFlag.LAZY_PERSIST)) {
      value |= CreateFlagProto.LAZY_PERSIST.getNumber();
    }
    if (flag.contains(CreateFlag.NEW_BLOCK)) {
      value |= CreateFlagProto.NEW_BLOCK.getNumber();
    }
    return value;
  }
  
  public static EnumSetWritable<CreateFlag> convertCreateFlag(int flag) {
    EnumSet<CreateFlag> result = 
       EnumSet.noneOf(CreateFlag.class);   
    if ((flag & CreateFlagProto.APPEND_VALUE) == CreateFlagProto.APPEND_VALUE) {
      result.add(CreateFlag.APPEND);
    }
    if ((flag & CreateFlagProto.CREATE_VALUE) == CreateFlagProto.CREATE_VALUE) {
      result.add(CreateFlag.CREATE);
    }
    if ((flag & CreateFlagProto.OVERWRITE_VALUE) 
        == CreateFlagProto.OVERWRITE_VALUE) {
      result.add(CreateFlag.OVERWRITE);
    }
    if ((flag & CreateFlagProto.LAZY_PERSIST_VALUE)
        == CreateFlagProto.LAZY_PERSIST_VALUE) {
      result.add(CreateFlag.LAZY_PERSIST);
    }
    if ((flag & CreateFlagProto.NEW_BLOCK_VALUE)
        == CreateFlagProto.NEW_BLOCK_VALUE) {
      result.add(CreateFlag.NEW_BLOCK);
    }
    return new EnumSetWritable<CreateFlag>(result, CreateFlag.class);
  }

  public static int convertCacheFlags(EnumSet<CacheFlag> flags) {
    int value = 0;
    if (flags.contains(CacheFlag.FORCE)) {
      value |= CacheFlagProto.FORCE.getNumber();
    }
    return value;
  }

  public static EnumSet<CacheFlag> convertCacheFlags(int flags) {
    EnumSet<CacheFlag> result = EnumSet.noneOf(CacheFlag.class);
    if ((flags & CacheFlagProto.FORCE_VALUE) == CacheFlagProto.FORCE_VALUE) {
      result.add(CacheFlag.FORCE);
    }
    return result;
  }

  public static HdfsFileStatus convert(HdfsFileStatusProto fs) {
    if (fs == null)
      return null;
    return new HdfsLocatedFileStatus(
        fs.getLength(), fs.getFileType().equals(FileType.IS_DIR), 
        fs.getBlockReplication(), fs.getBlocksize(),
        fs.getModificationTime(), fs.getAccessTime(),
        PBHelper.convert(fs.getPermission()), fs.getOwner(), fs.getGroup(), 
        fs.getFileType().equals(FileType.IS_SYMLINK) ? 
            fs.getSymlink().toByteArray() : null,
        fs.getPath().toByteArray(),
        fs.hasFileId()? fs.getFileId(): INodeId.GRANDFATHER_INODE_ID,
        fs.hasLocations() ? PBHelper.convert(fs.getLocations()) : null,
        fs.hasChildrenNum() ? fs.getChildrenNum() : -1,
        fs.hasFileEncryptionInfo() ? convert(fs.getFileEncryptionInfo()) : null,
        fs.hasStoragePolicy() ? (byte) fs.getStoragePolicy()
            : BlockStoragePolicySuite.ID_UNSPECIFIED);
  }

  public static SnapshottableDirectoryStatus convert(
      SnapshottableDirectoryStatusProto sdirStatusProto) {
    if (sdirStatusProto == null) {
      return null;
    }
    final HdfsFileStatusProto status = sdirStatusProto.getDirStatus();
    return new SnapshottableDirectoryStatus(
        status.getModificationTime(),
        status.getAccessTime(),
        PBHelper.convert(status.getPermission()),
        status.getOwner(),
        status.getGroup(),
        status.getPath().toByteArray(),
        status.getFileId(),
        status.getChildrenNum(),
        sdirStatusProto.getSnapshotNumber(),
        sdirStatusProto.getSnapshotQuota(),
        sdirStatusProto.getParentFullpath().toByteArray());
  }
  
  public static HdfsFileStatusProto convert(HdfsFileStatus fs) {
    if (fs == null)
      return null;
    FileType fType = FileType.IS_FILE;
    if (fs.isDir()) {
      fType = FileType.IS_DIR;
    } else if (fs.isSymlink()) {
      fType = FileType.IS_SYMLINK;
    }

    HdfsFileStatusProto.Builder builder = 
     HdfsFileStatusProto.newBuilder().
      setLength(fs.getLen()).
      setFileType(fType).
      setBlockReplication(fs.getReplication()).
      setBlocksize(fs.getBlockSize()).
      setModificationTime(fs.getModificationTime()).
      setAccessTime(fs.getAccessTime()).
      setPermission(PBHelper.convert(fs.getPermission())).
      setOwner(fs.getOwner()).
      setGroup(fs.getGroup()).
      setFileId(fs.getFileId()).
      setChildrenNum(fs.getChildrenNum()).
      setPath(ByteString.copyFrom(fs.getLocalNameInBytes())).
      setStoragePolicy(fs.getStoragePolicy());
    if (fs.isSymlink())  {
      builder.setSymlink(ByteString.copyFrom(fs.getSymlinkInBytes()));
    }
    if (fs.getFileEncryptionInfo() != null) {
      builder.setFileEncryptionInfo(convert(fs.getFileEncryptionInfo()));
    }
    if (fs instanceof HdfsLocatedFileStatus) {
      final HdfsLocatedFileStatus lfs = (HdfsLocatedFileStatus) fs;
      LocatedBlocks locations = lfs.getBlockLocations();
      if (locations != null) {
        builder.setLocations(PBHelper.convert(locations));
      }
    }
    return builder.build();
  }
  
  public static SnapshottableDirectoryStatusProto convert(
      SnapshottableDirectoryStatus status) {
    if (status == null) {
      return null;
    }
    int snapshotNumber = status.getSnapshotNumber();
    int snapshotQuota = status.getSnapshotQuota();
    byte[] parentFullPath = status.getParentFullPath();
    ByteString parentFullPathBytes = ByteString.copyFrom(
        parentFullPath == null ? DFSUtil.EMPTY_BYTES : parentFullPath);
    HdfsFileStatusProto fs = convert(status.getDirStatus());
    SnapshottableDirectoryStatusProto.Builder builder = 
        SnapshottableDirectoryStatusProto
        .newBuilder().setSnapshotNumber(snapshotNumber)
        .setSnapshotQuota(snapshotQuota).setParentFullpath(parentFullPathBytes)
        .setDirStatus(fs);
    return builder.build();
  }
  
  public static HdfsFileStatusProto[] convert(HdfsFileStatus[] fs) {
    if (fs == null) return null;
    final int len = fs.length;
    HdfsFileStatusProto[] result = new HdfsFileStatusProto[len];
    for (int i = 0; i < len; ++i) {
      result[i] = PBHelper.convert(fs[i]);
    }
    return result;
  }
  
  public static HdfsFileStatus[] convert(HdfsFileStatusProto[] fs) {
    if (fs == null) return null;
    final int len = fs.length;
    HdfsFileStatus[] result = new HdfsFileStatus[len];
    for (int i = 0; i < len; ++i) {
      result[i] = PBHelper.convert(fs[i]);
    }
    return result;
  }
  
  public static DirectoryListing convert(DirectoryListingProto dl) {
    if (dl == null)
      return null;
    List<HdfsFileStatusProto> partList =  dl.getPartialListingList();
    return new DirectoryListing( 
        partList.isEmpty() ? new HdfsLocatedFileStatus[0] 
          : PBHelper.convert(
              partList.toArray(new HdfsFileStatusProto[partList.size()])),
        dl.getRemainingEntries());
  }

  public static DirectoryListingProto convert(DirectoryListing d) {
    if (d == null)
      return null;
    return DirectoryListingProto.newBuilder().
        addAllPartialListing(Arrays.asList(
            PBHelper.convert(d.getPartialListing()))).
        setRemainingEntries(d.getRemainingEntries()).
        build();
  }

  public static long[] convert(GetFsStatsResponseProto res) {
    long[] result = new long[7];
    result[ClientProtocol.GET_STATS_CAPACITY_IDX] = res.getCapacity();
    result[ClientProtocol.GET_STATS_USED_IDX] = res.getUsed();
    result[ClientProtocol.GET_STATS_REMAINING_IDX] = res.getRemaining();
    result[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] = res.getUnderReplicated();
    result[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] = res.getCorruptBlocks();
    result[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] = res.getMissingBlocks();
    result[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX] =
        res.getMissingReplOneBlocks();
    return result;
  }
  
  public static GetFsStatsResponseProto convert(long[] fsStats) {
    GetFsStatsResponseProto.Builder result = GetFsStatsResponseProto
        .newBuilder();
    if (fsStats.length >= ClientProtocol.GET_STATS_CAPACITY_IDX + 1)
      result.setCapacity(fsStats[ClientProtocol.GET_STATS_CAPACITY_IDX]);
    if (fsStats.length >= ClientProtocol.GET_STATS_USED_IDX + 1)
      result.setUsed(fsStats[ClientProtocol.GET_STATS_USED_IDX]);
    if (fsStats.length >= ClientProtocol.GET_STATS_REMAINING_IDX + 1)
      result.setRemaining(fsStats[ClientProtocol.GET_STATS_REMAINING_IDX]);
    if (fsStats.length >= ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX + 1)
      result.setUnderReplicated(
              fsStats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX]);
    if (fsStats.length >= ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX + 1)
      result.setCorruptBlocks(
          fsStats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX]);
    if (fsStats.length >= ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX + 1)
      result.setMissingBlocks(
          fsStats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX]);
    if (fsStats.length >= ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX + 1)
      result.setMissingReplOneBlocks(
          fsStats[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX]);
    return result.build();
  }
  
  public static DatanodeReportTypeProto
    convert(DatanodeReportType t) {
    switch (t) {
    case ALL: return DatanodeReportTypeProto.ALL;
    case LIVE: return DatanodeReportTypeProto.LIVE;
    case DEAD: return DatanodeReportTypeProto.DEAD;
    case DECOMMISSIONING: return DatanodeReportTypeProto.DECOMMISSIONING;
    default: 
      throw new IllegalArgumentException("Unexpected data type report:" + t);
    }
  }
  
  public static DatanodeReportType 
    convert(DatanodeReportTypeProto t) {
    switch (t) {
    case ALL: return DatanodeReportType.ALL;
    case LIVE: return DatanodeReportType.LIVE;
    case DEAD: return DatanodeReportType.DEAD;
    case DECOMMISSIONING: return DatanodeReportType.DECOMMISSIONING;
    default: 
      throw new IllegalArgumentException("Unexpected data type report:" + t);
    }
  }

  public static SafeModeActionProto convert(
      SafeModeAction a) {
    switch (a) {
    case SAFEMODE_LEAVE:
      return SafeModeActionProto.SAFEMODE_LEAVE;
    case SAFEMODE_ENTER:
      return SafeModeActionProto.SAFEMODE_ENTER;
    case SAFEMODE_GET:
      return SafeModeActionProto.SAFEMODE_GET;
    default:
      throw new IllegalArgumentException("Unexpected SafeModeAction :" + a);
    }
  }
  
  public static SafeModeAction convert(
      ClientNamenodeProtocolProtos.SafeModeActionProto a) {
    switch (a) {
    case SAFEMODE_LEAVE:
      return SafeModeAction.SAFEMODE_LEAVE;
    case SAFEMODE_ENTER:
      return SafeModeAction.SAFEMODE_ENTER;
    case SAFEMODE_GET:
      return SafeModeAction.SAFEMODE_GET;
    default:
      throw new IllegalArgumentException("Unexpected SafeModeAction :" + a);
    }
  }
  
  public static RollingUpgradeActionProto convert(RollingUpgradeAction a) {
    switch (a) {
    case QUERY:
      return RollingUpgradeActionProto.QUERY;
    case PREPARE:
      return RollingUpgradeActionProto.START;
    case FINALIZE:
      return RollingUpgradeActionProto.FINALIZE;
    default:
      throw new IllegalArgumentException("Unexpected value: " + a);
    }
  }
  
  public static RollingUpgradeAction convert(RollingUpgradeActionProto a) {
    switch (a) {
    case QUERY:
      return RollingUpgradeAction.QUERY;
    case START:
      return RollingUpgradeAction.PREPARE;
    case FINALIZE:
      return RollingUpgradeAction.FINALIZE;
    default:
      throw new IllegalArgumentException("Unexpected value: " + a);
    }
  }

  public static RollingUpgradeStatusProto convertRollingUpgradeStatus(
      RollingUpgradeStatus status) {
    return RollingUpgradeStatusProto.newBuilder()
        .setBlockPoolId(status.getBlockPoolId())
        .setFinalized(status.isFinalized())
        .build();
  }

  public static RollingUpgradeStatus convert(RollingUpgradeStatusProto proto) {
    return new RollingUpgradeStatus(proto.getBlockPoolId(),
        proto.getFinalized());
  }

  public static RollingUpgradeInfoProto convert(RollingUpgradeInfo info) {
    return RollingUpgradeInfoProto.newBuilder()
        .setStatus(convertRollingUpgradeStatus(info))
        .setCreatedRollbackImages(info.createdRollbackImages())
        .setStartTime(info.getStartTime())
        .setFinalizeTime(info.getFinalizeTime())
        .build();
  }

  public static RollingUpgradeInfo convert(RollingUpgradeInfoProto proto) {
    RollingUpgradeStatusProto status = proto.getStatus();
    return new RollingUpgradeInfo(status.getBlockPoolId(),
        proto.getCreatedRollbackImages(),
        proto.getStartTime(), proto.getFinalizeTime());
  }

  public static CorruptFileBlocks convert(CorruptFileBlocksProto c) {
    if (c == null)
      return null;
    List<String> fileList = c.getFilesList();
    return new CorruptFileBlocks(fileList.toArray(new String[fileList.size()]),
        c.getCookie());
  }

  public static CorruptFileBlocksProto convert(CorruptFileBlocks c) {
    if (c == null)
      return null;
    return CorruptFileBlocksProto.newBuilder().
        addAllFiles(Arrays.asList(c.getFiles())).
        setCookie(c.getCookie()).
        build();
  }
  
  public static ContentSummary convert(ContentSummaryProto cs) {
    if (cs == null) return null;
    ContentSummary.Builder builder = new ContentSummary.Builder();
    builder.length(cs.getLength()).
        fileCount(cs.getFileCount()).
        directoryCount(cs.getDirectoryCount()).
        quota(cs.getQuota()).
        spaceConsumed(cs.getSpaceConsumed()).
        spaceQuota(cs.getSpaceQuota());
    if (cs.hasTypeQuotaInfos()) {
      for (HdfsProtos.StorageTypeQuotaInfoProto info :
          cs.getTypeQuotaInfos().getTypeQuotaInfoList()) {
        StorageType type = PBHelper.convertStorageType(info.getType());
        builder.typeConsumed(type, info.getConsumed());
        builder.typeQuota(type, info.getQuota());
      }
    }
    return builder.build();
  }
  
  public static ContentSummaryProto convert(ContentSummary cs) {
    if (cs == null) return null;
    ContentSummaryProto.Builder builder = ContentSummaryProto.newBuilder();
        builder.setLength(cs.getLength()).
        setFileCount(cs.getFileCount()).
        setDirectoryCount(cs.getDirectoryCount()).
        setQuota(cs.getQuota()).
        setSpaceConsumed(cs.getSpaceConsumed()).
        setSpaceQuota(cs.getSpaceQuota());

    if (cs.isTypeQuotaSet() || cs.isTypeConsumedAvailable()) {
      HdfsProtos.StorageTypeQuotaInfosProto.Builder isb =
          HdfsProtos.StorageTypeQuotaInfosProto.newBuilder();
      for (StorageType t: StorageType.getTypesSupportingQuota()) {
        HdfsProtos.StorageTypeQuotaInfoProto info =
            HdfsProtos.StorageTypeQuotaInfoProto.newBuilder().
                setType(convertStorageType(t)).
                setConsumed(cs.getTypeConsumed(t)).
                setQuota(cs.getTypeQuota(t)).
                build();
        isb.addTypeQuotaInfo(info);
      }
      builder.setTypeQuotaInfos(isb);
    }
    return builder.build();
  }

  public static NNHAStatusHeartbeat convert(NNHAStatusHeartbeatProto s) {
    if (s == null) return null;
    switch (s.getState()) {
    case ACTIVE:
      return new NNHAStatusHeartbeat(HAServiceState.ACTIVE, s.getTxid());
    case STANDBY:
      return new NNHAStatusHeartbeat(HAServiceState.STANDBY, s.getTxid());
    default:
      throw new IllegalArgumentException("Unexpected NNHAStatusHeartbeat.State:" + s.getState());
    }
  }

  public static NNHAStatusHeartbeatProto convert(NNHAStatusHeartbeat hb) {
    if (hb == null) return null;
    NNHAStatusHeartbeatProto.Builder builder =
      NNHAStatusHeartbeatProto.newBuilder();
    switch (hb.getState()) {
      case ACTIVE:
        builder.setState(NNHAStatusHeartbeatProto.State.ACTIVE);
        break;
      case STANDBY:
        builder.setState(NNHAStatusHeartbeatProto.State.STANDBY);
        break;
      default:
        throw new IllegalArgumentException("Unexpected NNHAStatusHeartbeat.State:" +
            hb.getState());
    }
    builder.setTxid(hb.getTxId());
    return builder.build();
  }

  public static DatanodeStorageProto convert(DatanodeStorage s) {
    return DatanodeStorageProto.newBuilder()
        .setState(PBHelper.convertState(s.getState()))
        .setStorageType(PBHelper.convertStorageType(s.getStorageType()))
        .setStorageUuid(s.getStorageID()).build();
  }

  private static StorageState convertState(State state) {
    switch(state) {
    case READ_ONLY_SHARED:
      return StorageState.READ_ONLY_SHARED;
    case NORMAL:
    default:
      return StorageState.NORMAL;
    }
  }

  public static List<StorageTypeProto> convertStorageTypes(
      StorageType[] types) {
    return convertStorageTypes(types, 0);
  }

  public static List<StorageTypeProto> convertStorageTypes(
      StorageType[] types, int startIdx) {
    if (types == null) {
      return null;
    }
    final List<StorageTypeProto> protos = new ArrayList<StorageTypeProto>(
        types.length);
    for (int i = startIdx; i < types.length; ++i) {
      protos.add(convertStorageType(types[i]));
    }
    return protos; 
  }

  public static StorageTypeProto convertStorageType(StorageType type) {
    switch(type) {
    case DISK:
      return StorageTypeProto.DISK;
    case SSD:
      return StorageTypeProto.SSD;
    case ARCHIVE:
      return StorageTypeProto.ARCHIVE;
    case RAM_DISK:
      return StorageTypeProto.RAM_DISK;
    default:
      throw new IllegalStateException(
          "BUG: StorageType not found, type=" + type);
    }
  }

  public static DatanodeStorage convert(DatanodeStorageProto s) {
    return new DatanodeStorage(s.getStorageUuid(),
                               PBHelper.convertState(s.getState()),
                               PBHelper.convertStorageType(s.getStorageType()));
  }

  private static State convertState(StorageState state) {
    switch(state) {
    case READ_ONLY_SHARED:
      return DatanodeStorage.State.READ_ONLY_SHARED;
    case NORMAL:
    default:
      return DatanodeStorage.State.NORMAL;
    }
  }

  public static StorageType convertStorageType(StorageTypeProto type) {
    switch(type) {
      case DISK:
        return StorageType.DISK;
      case SSD:
        return StorageType.SSD;
      case ARCHIVE:
        return StorageType.ARCHIVE;
      case RAM_DISK:
        return StorageType.RAM_DISK;
      default:
        throw new IllegalStateException(
            "BUG: StorageTypeProto not found, type=" + type);
    }
  }

  public static StorageType[] convertStorageTypes(
      List<StorageTypeProto> storageTypesList, int expectedSize) {
    final StorageType[] storageTypes = new StorageType[expectedSize];
    if (storageTypesList.size() != expectedSize) { // missing storage types
      Preconditions.checkState(storageTypesList.isEmpty());
      Arrays.fill(storageTypes, StorageType.DEFAULT);
    } else {
      for (int i = 0; i < storageTypes.length; ++i) {
        storageTypes[i] = convertStorageType(storageTypesList.get(i));
      }
    }
    return storageTypes;
  }

  public static StorageReportProto convert(StorageReport r) {
    StorageReportProto.Builder builder = StorageReportProto.newBuilder()
        .setBlockPoolUsed(r.getBlockPoolUsed()).setCapacity(r.getCapacity())
        .setDfsUsed(r.getDfsUsed()).setRemaining(r.getRemaining())
        .setStorageUuid(r.getStorage().getStorageID())
        .setStorage(convert(r.getStorage()));
    return builder.build();
  }

  public static StorageReport convert(StorageReportProto p) {
    return new StorageReport(
        p.hasStorage() ?
            convert(p.getStorage()) :
            new DatanodeStorage(p.getStorageUuid()),
        p.getFailed(), p.getCapacity(), p.getDfsUsed(), p.getRemaining(),
        p.getBlockPoolUsed());
  }

  public static StorageReport[] convertStorageReports(
      List<StorageReportProto> list) {
    final StorageReport[] report = new StorageReport[list.size()];
    for (int i = 0; i < report.length; i++) {
      report[i] = convert(list.get(i));
    }
    return report;
  }

  public static List<StorageReportProto> convertStorageReports(StorageReport[] storages) {
    final List<StorageReportProto> protos = new ArrayList<StorageReportProto>(
        storages.length);
    for(int i = 0; i < storages.length; i++) {
      protos.add(convert(storages[i]));
    }
    return protos;
  }

  public static VolumeFailureSummary convertVolumeFailureSummary(
      VolumeFailureSummaryProto proto) {
    List<String> failedStorageLocations = proto.getFailedStorageLocationsList();
    return new VolumeFailureSummary(
        failedStorageLocations.toArray(new String[failedStorageLocations.size()]),
        proto.getLastVolumeFailureDate(), proto.getEstimatedCapacityLostTotal());
  }

  public static VolumeFailureSummaryProto convertVolumeFailureSummary(
      VolumeFailureSummary volumeFailureSummary) {
    VolumeFailureSummaryProto.Builder builder =
        VolumeFailureSummaryProto.newBuilder();
    for (String failedStorageLocation:
        volumeFailureSummary.getFailedStorageLocations()) {
      builder.addFailedStorageLocations(failedStorageLocation);
    }
    builder.setLastVolumeFailureDate(
        volumeFailureSummary.getLastVolumeFailureDate());
    builder.setEstimatedCapacityLostTotal(
        volumeFailureSummary.getEstimatedCapacityLostTotal());
    return builder.build();
  }

  public static JournalInfo convert(JournalInfoProto info) {
    int lv = info.hasLayoutVersion() ? info.getLayoutVersion() : 0;
    int nsID = info.hasNamespaceID() ? info.getNamespaceID() : 0;
    return new JournalInfo(lv, info.getClusterID(), nsID);
  }

  /**
   * Method used for converting {@link JournalInfoProto} sent from Namenode
   * to Journal receivers to {@link NamenodeRegistration}.
   */
  public static JournalInfoProto convert(JournalInfo j) {
    return JournalInfoProto.newBuilder().setClusterID(j.getClusterId())
        .setLayoutVersion(j.getLayoutVersion())
        .setNamespaceID(j.getNamespaceId()).build();
  } 
  
  public static SnapshottableDirectoryStatus[] convert(
      SnapshottableDirectoryListingProto sdlp) {
    if (sdlp == null)
      return null;
    List<SnapshottableDirectoryStatusProto> list = sdlp
        .getSnapshottableDirListingList();
    if (list.isEmpty()) {
      return new SnapshottableDirectoryStatus[0];
    } else {
      SnapshottableDirectoryStatus[] result = 
          new SnapshottableDirectoryStatus[list.size()];
      for (int i = 0; i < list.size(); i++) {
        result[i] = PBHelper.convert(list.get(i));
      }
      return result;
    }
  }
  
  public static SnapshottableDirectoryListingProto convert(
      SnapshottableDirectoryStatus[] status) {
    if (status == null)
      return null;
    SnapshottableDirectoryStatusProto[] protos = 
        new SnapshottableDirectoryStatusProto[status.length];
    for (int i = 0; i < status.length; i++) {
      protos[i] = PBHelper.convert(status[i]);
    }
    List<SnapshottableDirectoryStatusProto> protoList = Arrays.asList(protos);
    return SnapshottableDirectoryListingProto.newBuilder()
        .addAllSnapshottableDirListing(protoList).build();
  }
  
  public static DiffReportEntry convert(SnapshotDiffReportEntryProto entry) {
    if (entry == null) {
      return null;
    }
    DiffType type = DiffType.getTypeFromLabel(entry
        .getModificationLabel());
    return type == null ? null : new DiffReportEntry(type, entry.getFullpath()
        .toByteArray(), entry.hasTargetPath() ? entry.getTargetPath()
        .toByteArray() : null);
  }
  
  public static SnapshotDiffReportEntryProto convert(DiffReportEntry entry) {
    if (entry == null) {
      return null;
    }
    ByteString sourcePath = ByteString
        .copyFrom(entry.getSourcePath() == null ? DFSUtil.EMPTY_BYTES : entry
            .getSourcePath());
    String modification = entry.getType().getLabel();
    SnapshotDiffReportEntryProto.Builder builder = SnapshotDiffReportEntryProto
        .newBuilder().setFullpath(sourcePath)
        .setModificationLabel(modification);
    if (entry.getType() == DiffType.RENAME) {
      ByteString targetPath = ByteString
          .copyFrom(entry.getTargetPath() == null ? DFSUtil.EMPTY_BYTES : entry
              .getTargetPath());
      builder.setTargetPath(targetPath);
    }
    return builder.build();
  }
  
  public static SnapshotDiffReport convert(SnapshotDiffReportProto reportProto) {
    if (reportProto == null) {
      return null;
    }
    String snapshotDir = reportProto.getSnapshotRoot();
    String fromSnapshot = reportProto.getFromSnapshot();
    String toSnapshot = reportProto.getToSnapshot();
    List<SnapshotDiffReportEntryProto> list = reportProto
        .getDiffReportEntriesList();
    List<DiffReportEntry> entries = new ArrayList<DiffReportEntry>();
    for (SnapshotDiffReportEntryProto entryProto : list) {
      DiffReportEntry entry = convert(entryProto);
      if (entry != null)
        entries.add(entry);
    }
    return new SnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot,
        entries);
  }
  
  public static SnapshotDiffReportProto convert(SnapshotDiffReport report) {
    if (report == null) {
      return null;
    }
    List<DiffReportEntry> entries = report.getDiffList();
    List<SnapshotDiffReportEntryProto> entryProtos = 
        new ArrayList<SnapshotDiffReportEntryProto>();
    for (DiffReportEntry entry : entries) {
      SnapshotDiffReportEntryProto entryProto = convert(entry);
      if (entryProto != null)
        entryProtos.add(entryProto);
    }
    
    SnapshotDiffReportProto reportProto = SnapshotDiffReportProto.newBuilder()
        .setSnapshotRoot(report.getSnapshotRoot())
        .setFromSnapshot(report.getFromSnapshot())
        .setToSnapshot(report.getLaterSnapshotName())
        .addAllDiffReportEntries(entryProtos).build();
    return reportProto;
  }

  public static DataChecksum.Type convert(HdfsProtos.ChecksumTypeProto type) {
    return DataChecksum.Type.valueOf(type.getNumber());
  }

  public static CacheDirectiveInfoProto convert
      (CacheDirectiveInfo info) {
    CacheDirectiveInfoProto.Builder builder = 
        CacheDirectiveInfoProto.newBuilder();
    if (info.getId() != null) {
      builder.setId(info.getId());
    }
    if (info.getPath() != null) {
      builder.setPath(info.getPath().toUri().getPath());
    }
    if (info.getReplication() != null) {
      builder.setReplication(info.getReplication());
    }
    if (info.getPool() != null) {
      builder.setPool(info.getPool());
    }
    if (info.getExpiration() != null) {
      builder.setExpiration(convert(info.getExpiration()));
    }
    return builder.build();
  }

  public static CacheDirectiveInfo convert
      (CacheDirectiveInfoProto proto) {
    CacheDirectiveInfo.Builder builder =
        new CacheDirectiveInfo.Builder();
    if (proto.hasId()) {
      builder.setId(proto.getId());
    }
    if (proto.hasPath()) {
      builder.setPath(new Path(proto.getPath()));
    }
    if (proto.hasReplication()) {
      builder.setReplication(Shorts.checkedCast(
          proto.getReplication()));
    }
    if (proto.hasPool()) {
      builder.setPool(proto.getPool());
    }
    if (proto.hasExpiration()) {
      builder.setExpiration(convert(proto.getExpiration()));
    }
    return builder.build();
  }

  public static CacheDirectiveInfoExpirationProto convert(
      CacheDirectiveInfo.Expiration expiration) {
    return CacheDirectiveInfoExpirationProto.newBuilder()
        .setIsRelative(expiration.isRelative())
        .setMillis(expiration.getMillis())
        .build();
  }

  public static CacheDirectiveInfo.Expiration convert(
      CacheDirectiveInfoExpirationProto proto) {
    if (proto.getIsRelative()) {
      return CacheDirectiveInfo.Expiration.newRelative(proto.getMillis());
    }
    return CacheDirectiveInfo.Expiration.newAbsolute(proto.getMillis());
  }

  public static CacheDirectiveStatsProto convert(CacheDirectiveStats stats) {
    CacheDirectiveStatsProto.Builder builder = 
        CacheDirectiveStatsProto.newBuilder();
    builder.setBytesNeeded(stats.getBytesNeeded());
    builder.setBytesCached(stats.getBytesCached());
    builder.setFilesNeeded(stats.getFilesNeeded());
    builder.setFilesCached(stats.getFilesCached());
    builder.setHasExpired(stats.hasExpired());
    return builder.build();
  }
  
  public static CacheDirectiveStats convert(CacheDirectiveStatsProto proto) {
    CacheDirectiveStats.Builder builder = new CacheDirectiveStats.Builder();
    builder.setBytesNeeded(proto.getBytesNeeded());
    builder.setBytesCached(proto.getBytesCached());
    builder.setFilesNeeded(proto.getFilesNeeded());
    builder.setFilesCached(proto.getFilesCached());
    builder.setHasExpired(proto.getHasExpired());
    return builder.build();
  }

  public static CacheDirectiveEntryProto convert(CacheDirectiveEntry entry) {
    CacheDirectiveEntryProto.Builder builder = 
        CacheDirectiveEntryProto.newBuilder();
    builder.setInfo(PBHelper.convert(entry.getInfo()));
    builder.setStats(PBHelper.convert(entry.getStats()));
    return builder.build();
  }
  
  public static CacheDirectiveEntry convert(CacheDirectiveEntryProto proto) {
    CacheDirectiveInfo info = PBHelper.convert(proto.getInfo());
    CacheDirectiveStats stats = PBHelper.convert(proto.getStats());
    return new CacheDirectiveEntry(info, stats);
  }

  public static CachePoolInfoProto convert(CachePoolInfo info) {
    CachePoolInfoProto.Builder builder = CachePoolInfoProto.newBuilder();
    builder.setPoolName(info.getPoolName());
    if (info.getOwnerName() != null) {
      builder.setOwnerName(info.getOwnerName());
    }
    if (info.getGroupName() != null) {
      builder.setGroupName(info.getGroupName());
    }
    if (info.getMode() != null) {
      builder.setMode(info.getMode().toShort());
    }
    if (info.getLimit() != null) {
      builder.setLimit(info.getLimit());
    }
    if (info.getMaxRelativeExpiryMs() != null) {
      builder.setMaxRelativeExpiry(info.getMaxRelativeExpiryMs());
    }
    return builder.build();
  }

  public static CachePoolInfo convert (CachePoolInfoProto proto) {
    // Pool name is a required field, the rest are optional
    String poolName = checkNotNull(proto.getPoolName());
    CachePoolInfo info = new CachePoolInfo(poolName);
    if (proto.hasOwnerName()) {
        info.setOwnerName(proto.getOwnerName());
    }
    if (proto.hasGroupName()) {
      info.setGroupName(proto.getGroupName());
    }
    if (proto.hasMode()) {
      info.setMode(new FsPermission((short)proto.getMode()));
    }
    if (proto.hasLimit())  {
      info.setLimit(proto.getLimit());
    }
    if (proto.hasMaxRelativeExpiry()) {
      info.setMaxRelativeExpiryMs(proto.getMaxRelativeExpiry());
    }
    return info;
  }

  public static CachePoolStatsProto convert(CachePoolStats stats) {
    CachePoolStatsProto.Builder builder = CachePoolStatsProto.newBuilder();
    builder.setBytesNeeded(stats.getBytesNeeded());
    builder.setBytesCached(stats.getBytesCached());
    builder.setBytesOverlimit(stats.getBytesOverlimit());
    builder.setFilesNeeded(stats.getFilesNeeded());
    builder.setFilesCached(stats.getFilesCached());
    return builder.build();
  }

  public static CachePoolStats convert (CachePoolStatsProto proto) {
    CachePoolStats.Builder builder = new CachePoolStats.Builder();
    builder.setBytesNeeded(proto.getBytesNeeded());
    builder.setBytesCached(proto.getBytesCached());
    builder.setBytesOverlimit(proto.getBytesOverlimit());
    builder.setFilesNeeded(proto.getFilesNeeded());
    builder.setFilesCached(proto.getFilesCached());
    return builder.build();
  }

  public static CachePoolEntryProto convert(CachePoolEntry entry) {
    CachePoolEntryProto.Builder builder = CachePoolEntryProto.newBuilder();
    builder.setInfo(PBHelper.convert(entry.getInfo()));
    builder.setStats(PBHelper.convert(entry.getStats()));
    return builder.build();
  }

  public static CachePoolEntry convert (CachePoolEntryProto proto) {
    CachePoolInfo info = PBHelper.convert(proto.getInfo());
    CachePoolStats stats = PBHelper.convert(proto.getStats());
    return new CachePoolEntry(info, stats);
  }
  
  public static HdfsProtos.ChecksumTypeProto convert(DataChecksum.Type type) {
    return HdfsProtos.ChecksumTypeProto.valueOf(type.id);
  }

  public static DatanodeLocalInfoProto convert(DatanodeLocalInfo info) {
    DatanodeLocalInfoProto.Builder builder = DatanodeLocalInfoProto.newBuilder();
    builder.setSoftwareVersion(info.getSoftwareVersion());
    builder.setConfigVersion(info.getConfigVersion());
    builder.setUptime(info.getUptime());
    return builder.build();
  }

  public static DatanodeLocalInfo convert(DatanodeLocalInfoProto proto) {
    return new DatanodeLocalInfo(proto.getSoftwareVersion(),
        proto.getConfigVersion(), proto.getUptime());
  }

  public static InputStream vintPrefixed(final InputStream input)
      throws IOException {
    final int firstByte = input.read();
    if (firstByte == -1) {
      throw new EOFException("Premature EOF: no length prefix available");
    }

    int size = CodedInputStream.readRawVarint32(firstByte, input);
    assert size >= 0;
    return new ExactSizeInputStream(input, size);
  }

  private static AclEntryScopeProto convert(AclEntryScope v) {
    return AclEntryScopeProto.valueOf(v.ordinal());
  }

  private static AclEntryScope convert(AclEntryScopeProto v) {
    return castEnum(v, ACL_ENTRY_SCOPE_VALUES);
  }

  private static AclEntryTypeProto convert(AclEntryType e) {
    return AclEntryTypeProto.valueOf(e.ordinal());
  }

  private static AclEntryType convert(AclEntryTypeProto v) {
    return castEnum(v, ACL_ENTRY_TYPE_VALUES);
  }
  
  private static XAttrNamespaceProto convert(XAttr.NameSpace v) {
    return XAttrNamespaceProto.valueOf(v.ordinal());
  }
  
  private static XAttr.NameSpace convert(XAttrNamespaceProto v) {
    return castEnum(v, XATTR_NAMESPACE_VALUES);
  }

  public static FsActionProto convert(FsAction v) {
    return FsActionProto.valueOf(v != null ? v.ordinal() : 0);
  }

  public static FsAction convert(FsActionProto v) {
    return castEnum(v, FSACTION_VALUES);
  }

  public static List<AclEntryProto> convertAclEntryProto(
      List<AclEntry> aclSpec) {
    ArrayList<AclEntryProto> r = Lists.newArrayListWithCapacity(aclSpec.size());
    for (AclEntry e : aclSpec) {
      AclEntryProto.Builder builder = AclEntryProto.newBuilder();
      builder.setType(convert(e.getType()));
      builder.setScope(convert(e.getScope()));
      builder.setPermissions(convert(e.getPermission()));
      if (e.getName() != null) {
        builder.setName(e.getName());
      }
      r.add(builder.build());
    }
    return r;
  }

  public static List<AclEntry> convertAclEntry(List<AclEntryProto> aclSpec) {
    ArrayList<AclEntry> r = Lists.newArrayListWithCapacity(aclSpec.size());
    for (AclEntryProto e : aclSpec) {
      AclEntry.Builder builder = new AclEntry.Builder();
      builder.setType(convert(e.getType()));
      builder.setScope(convert(e.getScope()));
      builder.setPermission(convert(e.getPermissions()));
      if (e.hasName()) {
        builder.setName(e.getName());
      }
      r.add(builder.build());
    }
    return r;
  }

  public static AclStatus convert(GetAclStatusResponseProto e) {
    AclStatusProto r = e.getResult();
    AclStatus.Builder builder = new AclStatus.Builder();
    builder.owner(r.getOwner()).group(r.getGroup()).stickyBit(r.getSticky())
        .addEntries(convertAclEntry(r.getEntriesList()));
    if (r.hasPermission()) {
      builder.setPermission(convert(r.getPermission()));
    }
    return builder.build();
  }

  public static GetAclStatusResponseProto convert(AclStatus e) {
    AclStatusProto.Builder builder = AclStatusProto.newBuilder();
    builder.setOwner(e.getOwner())
        .setGroup(e.getGroup()).setSticky(e.isStickyBit())
        .addAllEntries(convertAclEntryProto(e.getEntries()));
    if (e.getPermission() != null) {
      builder.setPermission(convert(e.getPermission()));
    }
    AclStatusProto r = builder.build();
    return GetAclStatusResponseProto.newBuilder().setResult(r).build();
  }
  
  public static XAttrProto convertXAttrProto(XAttr a) {
    XAttrProto.Builder builder = XAttrProto.newBuilder();
    builder.setNamespace(convert(a.getNameSpace()));
    if (a.getName() != null) {
      builder.setName(a.getName());
    }
    if (a.getValue() != null) {
      builder.setValue(getByteString(a.getValue()));
    }
    return builder.build();
  }
  
  public static List<XAttrProto> convertXAttrProto(
      List<XAttr> xAttrSpec) {
    if (xAttrSpec == null) {
      return Lists.newArrayListWithCapacity(0);
    }
    ArrayList<XAttrProto> xAttrs = Lists.newArrayListWithCapacity(
        xAttrSpec.size());
    for (XAttr a : xAttrSpec) {
      XAttrProto.Builder builder = XAttrProto.newBuilder();
      builder.setNamespace(convert(a.getNameSpace()));
      if (a.getName() != null) {
        builder.setName(a.getName());
      }
      if (a.getValue() != null) {
        builder.setValue(getByteString(a.getValue()));
      }
      xAttrs.add(builder.build());
    }
    return xAttrs;
  }
  
  /**
   * The flag field in PB is a bitmask whose values are the same a the 
   * emum values of XAttrSetFlag
   */
  public static int convert(EnumSet<XAttrSetFlag> flag) {
    int value = 0;
    if (flag.contains(XAttrSetFlag.CREATE)) {
      value |= XAttrSetFlagProto.XATTR_CREATE.getNumber();
    }
    if (flag.contains(XAttrSetFlag.REPLACE)) {
      value |= XAttrSetFlagProto.XATTR_REPLACE.getNumber();
    }
    return value;
  }
 
  public static EnumSet<XAttrSetFlag> convert(int flag) {
    EnumSet<XAttrSetFlag> result = 
        EnumSet.noneOf(XAttrSetFlag.class);
    if ((flag & XAttrSetFlagProto.XATTR_CREATE_VALUE) == 
        XAttrSetFlagProto.XATTR_CREATE_VALUE) {
      result.add(XAttrSetFlag.CREATE);
    }
    if ((flag & XAttrSetFlagProto.XATTR_REPLACE_VALUE) == 
        XAttrSetFlagProto.XATTR_REPLACE_VALUE) {
      result.add(XAttrSetFlag.REPLACE);
    }
    return result;
  }
  
  public static XAttr convertXAttr(XAttrProto a) {
    XAttr.Builder builder = new XAttr.Builder();
    builder.setNameSpace(convert(a.getNamespace()));
    if (a.hasName()) {
      builder.setName(a.getName());
    }
    if (a.hasValue()) {
      builder.setValue(a.getValue().toByteArray());
    }
    return builder.build();
  }
  
  public static List<XAttr> convertXAttrs(List<XAttrProto> xAttrSpec) {
    ArrayList<XAttr> xAttrs = Lists.newArrayListWithCapacity(xAttrSpec.size());
    for (XAttrProto a : xAttrSpec) {
      XAttr.Builder builder = new XAttr.Builder();
      builder.setNameSpace(convert(a.getNamespace()));
      if (a.hasName()) {
        builder.setName(a.getName());
      }
      if (a.hasValue()) {
        builder.setValue(a.getValue().toByteArray());
      }
      xAttrs.add(builder.build());
    }
    return xAttrs;
  }

  public static List<XAttr> convert(GetXAttrsResponseProto a) {
    List<XAttrProto> xAttrs = a.getXAttrsList();
    return convertXAttrs(xAttrs);
  }

  public static GetXAttrsResponseProto convertXAttrsResponse(
      List<XAttr> xAttrs) {
    GetXAttrsResponseProto.Builder builder = GetXAttrsResponseProto
        .newBuilder();
    if (xAttrs != null) {
      builder.addAllXAttrs(convertXAttrProto(xAttrs));
    }
    return builder.build();
  }

  public static List<XAttr> convert(ListXAttrsResponseProto a) {
    final List<XAttrProto> xAttrs = a.getXAttrsList();
    return convertXAttrs(xAttrs);
  }

  public static ListXAttrsResponseProto convertListXAttrsResponse(
    List<XAttr> names) {
    ListXAttrsResponseProto.Builder builder =
      ListXAttrsResponseProto.newBuilder();
    if (names != null) {
      builder.addAllXAttrs(convertXAttrProto(names));
    }
    return builder.build();
  }

  public static EncryptionZoneProto convert(EncryptionZone zone) {
    return EncryptionZoneProto.newBuilder()
        .setId(zone.getId())
        .setPath(zone.getPath())
        .setSuite(convert(zone.getSuite()))
        .setCryptoProtocolVersion(convert(zone.getVersion()))
        .setKeyName(zone.getKeyName())
        .build();
  }

  public static EncryptionZone convert(EncryptionZoneProto proto) {
    return new EncryptionZone(proto.getId(), proto.getPath(),
        convert(proto.getSuite()), convert(proto.getCryptoProtocolVersion()),
        proto.getKeyName());
  }

  public static ShortCircuitShmSlotProto convert(SlotId slotId) {
    return ShortCircuitShmSlotProto.newBuilder().
        setShmId(convert(slotId.getShmId())).
        setSlotIdx(slotId.getSlotIdx()).
        build();
  }

  public static ShortCircuitShmIdProto convert(ShmId shmId) {
    return ShortCircuitShmIdProto.newBuilder().
        setHi(shmId.getHi()).
        setLo(shmId.getLo()).
        build();

  }

  public static SlotId convert(ShortCircuitShmSlotProto slotId) {
    return new SlotId(PBHelper.convert(slotId.getShmId()),
        slotId.getSlotIdx());
  }

  public static ShmId convert(ShortCircuitShmIdProto shmId) {
    return new ShmId(shmId.getHi(), shmId.getLo());
  }

  private static Event.CreateEvent.INodeType createTypeConvert(InotifyProtos.INodeType
      type) {
    switch (type) {
    case I_TYPE_DIRECTORY:
      return Event.CreateEvent.INodeType.DIRECTORY;
    case I_TYPE_FILE:
      return Event.CreateEvent.INodeType.FILE;
    case I_TYPE_SYMLINK:
      return Event.CreateEvent.INodeType.SYMLINK;
    default:
      return null;
    }
  }

  private static InotifyProtos.MetadataUpdateType metadataUpdateTypeConvert(
      Event.MetadataUpdateEvent.MetadataType type) {
    switch (type) {
    case TIMES:
      return InotifyProtos.MetadataUpdateType.META_TYPE_TIMES;
    case REPLICATION:
      return InotifyProtos.MetadataUpdateType.META_TYPE_REPLICATION;
    case OWNER:
      return InotifyProtos.MetadataUpdateType.META_TYPE_OWNER;
    case PERMS:
      return InotifyProtos.MetadataUpdateType.META_TYPE_PERMS;
    case ACLS:
      return InotifyProtos.MetadataUpdateType.META_TYPE_ACLS;
    case XATTRS:
      return InotifyProtos.MetadataUpdateType.META_TYPE_XATTRS;
    default:
      return null;
    }
  }

  private static Event.MetadataUpdateEvent.MetadataType metadataUpdateTypeConvert(
      InotifyProtos.MetadataUpdateType type) {
    switch (type) {
    case META_TYPE_TIMES:
      return Event.MetadataUpdateEvent.MetadataType.TIMES;
    case META_TYPE_REPLICATION:
      return Event.MetadataUpdateEvent.MetadataType.REPLICATION;
    case META_TYPE_OWNER:
      return Event.MetadataUpdateEvent.MetadataType.OWNER;
    case META_TYPE_PERMS:
      return Event.MetadataUpdateEvent.MetadataType.PERMS;
    case META_TYPE_ACLS:
      return Event.MetadataUpdateEvent.MetadataType.ACLS;
    case META_TYPE_XATTRS:
      return Event.MetadataUpdateEvent.MetadataType.XATTRS;
    default:
      return null;
    }
  }

  private static InotifyProtos.INodeType createTypeConvert(Event.CreateEvent.INodeType
      type) {
    switch (type) {
    case DIRECTORY:
      return InotifyProtos.INodeType.I_TYPE_DIRECTORY;
    case FILE:
      return InotifyProtos.INodeType.I_TYPE_FILE;
    case SYMLINK:
      return InotifyProtos.INodeType.I_TYPE_SYMLINK;
    default:
      return null;
    }
  }

  public static EventBatchList convert(GetEditsFromTxidResponseProto resp) throws
    IOException {
    final InotifyProtos.EventsListProto list = resp.getEventsList();
    final long firstTxid = list.getFirstTxid();
    final long lastTxid = list.getLastTxid();

    List<EventBatch> batches = Lists.newArrayList();
    if (list.getEventsList().size() > 0) {
      throw new IOException("Can't handle old inotify server response.");
    }
    for (InotifyProtos.EventBatchProto bp : list.getBatchList()) {
      long txid = bp.getTxid();
      if ((txid != -1) && ((txid < firstTxid) || (txid > lastTxid))) {
        throw new IOException("Error converting TxidResponseProto: got a " +
            "transaction id " + txid + " that was outside the range of [" +
            firstTxid + ", " + lastTxid + "].");
      }
      List<Event> events = Lists.newArrayList();
      for (InotifyProtos.EventProto p : bp.getEventsList()) {
        switch (p.getType()) {
          case EVENT_CLOSE:
            InotifyProtos.CloseEventProto close =
                InotifyProtos.CloseEventProto.parseFrom(p.getContents());
            events.add(new Event.CloseEvent(close.getPath(),
                close.getFileSize(), close.getTimestamp()));
            break;
          case EVENT_CREATE:
            InotifyProtos.CreateEventProto create =
                InotifyProtos.CreateEventProto.parseFrom(p.getContents());
            events.add(new Event.CreateEvent.Builder()
                .iNodeType(createTypeConvert(create.getType()))
                .path(create.getPath())
                .ctime(create.getCtime())
                .ownerName(create.getOwnerName())
                .groupName(create.getGroupName())
                .perms(convert(create.getPerms()))
                .replication(create.getReplication())
                .symlinkTarget(create.getSymlinkTarget().isEmpty() ? null :
                    create.getSymlinkTarget())
                .defaultBlockSize(create.getDefaultBlockSize())
                .overwrite(create.getOverwrite()).build());
            break;
          case EVENT_METADATA:
            InotifyProtos.MetadataUpdateEventProto meta =
                InotifyProtos.MetadataUpdateEventProto.parseFrom(p.getContents());
            events.add(new Event.MetadataUpdateEvent.Builder()
                .path(meta.getPath())
                .metadataType(metadataUpdateTypeConvert(meta.getType()))
                .mtime(meta.getMtime())
                .atime(meta.getAtime())
                .replication(meta.getReplication())
                .ownerName(
                    meta.getOwnerName().isEmpty() ? null : meta.getOwnerName())
                .groupName(
                    meta.getGroupName().isEmpty() ? null : meta.getGroupName())
                .perms(meta.hasPerms() ? convert(meta.getPerms()) : null)
                .acls(meta.getAclsList().isEmpty() ? null : convertAclEntry(
                    meta.getAclsList()))
                .xAttrs(meta.getXAttrsList().isEmpty() ? null : convertXAttrs(
                    meta.getXAttrsList()))
                .xAttrsRemoved(meta.getXAttrsRemoved())
                .build());
            break;
          case EVENT_RENAME:
            InotifyProtos.RenameEventProto rename =
                InotifyProtos.RenameEventProto.parseFrom(p.getContents());
            events.add(new Event.RenameEvent.Builder()
                  .srcPath(rename.getSrcPath())
                  .dstPath(rename.getDestPath())
                  .timestamp(rename.getTimestamp())
                  .build());
            break;
          case EVENT_APPEND:
            InotifyProtos.AppendEventProto append =
                InotifyProtos.AppendEventProto.parseFrom(p.getContents());
            events.add(new Event.AppendEvent.Builder().path(append.getPath())
                .newBlock(append.hasNewBlock() && append.getNewBlock())
                .build());
            break;
          case EVENT_UNLINK:
            InotifyProtos.UnlinkEventProto unlink =
                InotifyProtos.UnlinkEventProto.parseFrom(p.getContents());
            events.add(new Event.UnlinkEvent.Builder()
                  .path(unlink.getPath())
                  .timestamp(unlink.getTimestamp())
                  .build());
            break;
          default:
            throw new RuntimeException("Unexpected inotify event type: " +
                p.getType());
        }
      }
      batches.add(new EventBatch(txid, events.toArray(new Event[0])));
    }
    return new EventBatchList(batches, resp.getEventsList().getFirstTxid(),
        resp.getEventsList().getLastTxid(), resp.getEventsList().getSyncTxid());
  }

  public static GetEditsFromTxidResponseProto convertEditsResponse(EventBatchList el) {
    InotifyProtos.EventsListProto.Builder builder =
        InotifyProtos.EventsListProto.newBuilder();
    for (EventBatch b : el.getBatches()) {
      List<InotifyProtos.EventProto> events = Lists.newArrayList();
      for (Event e : b.getEvents()) {
        switch (e.getEventType()) {
          case CLOSE:
            Event.CloseEvent ce = (Event.CloseEvent) e;
            events.add(InotifyProtos.EventProto.newBuilder()
                .setType(InotifyProtos.EventType.EVENT_CLOSE)
                .setContents(
                    InotifyProtos.CloseEventProto.newBuilder()
                        .setPath(ce.getPath())
                        .setFileSize(ce.getFileSize())
                        .setTimestamp(ce.getTimestamp()).build().toByteString()
                ).build());
            break;
          case CREATE:
            Event.CreateEvent ce2 = (Event.CreateEvent) e;
            events.add(InotifyProtos.EventProto.newBuilder()
                .setType(InotifyProtos.EventType.EVENT_CREATE)
                .setContents(
                    InotifyProtos.CreateEventProto.newBuilder()
                        .setType(createTypeConvert(ce2.getiNodeType()))
                        .setPath(ce2.getPath())
                        .setCtime(ce2.getCtime())
                        .setOwnerName(ce2.getOwnerName())
                        .setGroupName(ce2.getGroupName())
                        .setPerms(convert(ce2.getPerms()))
                        .setReplication(ce2.getReplication())
                        .setSymlinkTarget(ce2.getSymlinkTarget() == null ?
                            "" : ce2.getSymlinkTarget())
                        .setDefaultBlockSize(ce2.getDefaultBlockSize())
                        .setOverwrite(ce2.getOverwrite()).build().toByteString()
                ).build());
            break;
          case METADATA:
            Event.MetadataUpdateEvent me = (Event.MetadataUpdateEvent) e;
            InotifyProtos.MetadataUpdateEventProto.Builder metaB =
                InotifyProtos.MetadataUpdateEventProto.newBuilder()
                    .setPath(me.getPath())
                    .setType(metadataUpdateTypeConvert(me.getMetadataType()))
                    .setMtime(me.getMtime())
                    .setAtime(me.getAtime())
                    .setReplication(me.getReplication())
                    .setOwnerName(me.getOwnerName() == null ? "" :
                        me.getOwnerName())
                    .setGroupName(me.getGroupName() == null ? "" :
                        me.getGroupName())
                    .addAllAcls(me.getAcls() == null ?
                        Lists.<AclEntryProto>newArrayList() :
                        convertAclEntryProto(me.getAcls()))
                    .addAllXAttrs(me.getxAttrs() == null ?
                        Lists.<XAttrProto>newArrayList() :
                        convertXAttrProto(me.getxAttrs()))
                    .setXAttrsRemoved(me.isxAttrsRemoved());
            if (me.getPerms() != null) {
              metaB.setPerms(convert(me.getPerms()));
            }
            events.add(InotifyProtos.EventProto.newBuilder()
                .setType(InotifyProtos.EventType.EVENT_METADATA)
                .setContents(metaB.build().toByteString())
                .build());
            break;
          case RENAME:
            Event.RenameEvent re = (Event.RenameEvent) e;
            events.add(InotifyProtos.EventProto.newBuilder()
                .setType(InotifyProtos.EventType.EVENT_RENAME)
                .setContents(
                    InotifyProtos.RenameEventProto.newBuilder()
                        .setSrcPath(re.getSrcPath())
                        .setDestPath(re.getDstPath())
                        .setTimestamp(re.getTimestamp()).build().toByteString()
                ).build());
            break;
          case APPEND:
            Event.AppendEvent re2 = (Event.AppendEvent) e;
            events.add(InotifyProtos.EventProto.newBuilder()
                .setType(InotifyProtos.EventType.EVENT_APPEND)
                .setContents(InotifyProtos.AppendEventProto.newBuilder()
                    .setPath(re2.getPath())
                    .setNewBlock(re2.toNewBlock()).build().toByteString())
                .build());
            break;
          case UNLINK:
            Event.UnlinkEvent ue = (Event.UnlinkEvent) e;
            events.add(InotifyProtos.EventProto.newBuilder()
                .setType(InotifyProtos.EventType.EVENT_UNLINK)
                .setContents(
                    InotifyProtos.UnlinkEventProto.newBuilder()
                        .setPath(ue.getPath())
                        .setTimestamp(ue.getTimestamp()).build().toByteString()
                ).build());
            break;
          default:
            throw new RuntimeException("Unexpected inotify event: " + e);
        }
      }
      builder.addBatch(InotifyProtos.EventBatchProto.newBuilder().
          setTxid(b.getTxid()).
          addAllEvents(events));
    }
    builder.setFirstTxid(el.getFirstTxid());
    builder.setLastTxid(el.getLastTxid());
    builder.setSyncTxid(el.getSyncTxid());
    return GetEditsFromTxidResponseProto.newBuilder().setEventsList(
        builder.build()).build();
  }
  
  public static CipherOptionProto convert(CipherOption option) {
    if (option != null) {
      CipherOptionProto.Builder builder = CipherOptionProto.
          newBuilder();
      if (option.getCipherSuite() != null) {
        builder.setSuite(convert(option.getCipherSuite()));
      }
      if (option.getInKey() != null) {
        builder.setInKey(ByteString.copyFrom(option.getInKey()));
      }
      if (option.getInIv() != null) {
        builder.setInIv(ByteString.copyFrom(option.getInIv()));
      }
      if (option.getOutKey() != null) {
        builder.setOutKey(ByteString.copyFrom(option.getOutKey()));
      }
      if (option.getOutIv() != null) {
        builder.setOutIv(ByteString.copyFrom(option.getOutIv()));
      }
      return builder.build();
    }
    return null;
  }
  
  public static CipherOption convert(CipherOptionProto proto) {
    if (proto != null) {
      CipherSuite suite = null;
      if (proto.getSuite() != null) {
        suite = convert(proto.getSuite());
      }
      byte[] inKey = null;
      if (proto.getInKey() != null) {
        inKey = proto.getInKey().toByteArray();
      }
      byte[] inIv = null;
      if (proto.getInIv() != null) {
        inIv = proto.getInIv().toByteArray();
      }
      byte[] outKey = null;
      if (proto.getOutKey() != null) {
        outKey = proto.getOutKey().toByteArray();
      }
      byte[] outIv = null;
      if (proto.getOutIv() != null) {
        outIv = proto.getOutIv().toByteArray();
      }
      return new CipherOption(suite, inKey, inIv, outKey, outIv);
    }
    return null;
  }
  
  public static List<CipherOptionProto> convertCipherOptions(
      List<CipherOption> options) {
    if (options != null) {
      List<CipherOptionProto> protos = 
          Lists.newArrayListWithCapacity(options.size());
      for (CipherOption option : options) {
        protos.add(convert(option));
      }
      return protos;
    }
    return null;
  }
  
  public static List<CipherOption> convertCipherOptionProtos(
      List<CipherOptionProto> protos) {
    if (protos != null) {
      List<CipherOption> options = 
          Lists.newArrayListWithCapacity(protos.size());
      for (CipherOptionProto proto : protos) {
        options.add(convert(proto));
      }
      return options;
    }
    return null;
  }

  public static CipherSuiteProto convert(CipherSuite suite) {
    switch (suite) {
    case UNKNOWN:
      return CipherSuiteProto.UNKNOWN;
    case AES_CTR_NOPADDING:
      return CipherSuiteProto.AES_CTR_NOPADDING;
    default:
      return null;
    }
  }

  public static CipherSuite convert(CipherSuiteProto proto) {
    switch (proto) {
    case AES_CTR_NOPADDING:
      return CipherSuite.AES_CTR_NOPADDING;
    default:
      // Set to UNKNOWN and stash the unknown enum value
      CipherSuite suite = CipherSuite.UNKNOWN;
      suite.setUnknownValue(proto.getNumber());
      return suite;
    }
  }

  public static List<CryptoProtocolVersionProto> convert(
      CryptoProtocolVersion[] versions) {
    List<CryptoProtocolVersionProto> protos =
        Lists.newArrayListWithCapacity(versions.length);
    for (CryptoProtocolVersion v: versions) {
      protos.add(convert(v));
    }
    return protos;
  }

  public static CryptoProtocolVersion[] convertCryptoProtocolVersions(
      List<CryptoProtocolVersionProto> protos) {
    List<CryptoProtocolVersion> versions =
        Lists.newArrayListWithCapacity(protos.size());
    for (CryptoProtocolVersionProto p: protos) {
      versions.add(convert(p));
    }
    return versions.toArray(new CryptoProtocolVersion[] {});
  }

  public static CryptoProtocolVersion convert(CryptoProtocolVersionProto
      proto) {
    switch(proto) {
    case ENCRYPTION_ZONES:
      return CryptoProtocolVersion.ENCRYPTION_ZONES;
    default:
      // Set to UNKNOWN and stash the unknown enum value
      CryptoProtocolVersion version = CryptoProtocolVersion.UNKNOWN;
      version.setUnknownValue(proto.getNumber());
      return version;
    }
  }

  public static CryptoProtocolVersionProto convert(CryptoProtocolVersion
      version) {
    switch(version) {
    case UNKNOWN:
      return CryptoProtocolVersionProto.UNKNOWN_PROTOCOL_VERSION;
    case ENCRYPTION_ZONES:
      return CryptoProtocolVersionProto.ENCRYPTION_ZONES;
    default:
      return null;
    }
  }

  public static HdfsProtos.FileEncryptionInfoProto convert(
      FileEncryptionInfo info) {
    if (info == null) {
      return null;
    }
    return HdfsProtos.FileEncryptionInfoProto.newBuilder()
        .setSuite(convert(info.getCipherSuite()))
        .setCryptoProtocolVersion(convert(info.getCryptoProtocolVersion()))
        .setKey(getByteString(info.getEncryptedDataEncryptionKey()))
        .setIv(getByteString(info.getIV()))
        .setEzKeyVersionName(info.getEzKeyVersionName())
        .setKeyName(info.getKeyName())
        .build();
  }

  public static HdfsProtos.PerFileEncryptionInfoProto convertPerFileEncInfo(
      FileEncryptionInfo info) {
    if (info == null) {
      return null;
    }
    return HdfsProtos.PerFileEncryptionInfoProto.newBuilder()
        .setKey(getByteString(info.getEncryptedDataEncryptionKey()))
        .setIv(getByteString(info.getIV()))
        .setEzKeyVersionName(info.getEzKeyVersionName())
        .build();
  }

  public static HdfsProtos.ZoneEncryptionInfoProto convert(
      CipherSuite suite, CryptoProtocolVersion version, String keyName) {
    if (suite == null || version == null || keyName == null) {
      return null;
    }
    return HdfsProtos.ZoneEncryptionInfoProto.newBuilder()
        .setSuite(convert(suite))
        .setCryptoProtocolVersion(convert(version))
        .setKeyName(keyName)
        .build();
  }

  public static FileEncryptionInfo convert(
      HdfsProtos.FileEncryptionInfoProto proto) {
    if (proto == null) {
      return null;
    }
    CipherSuite suite = convert(proto.getSuite());
    CryptoProtocolVersion version = convert(proto.getCryptoProtocolVersion());
    byte[] key = proto.getKey().toByteArray();
    byte[] iv = proto.getIv().toByteArray();
    String ezKeyVersionName = proto.getEzKeyVersionName();
    String keyName = proto.getKeyName();
    return new FileEncryptionInfo(suite, version, key, iv, keyName,
        ezKeyVersionName);
  }

  public static FileEncryptionInfo convert(
      HdfsProtos.PerFileEncryptionInfoProto fileProto,
      CipherSuite suite, CryptoProtocolVersion version, String keyName) {
    if (fileProto == null || suite == null || version == null ||
        keyName == null) {
      return null;
    }
    byte[] key = fileProto.getKey().toByteArray();
    byte[] iv = fileProto.getIv().toByteArray();
    String ezKeyVersionName = fileProto.getEzKeyVersionName();
    return new FileEncryptionInfo(suite, version, key, iv, keyName,
        ezKeyVersionName);
  }

  public static List<Boolean> convert(boolean[] targetPinnings, int idx) {
    List<Boolean> pinnings = new ArrayList<Boolean>();
    if (targetPinnings == null) {
      pinnings.add(Boolean.FALSE);
    } else {
      for (; idx < targetPinnings.length; ++idx) {
        pinnings.add(Boolean.valueOf(targetPinnings[idx]));
      }
    }
    return pinnings;
  }

  public static boolean[] convertBooleanList(
    List<Boolean> targetPinningsList) {
    final boolean[] targetPinnings = new boolean[targetPinningsList.size()];
    for (int i = 0; i < targetPinningsList.size(); i++) {
      targetPinnings[i] = targetPinningsList.get(i);
    }
    return targetPinnings;
  }

  public static BlockReportContext convert(BlockReportContextProto proto) {
    return new BlockReportContext(proto.getTotalRpcs(),
        proto.getCurRpc(), proto.getId());
  }

  public static BlockReportContextProto convert(BlockReportContext context) {
    return BlockReportContextProto.newBuilder().
        setTotalRpcs(context.getTotalRpcs()).
        setCurRpc(context.getCurRpc()).
        setId(context.getReportId()).
        build();
  }
}
