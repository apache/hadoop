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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Shorts;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.CodedInputStream;

import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.BlockChecksumType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing.DiffReportListingEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.AclEntryScopeProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.AclEntryTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEntryProto.FsActionProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.FsPermissionProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockFlagProto;
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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.OpenFilesBatchResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.OpenFilesTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeActionProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SafeModeActionProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmIdProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmSlotProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.EncryptionZoneProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ReencryptActionProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ReencryptionStateProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ZoneReencryptionStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.AccessModeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.AddErasureCodingPolicyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockStoragePolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockTokenSecretProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ContentSummaryProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CorruptFileBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CryptoProtocolVersionProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DataEncryptionKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto.AdminState;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfosProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeLocalInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeStorageProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeStorageProto.StorageState;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.FsServerDefaultsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto.FileType;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.QuotaUsageProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RollingUpgradeStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotDiffReportListingEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotDiffReportListingProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotDiffReportEntryProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotDiffReportProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshottableDirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshottableDirectoryStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotListingProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshotStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageReportProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypesProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ZoneEncryptionInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.InotifyProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrProto.XAttrNamespaceProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrSetFlagProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.CodecProto;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Lists;

/**
 * Utilities for converting protobuf classes to and from hdfs-client side
 * implementation classes and other helper utilities to help in dealing with
 * protobuf.
 *
 * Note that when converting from an internal type to protobuf type, the
 * converter never return null for protobuf type. The check for internal type
 * being null must be done before calling the convert() method.
 */
public class PBHelperClient {
  private static final XAttr.NameSpace[] XATTR_NAMESPACE_VALUES =
      XAttr.NameSpace.values();
  private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES =
      AclEntryType.values();
  private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES =
      AclEntryScope.values();
  private static final FsAction[] FSACTION_VALUES =
      FsAction.values();

  private static ByteString getFixedByteString(String key) {
    return ShadedProtobufHelper.getFixedByteString(key);
  }

  /**
   * Guava cache for caching String to ByteString encoding. Use this when the
   * set of Strings is large, mutable, or unknown.
   */
  private static LoadingCache<String, ByteString> bytestringCache =
      CacheBuilder.newBuilder()
          .maximumSize(10000)
          .build(
              new CacheLoader<String, ByteString>() {
                public ByteString load(String key) {
                  return ByteString.copyFromUtf8(key);
                }
              });

  private PBHelperClient() {
    /** Hidden constructor */
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    // return singleton to reduce object allocation
    return ShadedProtobufHelper.getByteString(bytes);
  }

  public static ShmId convert(ShortCircuitShmIdProto shmId) {
    return new ShmId(shmId.getHi(), shmId.getLo());
  }

  public static DataChecksum.Type convert(HdfsProtos.ChecksumTypeProto type) {
    return DataChecksum.Type.valueOf(type.getNumber());
  }

  public static HdfsProtos.ChecksumTypeProto convert(DataChecksum.Type type) {
    return HdfsProtos.ChecksumTypeProto.forNumber(type.id);
  }

  public static HdfsProtos.BlockChecksumTypeProto convert(
      BlockChecksumType type) {
    switch(type) {
    case MD5CRC:
      return HdfsProtos.BlockChecksumTypeProto.MD5CRC;
    case COMPOSITE_CRC:
      return HdfsProtos.BlockChecksumTypeProto.COMPOSITE_CRC;
    default:
      throw new IllegalStateException(
          "BUG: BlockChecksumType not found, type=" + type);
    }
  }

  public static BlockChecksumType convert(
      HdfsProtos.BlockChecksumTypeProto blockChecksumTypeProto) {
    switch(blockChecksumTypeProto) {
    case MD5CRC:
      return BlockChecksumType.MD5CRC;
    case COMPOSITE_CRC:
      return BlockChecksumType.COMPOSITE_CRC;
    default:
      throw new IllegalStateException(
          "BUG: BlockChecksumTypeProto not found, type="
          + blockChecksumTypeProto);
    }
  }

  public static HdfsProtos.BlockChecksumOptionsProto convert(
      BlockChecksumOptions options) {
    return HdfsProtos.BlockChecksumOptionsProto.newBuilder()
        .setBlockChecksumType(convert(options.getBlockChecksumType()))
        .setStripeLength(options.getStripeLength())
        .build();
  }

  public static BlockChecksumOptions convert(
      HdfsProtos.BlockChecksumOptionsProto options) {
    return new BlockChecksumOptions(
        convert(options.getBlockChecksumType()),
        options.getStripeLength());
  }

  public static ExtendedBlockProto convert(final ExtendedBlock b) {
    if (b == null) return null;
    return ExtendedBlockProto.newBuilder().
        setPoolIdBytes(getFixedByteString(b.getBlockPoolId())).
        setBlockId(b.getBlockId()).
        setNumBytes(b.getNumBytes()).
        setGenerationStamp(b.getGenerationStamp()).
        build();
  }

  public static TokenProto convert(Token<?> tok) {
    return ShadedProtobufHelper.protoFromToken(tok);
  }

  public static ShortCircuitShmIdProto convert(ShmId shmId) {
    return ShortCircuitShmIdProto.newBuilder().
        setHi(shmId.getHi()).
        setLo(shmId.getLo()).
        build();

  }

  public static ShortCircuitShmSlotProto convert(SlotId slotId) {
    return ShortCircuitShmSlotProto.newBuilder().
        setShmId(convert(slotId.getShmId())).
        setSlotIdx(slotId.getSlotIdx()).
        build();
  }

  public static DatanodeIDProto convert(DatanodeID dn) {
    // For wire compatibility with older versions we transmit the StorageID
    // which is the same as the DatanodeUuid. Since StorageID is a required
    // field we pass the empty string if the DatanodeUuid is not yet known.
    return DatanodeIDProto.newBuilder()
        .setIpAddrBytes(dn.getIpAddrBytes())
        .setHostNameBytes(dn.getHostNameBytes())
        .setXferPort(dn.getXferPort())
        .setDatanodeUuidBytes(dn.getDatanodeUuidBytes())
        .setInfoPort(dn.getInfoPort())
        .setInfoSecurePort(dn.getInfoSecurePort())
        .setIpcPort(dn.getIpcPort()).build();
  }

  public static DatanodeInfoProto.AdminState convert(
      final DatanodeInfo.AdminStates inAs) {
    switch (inAs) {
    case NORMAL: return  DatanodeInfoProto.AdminState.NORMAL;
    case DECOMMISSION_INPROGRESS:
      return DatanodeInfoProto.AdminState.DECOMMISSION_INPROGRESS;
    case DECOMMISSIONED: return DatanodeInfoProto.AdminState.DECOMMISSIONED;
    case ENTERING_MAINTENANCE:
      return DatanodeInfoProto.AdminState.ENTERING_MAINTENANCE;
    case IN_MAINTENANCE:
      return DatanodeInfoProto.AdminState.IN_MAINTENANCE;
    default: return DatanodeInfoProto.AdminState.NORMAL;
    }
  }

  public static DatanodeInfoProto convert(DatanodeInfo info) {
    DatanodeInfoProto.Builder builder = DatanodeInfoProto.newBuilder();
    if (info.getNetworkLocation() != null) {
      builder.setLocationBytes(
          bytestringCache.getUnchecked(info.getNetworkLocation()));
    }
    if (info.getUpgradeDomain() != null) {
      builder.setUpgradeDomain(info.getUpgradeDomain());
    }
    if (info.getSoftwareVersion() != null) {
      builder.setSoftwareVersion(info.getSoftwareVersion());
    }
    builder
        .setId(convert((DatanodeID) info))
        .setCapacity(info.getCapacity())
        .setDfsUsed(info.getDfsUsed())
        .setNonDfsUsed(info.getNonDfsUsed())
        .setRemaining(info.getRemaining())
        .setBlockPoolUsed(info.getBlockPoolUsed())
        .setCacheCapacity(info.getCacheCapacity())
        .setCacheUsed(info.getCacheUsed())
        .setLastUpdate(info.getLastUpdate())
        .setLastUpdateMonotonic(info.getLastUpdateMonotonic())
        .setXceiverCount(info.getXceiverCount())
        .setAdminState(convert(info.getAdminState()))
        .setLastBlockReportTime(info.getLastBlockReportTime())
        .setLastBlockReportMonotonic(info.getLastBlockReportMonotonic())
        .setNumBlocks(info.getNumBlocks())
        .build();
    return builder.build();
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

  public static List<Boolean> convert(boolean[] targetPinnings, int idx) {
    List<Boolean> pinnings = new ArrayList<>();
    if (targetPinnings == null) {
      pinnings.add(Boolean.FALSE);
    } else {
      for (; idx < targetPinnings.length; ++idx) {
        pinnings.add(targetPinnings[idx]);
      }
    }
    return pinnings;
  }

  public static List<String> convert(String[] targetIds, int idx) {
    List<String> ids = new ArrayList<>();
    if (targetIds != null) {
      for (; idx < targetIds.length; ++idx) {
        ids.add(targetIds[idx]);
      }
    }
    return ids;
  }

  public static ExtendedBlock convert(ExtendedBlockProto eb) {
    if (eb == null) return null;
    return new ExtendedBlock( eb.getPoolId(), eb.getBlockId(), eb.getNumBytes(),
        eb.getGenerationStamp());
  }

  public static DatanodeLocalInfo convert(DatanodeLocalInfoProto proto) {
    return new DatanodeLocalInfo(proto.getSoftwareVersion(),
        proto.getConfigVersion(), proto.getUptime());
  }

  static public DatanodeInfoProto convertDatanodeInfo(DatanodeInfo di) {
    if (di == null) return null;
    return convert(di);
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
    case PROVIDED:
      return StorageTypeProto.PROVIDED;
    case NVDIMM:
      return StorageTypeProto.NVDIMM;
    default:
      throw new IllegalStateException(
          "BUG: StorageType not found, type=" + type);
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
    case PROVIDED:
      return StorageType.PROVIDED;
    case NVDIMM:
      return StorageType.NVDIMM;
    default:
      throw new IllegalStateException(
          "BUG: StorageTypeProto not found, type=" + type);
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
    final List<StorageTypeProto> protos = new ArrayList<>(
        types.length);
    for (int i = startIdx; i < types.length; ++i) {
      protos.add(convertStorageType(types[i]));
    }
    return protos;
  }

  public static InputStream vintPrefixed(final InputStream input)
      throws IOException {
    final int firstByte = input.read();
    if (firstByte == -1) {
      throw new EOFException(
          "Unexpected EOF while trying to read response from server");
    }

    int size = CodedInputStream.readRawVarint32(firstByte, input);
    assert size >= 0;
    return new LimitInputStream(input, size);
  }

  public static CipherOption convert(HdfsProtos.CipherOptionProto proto) {
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

  public static CipherSuite convert(HdfsProtos.CipherSuiteProto proto) {
    switch (proto) {
    case AES_CTR_NOPADDING:
      return CipherSuite.AES_CTR_NOPADDING;
    case SM4_CTR_NOPADDING:
      return CipherSuite.SM4_CTR_NOPADDING;
    default:
      // Set to UNKNOWN and stash the unknown enum value
      CipherSuite suite = CipherSuite.UNKNOWN;
      suite.setUnknownValue(proto.getNumber());
      return suite;
    }
  }

  public static HdfsProtos.CipherOptionProto convert(CipherOption option) {
    if (option != null) {
      HdfsProtos.CipherOptionProto.Builder builder =
          HdfsProtos.CipherOptionProto.newBuilder();
      if (option.getCipherSuite() != null) {
        builder.setSuite(convert(option.getCipherSuite()));
      }
      if (option.getInKey() != null) {
        builder.setInKey(getByteString(option.getInKey()));
      }
      if (option.getInIv() != null) {
        builder.setInIv(getByteString(option.getInIv()));
      }
      if (option.getOutKey() != null) {
        builder.setOutKey(getByteString(option.getOutKey()));
      }
      if (option.getOutIv() != null) {
        builder.setOutIv(getByteString(option.getOutIv()));
      }
      return builder.build();
    }
    return null;
  }

  public static HdfsProtos.CipherSuiteProto convert(CipherSuite suite) {
    switch (suite) {
    case UNKNOWN:
      return HdfsProtos.CipherSuiteProto.UNKNOWN;
    case AES_CTR_NOPADDING:
      return HdfsProtos.CipherSuiteProto.AES_CTR_NOPADDING;
    case SM4_CTR_NOPADDING:
      return HdfsProtos.CipherSuiteProto.SM4_CTR_NOPADDING;
    default:
      return null;
    }
  }

  public static List<HdfsProtos.CipherOptionProto> convertCipherOptions(
      List<CipherOption> options) {
    if (options != null) {
      List<HdfsProtos.CipherOptionProto> protos =
          Lists.newArrayListWithCapacity(options.size());
      for (CipherOption option : options) {
        protos.add(convert(option));
      }
      return protos;
    }
    return null;
  }

  public static List<CipherOption> convertCipherOptionProtos(
      List<HdfsProtos.CipherOptionProto> protos) {
    if (protos != null) {
      List<CipherOption> options =
          Lists.newArrayListWithCapacity(protos.size());
      for (HdfsProtos.CipherOptionProto proto : protos) {
        options.add(convert(proto));
      }
      return options;
    }
    return null;
  }

  public static LocatedBlock convertLocatedBlockProto(LocatedBlockProto proto) {
    if (proto == null) return null;
    List<DatanodeInfoProto> locs = proto.getLocsList();
    DatanodeInfo[] targets = new DatanodeInfo[locs.size()];
    for (int i = 0; i < locs.size(); i++) {
      targets[i] = convert(locs.get(i));
    }

    final StorageType[] storageTypes = convertStorageTypes(
        proto.getStorageTypesList(), locs.size());

    final int storageIDsCount = proto.getStorageIDsCount();
    final String[] storageIDs;
    if (storageIDsCount == 0) {
      storageIDs = null;
    } else {
      Preconditions.checkState(storageIDsCount == locs.size());
      storageIDs = proto.getStorageIDsList()
          .toArray(new String[storageIDsCount]);
    }

    byte[] indices = null;
    if (proto.hasBlockIndices()) {
      indices = proto.getBlockIndices().toByteArray();
    }

    // Set values from the isCached list, re-using references from loc
    List<DatanodeInfo> cachedLocs = new ArrayList<>(locs.size());
    List<Boolean> isCachedList = proto.getIsCachedList();
    for (int i=0; i<isCachedList.size(); i++) {
      if (isCachedList.get(i)) {
        cachedLocs.add(targets[i]);
      }
    }

    final LocatedBlock lb;
    if (indices == null) {
      lb = new LocatedBlock(PBHelperClient.convert(proto.getB()), targets,
          storageIDs, storageTypes, proto.getOffset(), proto.getCorrupt(),
          cachedLocs.toArray(new DatanodeInfo[cachedLocs.size()]));
    } else {
      lb = new LocatedStripedBlock(PBHelperClient.convert(proto.getB()),
          targets, storageIDs, storageTypes, indices, proto.getOffset(),
          proto.getCorrupt(),
          cachedLocs.toArray(new DatanodeInfo[cachedLocs.size()]));
      List<TokenProto> tokenProtos = proto.getBlockTokensList();
      Token<BlockTokenIdentifier>[] blockTokens =
          convertTokens(tokenProtos);
      ((LocatedStripedBlock) lb).setBlockTokens(blockTokens);
    }
    lb.setBlockToken(convert(proto.getBlockToken()));

    return lb;
  }

  static public Token<BlockTokenIdentifier>[] convertTokens(
      List<TokenProto> tokenProtos) {

    @SuppressWarnings("unchecked")
    Token<BlockTokenIdentifier>[] blockTokens = new Token[tokenProtos.size()];
    for (int i = 0; i < blockTokens.length; i++) {
      blockTokens[i] = convert(tokenProtos.get(i));
    }

    return blockTokens;
  }

  public static AccessModeProto convert(BlockTokenIdentifier.AccessMode aMode) {
    switch (aMode) {
    case READ: return AccessModeProto.READ;
    case WRITE: return AccessModeProto.WRITE;
    case COPY: return AccessModeProto.COPY;
    case REPLACE: return AccessModeProto.REPLACE;
    default:
      throw new IllegalArgumentException("Unexpected AccessMode: " + aMode);
    }
  }

  public static BlockTokenIdentifier.AccessMode convert(
      AccessModeProto accessModeProto) {
    switch (accessModeProto) {
    case READ: return BlockTokenIdentifier.AccessMode.READ;
    case WRITE: return BlockTokenIdentifier.AccessMode.WRITE;
    case COPY: return BlockTokenIdentifier.AccessMode.COPY;
    case REPLACE: return BlockTokenIdentifier.AccessMode.REPLACE;
    default:
      throw new IllegalArgumentException("Unexpected AccessModeProto: " +
          accessModeProto);
    }
  }

  public static BlockTokenSecretProto convert(
      BlockTokenIdentifier blockTokenSecret) {
    BlockTokenSecretProto.Builder builder =
        BlockTokenSecretProto.newBuilder();
    builder.setExpiryDate(blockTokenSecret.getExpiryDate());
    builder.setKeyId(blockTokenSecret.getKeyId());
    String userId = blockTokenSecret.getUserId();
    if (userId != null) {
      builder.setUserId(userId);
    }

    String blockPoolId = blockTokenSecret.getBlockPoolId();
    if (blockPoolId != null) {
      builder.setBlockPoolId(blockPoolId);
    }

    builder.setBlockId(blockTokenSecret.getBlockId());

    for (BlockTokenIdentifier.AccessMode aMode :
        blockTokenSecret.getAccessModes()) {
      builder.addModes(convert(aMode));
    }
    for (StorageType storageType : blockTokenSecret.getStorageTypes()) {
      builder.addStorageTypes(convertStorageType(storageType));
    }
    for (String storageId : blockTokenSecret.getStorageIds()) {
      builder.addStorageIds(storageId);
    }

    byte[] handshake = blockTokenSecret.getHandshakeMsg();
    if (handshake != null && handshake.length > 0) {
      builder.setHandshakeSecret(getByteString(handshake));
    }
    return builder.build();
  }

  static public DatanodeInfo convert(DatanodeInfoProto di) {
    if (di == null) {
      return null;
    }
    DatanodeInfoBuilder dinfo =
        new DatanodeInfoBuilder().setNodeID(convert(di.getId()))
            .setNetworkLocation(di.hasLocation() ? di.getLocation() : null)
            .setCapacity(di.getCapacity()).setDfsUsed(di.getDfsUsed())
            .setRemaining(di.getRemaining())
            .setBlockPoolUsed(di.getBlockPoolUsed())
            .setCacheCapacity(di.getCacheCapacity())
            .setCacheUsed(di.getCacheUsed()).setLastUpdate(di.getLastUpdate())
            .setLastUpdateMonotonic(di.getLastUpdateMonotonic())
            .setXceiverCount(di.getXceiverCount())
            .setAdminState(convert(di.getAdminState())).setUpgradeDomain(
            di.hasUpgradeDomain() ? di.getUpgradeDomain() : null)
            .setLastBlockReportTime(di.hasLastBlockReportTime() ?
                di.getLastBlockReportTime() : 0)
            .setLastBlockReportMonotonic(di.hasLastBlockReportMonotonic() ?
                di.getLastBlockReportMonotonic() : 0)
            .setNumBlocks(di.getNumBlocks())
            .setSoftwareVersion(di.getSoftwareVersion());

    if (di.hasNonDfsUsed()) {
      dinfo.setNonDfsUsed(di.getNonDfsUsed());
    } else {
      // use the legacy way for older datanodes
      long nonDFSUsed = di.getCapacity() - di.getDfsUsed() - di.getRemaining();
      dinfo.setNonDfsUsed(nonDFSUsed < 0 ? 0 : nonDFSUsed);
    }
    return dinfo.build();
  }

  public static StorageType[] convertStorageTypes(
      List<StorageTypeProto> storageTypesList, int expectedSize) {
    final StorageType[] storageTypes = new StorageType[expectedSize];
    if (storageTypesList.size() != expectedSize) {
      // missing storage types
      Preconditions.checkState(storageTypesList.isEmpty());
      Arrays.fill(storageTypes, StorageType.DEFAULT);
    } else {
      for (int i = 0; i < storageTypes.length; ++i) {
        storageTypes[i] = convertStorageType(storageTypesList.get(i));
      }
    }
    return storageTypes;
  }

  public static Token<BlockTokenIdentifier> convert(
      TokenProto blockToken) {
    return (Token<BlockTokenIdentifier>) ShadedProtobufHelper.tokenFromProto(
        blockToken);
  }

  // DatanodeId
  public static DatanodeID convert(DatanodeIDProto dn) {
    return new DatanodeID(dn.getIpAddr(), dn.getHostName(),
        dn.getDatanodeUuid(), dn.getXferPort(), dn.getInfoPort(),
        dn.hasInfoSecurePort() ? dn.getInfoSecurePort() : 0, dn.getIpcPort());
  }

  public static AdminStates convert(AdminState adminState) {
    switch(adminState) {
    case DECOMMISSION_INPROGRESS:
      return AdminStates.DECOMMISSION_INPROGRESS;
    case DECOMMISSIONED:
      return AdminStates.DECOMMISSIONED;
    case ENTERING_MAINTENANCE:
      return AdminStates.ENTERING_MAINTENANCE;
    case IN_MAINTENANCE:
      return AdminStates.IN_MAINTENANCE;
    case NORMAL:
    default:
      return AdminStates.NORMAL;
    }
  }

  // LocatedBlocks
  public static LocatedBlocks convert(LocatedBlocksProto lb) {
    return new LocatedBlocks(
        lb.getFileLength(), lb.getUnderConstruction(),
        convertLocatedBlocks(lb.getBlocksList()),
        lb.hasLastBlock() ?
            convertLocatedBlockProto(lb.getLastBlock()) : null,
        lb.getIsLastBlockComplete(),
        lb.hasFileEncryptionInfo() ? convert(lb.getFileEncryptionInfo()) : null,
        lb.hasEcPolicy() ? convertErasureCodingPolicy(lb.getEcPolicy()) : null);
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

  public static EventBatchList convert(GetEditsFromTxidResponseProto resp)
      throws IOException {
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
          Event.CreateEvent.Builder builder = new Event.CreateEvent.Builder()
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
                  .overwrite(create.getOverwrite());
          if (create.hasErasureCoded()) {
            builder.erasureCoded(create.getErasureCoded());
          }
          events.add(builder.build());
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
        case EVENT_TRUNCATE:
          InotifyProtos.TruncateEventProto truncate =
              InotifyProtos.TruncateEventProto.parseFrom(p.getContents());
          events.add(new Event.TruncateEvent(truncate.getPath(),
              truncate.getFileSize(), truncate.getTimestamp()));
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

  // Located Block Arrays and Lists
  public static LocatedBlockProto[] convertLocatedBlocks(LocatedBlock[] lb) {
    if (lb == null) return null;
    return convertLocatedBlocks2(Arrays.asList(lb))
        .toArray(new LocatedBlockProto[lb.length]);
  }

  public static LocatedBlock[] convertLocatedBlocks(LocatedBlockProto[] lb) {
    if (lb == null) return null;
    return convertLocatedBlocks(Arrays.asList(lb))
        .toArray(new LocatedBlock[lb.length]);
  }

  public static List<LocatedBlock> convertLocatedBlocks(
      List<LocatedBlockProto> lb) {
    if (lb == null) return null;
    final int len = lb.size();
    List<LocatedBlock> result = new ArrayList<>(len);
    for (LocatedBlockProto aLb : lb) {
      result.add(convertLocatedBlockProto(aLb));
    }
    return result;
  }

  public static List<LocatedBlockProto> convertLocatedBlocks2(
      List<LocatedBlock> lb) {
    if (lb == null) return null;
    final int len = lb.size();
    List<LocatedBlockProto> result = new ArrayList<>(len);
    for (LocatedBlock aLb : lb) {
      result.add(convertLocatedBlock(aLb));
    }
    return result;
  }

  public static LocatedBlockProto convertLocatedBlock(LocatedBlock b) {
    if (b == null) return null;
    Builder builder = LocatedBlockProto.newBuilder();
    DatanodeInfo[] locs = b.getLocations();
    List<DatanodeInfo> cachedLocs =
        Lists.newLinkedList(Arrays.asList(b.getCachedLocations()));
    for (int i = 0; i < locs.length; i++) {
      DatanodeInfo loc = locs[i];
      builder.addLocs(i, PBHelperClient.convert(loc));
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
      for (StorageType storageType : storageTypes) {
        builder.addStorageTypes(convertStorageType(storageType));
      }
    }
    final String[] storageIDs = b.getStorageIDs();
    if (storageIDs != null) {
      builder.addAllStorageIDs(Arrays.asList(storageIDs));
    }
    if (b instanceof LocatedStripedBlock) {
      LocatedStripedBlock sb = (LocatedStripedBlock) b;
      byte[] indices = sb.getBlockIndices();
      builder.setBlockIndices(PBHelperClient.getByteString(indices));
      Token<BlockTokenIdentifier>[] blockTokens = sb.getBlockTokens();
      builder.addAllBlockTokens(convert(blockTokens));
    }

    return builder.setB(PBHelperClient.convert(b.getBlock()))
        .setBlockToken(PBHelperClient.convert(b.getBlockToken()))
        .setCorrupt(b.isCorrupt()).setOffset(b.getStartOffset()).build();
  }

  public static List<TokenProto> convert(
      Token<BlockTokenIdentifier>[] blockTokens) {
    List<TokenProto> results = new ArrayList<>(blockTokens.length);
    for (Token<BlockTokenIdentifier> bt : blockTokens) {
      results.add(convert(bt));
    }

    return results;
  }

  public static List<Integer> convertBlockIndices(byte[] blockIndices) {
    List<Integer> results = new ArrayList<>(blockIndices.length);
    for (byte bt : blockIndices) {
      results.add(Integer.valueOf(bt));
    }
    return results;
  }

  public static byte[] convertBlockIndices(List<Integer> blockIndices) {
    byte[] blkIndices = new byte[blockIndices.size()];
    for (int i = 0; i < blockIndices.size(); i++) {
      blkIndices[i] = (byte) blockIndices.get(i).intValue();
    }
    return blkIndices;
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

  public static FsActionProto convert(FsAction v) {
    return FsActionProto.forNumber(v != null ? v.ordinal() : 0);
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

  public static List<XAttr> convert(ListXAttrsResponseProto a) {
    final List<XAttrProto> xAttrs = a.getXAttrsList();
    return convertXAttrs(xAttrs);
  }

  public static List<XAttr> convert(GetXAttrsResponseProto a) {
    List<XAttrProto> xAttrs = a.getXAttrsList();
    return convertXAttrs(xAttrs);
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

  static XAttrNamespaceProto convert(XAttr.NameSpace v) {
    return XAttrNamespaceProto.forNumber(v.ordinal());
  }

  static XAttr.NameSpace convert(XAttrNamespaceProto v) {
    return castEnum(v, XATTR_NAMESPACE_VALUES);
  }

  static <T extends Enum<T>, U extends Enum<U>> U castEnum(T from, U[] to) {
    return to[from.ordinal()];
  }

  static InotifyProtos.MetadataUpdateType metadataUpdateTypeConvert(
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

  static InotifyProtos.INodeType createTypeConvert(Event.CreateEvent.INodeType
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

  public static List<LocatedBlock> convertLocatedBlock(
      List<LocatedBlockProto> lb) {
    if (lb == null) return null;
    final int len = lb.size();
    List<LocatedBlock> result = new ArrayList<>(len);
    for (LocatedBlockProto aLb : lb) {
      result.add(convertLocatedBlockProto(aLb));
    }
    return result;
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

  static AclEntryScopeProto convert(AclEntryScope v) {
    return AclEntryScopeProto.forNumber(v.ordinal());
  }

  private static AclEntryScope convert(AclEntryScopeProto v) {
    return castEnum(v, ACL_ENTRY_SCOPE_VALUES);
  }

  static AclEntryTypeProto convert(AclEntryType e) {
    return AclEntryTypeProto.forNumber(e.ordinal());
  }

  private static AclEntryType convert(AclEntryTypeProto v) {
    return castEnum(v, ACL_ENTRY_TYPE_VALUES);
  }

  public static FsAction convert(FsActionProto v) {
    return castEnum(v, FSACTION_VALUES);
  }

  public static FsPermission convert(FsPermissionProto p) {
    return new FsPermission((short)p.getPerm());
  }

  private static Event.CreateEvent.INodeType createTypeConvert(
      InotifyProtos.INodeType type) {
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

  public static EncryptionZone convert(EncryptionZoneProto proto) {
    return new EncryptionZone(proto.getId(), proto.getPath(),
        convert(proto.getSuite()), convert(proto.getCryptoProtocolVersion()),
        proto.getKeyName());
  }

  public static OpenFilesBatchResponseProto convert(OpenFileEntry
      openFileEntry) {
    return OpenFilesBatchResponseProto.newBuilder()
        .setId(openFileEntry.getId())
        .setPath(openFileEntry.getFilePath())
        .setClientName(openFileEntry.getClientName())
        .setClientMachine(openFileEntry.getClientMachine())
        .build();
  }

  public static OpenFileEntry convert(OpenFilesBatchResponseProto proto) {
    return new OpenFileEntry(proto.getId(), proto.getPath(),
        proto.getClientName(), proto.getClientMachine());
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

  public static CachePoolEntry convert(CachePoolEntryProto proto) {
    CachePoolInfo info = convert(proto.getInfo());
    CachePoolStats stats = convert(proto.getStats());
    return new CachePoolEntry(info, stats);
  }

  public static CachePoolInfo convert (CachePoolInfoProto proto) {
    // Pool name is a required field, the rest are optional
    String poolName = Preconditions.checkNotNull(proto.getPoolName());
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
    if (proto.hasDefaultReplication()) {
      info.setDefaultReplication(Shorts.checkedCast(
          proto.getDefaultReplication()));
    }
    if (proto.hasMaxRelativeExpiry()) {
      info.setMaxRelativeExpiryMs(proto.getMaxRelativeExpiry());
    }
    return info;
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
    if (info.getDefaultReplication() != null) {
      builder.setDefaultReplication(info.getDefaultReplication());
    }
    if (info.getMaxRelativeExpiryMs() != null) {
      builder.setMaxRelativeExpiry(info.getMaxRelativeExpiryMs());
    }
    return builder.build();
  }

  public static CacheDirectiveInfoProto convert(CacheDirectiveInfo info) {
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

  public static CacheDirectiveInfoExpirationProto convert(
      CacheDirectiveInfo.Expiration expiration) {
    return CacheDirectiveInfoExpirationProto.newBuilder()
        .setIsRelative(expiration.isRelative())
        .setMillis(expiration.getMillis())
        .build();
  }

  public static CacheDirectiveEntry convert(CacheDirectiveEntryProto proto) {
    CacheDirectiveInfo info = convert(proto.getInfo());
    CacheDirectiveStats stats = convert(proto.getStats());
    return new CacheDirectiveEntry(info, stats);
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

  public static CacheDirectiveInfo convert(CacheDirectiveInfoProto proto) {
    CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
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

  public static CacheDirectiveInfo.Expiration convert(
      CacheDirectiveInfoExpirationProto proto) {
    if (proto.getIsRelative()) {
      return CacheDirectiveInfo.Expiration.newRelative(proto.getMillis());
    }
    return CacheDirectiveInfo.Expiration.newAbsolute(proto.getMillis());
  }

  public static int convertCacheFlags(EnumSet<CacheFlag> flags) {
    int value = 0;
    if (flags.contains(CacheFlag.FORCE)) {
      value |= CacheFlagProto.FORCE.getNumber();
    }
    return value;
  }

  public static SnapshotDiffReport convert(
      SnapshotDiffReportProto reportProto) {
    if (reportProto == null) {
      return null;
    }
    String snapshotDir = reportProto.getSnapshotRoot();
    String fromSnapshot = reportProto.getFromSnapshot();
    String toSnapshot = reportProto.getToSnapshot();
    List<SnapshotDiffReportEntryProto> list = reportProto
        .getDiffReportEntriesList();
    List<DiffReportEntry> entries = new ChunkedArrayList<>();
    for (SnapshotDiffReportEntryProto entryProto : list) {
      DiffReportEntry entry = convert(entryProto);
      if (entry != null)
        entries.add(entry);
    }
    return new SnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot,
        entries);
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

  public static SnapshotDiffReportListing convert(
      SnapshotDiffReportListingProto reportProto) {
    if (reportProto == null) {
      return null;
    }
    List<SnapshotDiffReportListingEntryProto> modifyList =
        reportProto.getModifiedEntriesList();
    List<DiffReportListingEntry> modifiedEntries = new ChunkedArrayList<>();
    for (SnapshotDiffReportListingEntryProto entryProto : modifyList) {
      DiffReportListingEntry entry = convert(entryProto);
      if (entry != null) {
        modifiedEntries.add(entry);
      }
    }
    List<SnapshotDiffReportListingEntryProto> createList =
        reportProto.getCreatedEntriesList();
    List<DiffReportListingEntry> createdEntries = new ChunkedArrayList<>();
    for (SnapshotDiffReportListingEntryProto entryProto : createList) {
      DiffReportListingEntry entry = convert(entryProto);
      if (entry != null) {
        createdEntries.add(entry);
      }
    }
    List<SnapshotDiffReportListingEntryProto> deletedList =
        reportProto.getDeletedEntriesList();
    List<DiffReportListingEntry> deletedEntries = new ChunkedArrayList<>();
    for (SnapshotDiffReportListingEntryProto entryProto : deletedList) {
      DiffReportListingEntry entry = convert(entryProto);
      if (entry != null) {
        deletedEntries.add(entry);
      }
    }
    byte[] startPath = reportProto.getCursor().getStartPath().toByteArray();
    boolean isFromEarlier = reportProto.getIsFromEarlier();

    int index = reportProto.getCursor().getIndex();
    return new SnapshotDiffReportListing(startPath, modifiedEntries,
        createdEntries, deletedEntries, index, isFromEarlier);
  }

  public static DiffReportListingEntry convert(
      SnapshotDiffReportListingEntryProto entry) {
    if (entry == null) {
      return null;
    }
    long dirId = entry.getDirId();
    long fileId = entry.getFileId();
    boolean isReference = entry.getIsReference();
    byte[] sourceName = entry.getFullpath().toByteArray();
    byte[] targetName =
        entry.hasTargetPath() ? entry.getTargetPath().toByteArray() : null;
    return new DiffReportListingEntry(dirId, fileId, sourceName, isReference,
        targetName);
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
        result[i] = convert(list.get(i));
      }
      return result;
    }
  }

  public static SnapshottableDirectoryStatus convert(
      SnapshottableDirectoryStatusProto sdirStatusProto) {
    if (sdirStatusProto == null) {
      return null;
    }
    final HdfsFileStatusProto status = sdirStatusProto.getDirStatus();
    EnumSet<HdfsFileStatus.Flags> flags = status.hasFlags()
        ? convertFlags(status.getFlags())
        : convertFlags(status.getPermission());
    return new SnapshottableDirectoryStatus(
        status.getModificationTime(),
        status.getAccessTime(),
        convert(status.getPermission()),
        flags,
        status.getOwner(),
        status.getGroup(),
        status.getPath().toByteArray(),
        status.getFileId(),
        status.getChildrenNum(),
        sdirStatusProto.getSnapshotNumber(),
        sdirStatusProto.getSnapshotQuota(),
        sdirStatusProto.getParentFullpath().toByteArray());
  }

  public static SnapshotStatus[] convert(
      HdfsProtos.SnapshotListingProto sdlp) {
    if (sdlp == null) {
      return null;
    }
    List<HdfsProtos.SnapshotStatusProto> list = sdlp
        .getSnapshotListingList();
    if (list.isEmpty()) {
      return new SnapshotStatus[0];
    } else {
      SnapshotStatus[] result =
          new SnapshotStatus[list.size()];
      for (int i = 0; i < list.size(); i++) {
        result[i] = convert(list.get(i));
      }
      return result;
    }
  }

  public static SnapshotStatus convert(
      HdfsProtos.SnapshotStatusProto sdirStatusProto) {
    if (sdirStatusProto == null) {
      return null;
    }
    final HdfsFileStatusProto status = sdirStatusProto.getDirStatus();
    EnumSet<HdfsFileStatus.Flags> flags = status.hasFlags()
        ? convertFlags(status.getFlags())
        : convertFlags(status.getPermission());
    return new SnapshotStatus(
        status.getModificationTime(),
        status.getAccessTime(),
        convert(status.getPermission()),
        flags,
        status.getOwner(),
        status.getGroup(),
        status.getPath().toByteArray(),
        status.getFileId(),
        status.getChildrenNum(),
        sdirStatusProto.getSnapshotID(),
        sdirStatusProto.getIsDeleted(),
        sdirStatusProto.getParentFullpath().toByteArray());
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

  public static Token<DelegationTokenIdentifier> convertDelegationToken(
      TokenProto blockToken) {
    return new Token<>(blockToken.getIdentifier()
        .toByteArray(), blockToken.getPassword().toByteArray(), new Text(
        blockToken.getKind()), new Text(blockToken.getService()));
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

  public static FsPermissionProto convert(FsPermission p) {
    return FsPermissionProto.newBuilder().setPerm(p.toShort()).build();
  }

  public static HdfsFileStatus convert(HdfsFileStatusProto fs) {
    if (fs == null) {
      return null;
    }
    EnumSet<HdfsFileStatus.Flags> flags = fs.hasFlags()
        ? convertFlags(fs.getFlags())
        : convertFlags(fs.getPermission());
    HdfsFileStatus hdfsFileStatus = new HdfsFileStatus.Builder()
        .length(fs.getLength())
        .isdir(fs.getFileType().equals(FileType.IS_DIR))
        .replication(fs.getBlockReplication())
        .blocksize(fs.getBlocksize())
        .mtime(fs.getModificationTime())
        .atime(fs.getAccessTime())
        .perm(convert(fs.getPermission()))
        .flags(flags)
        .owner(fs.getOwner())
        .group(fs.getGroup())
        .symlink(FileType.IS_SYMLINK.equals(fs.getFileType())
            ? fs.getSymlink().toByteArray()
            : null)
        .path(fs.getPath().toByteArray())
        .fileId(fs.hasFileId()
            ? fs.getFileId()
            : HdfsConstants.GRANDFATHER_INODE_ID)
        .locations(fs.hasLocations() ? convert(fs.getLocations()) : null)
        .children(fs.hasChildrenNum() ? fs.getChildrenNum() : -1)
        .feInfo(fs.hasFileEncryptionInfo()
            ? convert(fs.getFileEncryptionInfo())
            : null)
        .storagePolicy(fs.hasStoragePolicy()
            ? (byte) fs.getStoragePolicy()
            : HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED)
        .ecPolicy(fs.hasEcPolicy()
            ? convertErasureCodingPolicy(fs.getEcPolicy())
            : null)
        .build();
    if (fs.hasNamespace()) {
      hdfsFileStatus.setNamespace(fs.getNamespace());
    }
    return hdfsFileStatus;
  }

  private static EnumSet<HdfsFileStatus.Flags> convertFlags(int flags) {
    EnumSet<HdfsFileStatus.Flags> f =
        EnumSet.noneOf(HdfsFileStatus.Flags.class);
    for (HdfsFileStatusProto.Flags pbf : HdfsFileStatusProto.Flags.values()) {
      if ((pbf.getNumber() & flags) != 0) {
        switch (pbf) {
        case HAS_ACL:
          f.add(HdfsFileStatus.Flags.HAS_ACL);
          break;
        case HAS_CRYPT:
          f.add(HdfsFileStatus.Flags.HAS_CRYPT);
          break;
        case HAS_EC:
          f.add(HdfsFileStatus.Flags.HAS_EC);
          break;
        case SNAPSHOT_ENABLED:
          f.add(HdfsFileStatus.Flags.SNAPSHOT_ENABLED);
          break;
        default:
          // ignore unknown
          break;
        }
      }
    }
    return f;
  }

  private static EnumSet<HdfsFileStatus.Flags> convertFlags(
      FsPermissionProto pbp) {
    EnumSet<HdfsFileStatus.Flags> f =
        EnumSet.noneOf(HdfsFileStatus.Flags.class);
    FsPermission p = new FsPermissionExtension((short)pbp.getPerm());
    if (p.getAclBit()) {
      f.add(HdfsFileStatus.Flags.HAS_ACL);
    }
    if (p.getEncryptedBit()) {
      f.add(HdfsFileStatus.Flags.HAS_CRYPT);
    }
    if (p.getErasureCodedBit()) {
      f.add(HdfsFileStatus.Flags.HAS_EC);
    }
    return f;
  }

  public static CorruptFileBlocks convert(CorruptFileBlocksProto c) {
    if (c == null)
      return null;
    List<String> fileList = c.getFilesList();
    return new CorruptFileBlocks(fileList.toArray(new String[fileList.size()]),
        c.getCookie());
  }

  public static ContentSummary convert(ContentSummaryProto cs) {
    if (cs == null) return null;
    ContentSummary.Builder builder = new ContentSummary.Builder();
    builder.length(cs.getLength()).
        fileCount(cs.getFileCount()).
        directoryCount(cs.getDirectoryCount()).
        snapshotLength(cs.getSnapshotLength()).
        snapshotFileCount(cs.getSnapshotFileCount()).
        snapshotDirectoryCount(cs.getSnapshotDirectoryCount()).
        snapshotSpaceConsumed(cs.getSnapshotSpaceConsumed()).
        quota(cs.getQuota()).
        spaceConsumed(cs.getSpaceConsumed()).
        spaceQuota(cs.getSpaceQuota()).
        erasureCodingPolicy(cs.getErasureCodingPolicy());
    if (cs.hasTypeQuotaInfos()) {
      addStorageTypes(cs.getTypeQuotaInfos(), builder);
    }
    return builder.build();
  }

  public static QuotaUsage convert(QuotaUsageProto qu) {
    if (qu == null) {
      return null;
    }
    QuotaUsage.Builder builder = new QuotaUsage.Builder();
    builder.fileAndDirectoryCount(qu.getFileAndDirectoryCount()).
        quota(qu.getQuota()).
        spaceConsumed(qu.getSpaceConsumed()).
        spaceQuota(qu.getSpaceQuota());
    if (qu.hasTypeQuotaInfos()) {
      addStorageTypes(qu.getTypeQuotaInfos(), builder);
    }
    if (qu.hasPath()) {
      builder.setPath(qu.getPath());
    }
    return builder.build();
  }

  public static QuotaUsageProto convert(QuotaUsage qu) {
    if (qu == null) {
      return null;
    }
    QuotaUsageProto.Builder builder = QuotaUsageProto.newBuilder();
    builder.setFileAndDirectoryCount(qu.getFileAndDirectoryCount()).
        setQuota(qu.getQuota()).
        setSpaceConsumed(qu.getSpaceConsumed()).
        setSpaceQuota(qu.getSpaceQuota());
    if (qu.isTypeQuotaSet() || qu.isTypeConsumedAvailable()) {
      builder.setTypeQuotaInfos(getBuilder(qu));
    }
    if (qu.getPath() != null) {
      builder.setPath(qu.getPath());
    }
    return builder.build();
  }

  public static HdfsProtos.QuotaListingProto convert(QuotaUsage[] qus) {
    if (qus == null) {
      return null;
    }
    QuotaUsageProto[] protos =
        new HdfsProtos.QuotaUsageProto[qus.length];
    for (int i = 0; i < qus.length; i++) {
      protos[i] = convert(qus[i]);
    }
    List<QuotaUsageProto> protoList =
         Arrays.asList(protos);
    return HdfsProtos.QuotaListingProto.newBuilder()
        .addAllQuotaListing(protoList).build();
  }

  public static QuotaUsage[] convert(
      HdfsProtos.QuotaListingProto qtls) {
    if (qtls == null) {
      return null;
    }
    List<HdfsProtos.QuotaUsageProto> list = qtls
        .getQuotaListingList();
    if (list.isEmpty()) {
      return new QuotaUsage[0];
    } else {
      QuotaUsage[] result =
          new QuotaUsage[list.size()];
      for (int i = 0; i < list.size(); i++) {
        result[i] = convert(list.get(i));
      }
      return result;
    }
  }
  
  public static ReencryptActionProto convert(ReencryptAction a) {
    switch (a) {
    case CANCEL:
      return ReencryptActionProto.CANCEL_REENCRYPT;
    case START:
      return ReencryptActionProto.START_REENCRYPT;
    default:
      throw new IllegalArgumentException("Unexpected value: " + a);
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

  public static RollingUpgradeInfo convert(RollingUpgradeInfoProto proto) {
    RollingUpgradeStatusProto status = proto.getStatus();
    return new RollingUpgradeInfo(status.getBlockPoolId(),
        proto.getCreatedRollbackImages(),
        proto.getStartTime(), proto.getFinalizeTime());
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

  public static DatanodeStorageReport convertDatanodeStorageReport(
      DatanodeStorageReportProto proto) {
    return new DatanodeStorageReport(
        convert(proto.getDatanodeInfo()),
        convertStorageReports(proto.getStorageReportsList()));
  }

  public static StorageReport[] convertStorageReports(
      List<StorageReportProto> list) {
    final StorageReport[] report = new StorageReport[list.size()];
    for (int i = 0; i < report.length; i++) {
      report[i] = convert(list.get(i));
    }
    return report;
  }

  public static StorageReport convert(StorageReportProto p) {
    long nonDfsUsed = p.hasNonDfsUsed() ? p.getNonDfsUsed() : p.getCapacity()
        - p.getDfsUsed() - p.getRemaining();
    return new StorageReport(p.hasStorage() ? convert(p.getStorage())
        : new DatanodeStorage(p.getStorageUuid()), p.getFailed(),
        p.getCapacity(), p.getDfsUsed(), p.getRemaining(),
        p.getBlockPoolUsed(), nonDfsUsed, p.getMount());
  }

  public static DatanodeStorage convert(DatanodeStorageProto s) {
    return new DatanodeStorage(s.getStorageUuid(),
        convertState(s.getState()), convertStorageType(s.getStorageType()));
  }

  private static State convertState(StorageState state) {
    switch(state) {
    case READ_ONLY_SHARED:
      return State.READ_ONLY_SHARED;
    case NORMAL:
    default:
      return State.NORMAL;
    }
  }

  public static SafeModeActionProto convert(SafeModeAction a) {
    switch (a) {
    case SAFEMODE_LEAVE:
      return SafeModeActionProto.SAFEMODE_LEAVE;
    case SAFEMODE_ENTER:
      return SafeModeActionProto.SAFEMODE_ENTER;
    case SAFEMODE_GET:
      return SafeModeActionProto.SAFEMODE_GET;
    case SAFEMODE_FORCE_EXIT:
      return  SafeModeActionProto.SAFEMODE_FORCE_EXIT;
    default:
      throw new IllegalArgumentException("Unexpected SafeModeAction :" + a);
    }
  }

  public static DatanodeInfo[] convert(List<DatanodeInfoProto> list) {
    DatanodeInfo[] info = new DatanodeInfo[list.size()];
    for (int i = 0; i < info.length; i++) {
      info[i] = convert(list.get(i));
    }
    return info;
  }

  public static long[] convert(GetFsStatsResponseProto res) {
    long[] result = new long[ClientProtocol.STATS_ARRAY_LENGTH];
    result[ClientProtocol.GET_STATS_CAPACITY_IDX] = res.getCapacity();
    result[ClientProtocol.GET_STATS_USED_IDX] = res.getUsed();
    result[ClientProtocol.GET_STATS_REMAINING_IDX] = res.getRemaining();
    result[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] =
        res.getUnderReplicated();
    result[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] =
        res.getCorruptBlocks();
    result[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] =
        res.getMissingBlocks();
    result[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX] =
        res.getMissingReplOneBlocks();
    result[ClientProtocol.GET_STATS_BYTES_IN_FUTURE_BLOCKS_IDX] =
        res.hasBlocksInFuture() ? res.getBlocksInFuture() : 0;
    result[ClientProtocol.GET_STATS_PENDING_DELETION_BLOCKS_IDX] =
        res.getPendingDeletionBlocks();
    return result;
  }

  public static ReplicatedBlockStats convert(
      GetFsReplicatedBlockStatsResponseProto res) {
    if (res.hasHighestPrioLowRedundancyBlocks()) {
      return new ReplicatedBlockStats(res.getLowRedundancy(),
          res.getCorruptBlocks(), res.getMissingBlocks(),
          res.getMissingReplOneBlocks(), res.getBlocksInFuture(),
          res.getPendingDeletionBlocks(),
          res.getHighestPrioLowRedundancyBlocks());
    }
    return new ReplicatedBlockStats(res.getLowRedundancy(),
        res.getCorruptBlocks(), res.getMissingBlocks(),
        res.getMissingReplOneBlocks(), res.getBlocksInFuture(),
        res.getPendingDeletionBlocks());
  }

  public static ECBlockGroupStats convert(
      GetFsECBlockGroupStatsResponseProto res) {
    if (res.hasHighestPrioLowRedundancyBlocks()) {
      return new ECBlockGroupStats(res.getLowRedundancy(),
          res.getCorruptBlocks(), res.getMissingBlocks(),
          res.getBlocksInFuture(), res.getPendingDeletionBlocks(),
          res.getHighestPrioLowRedundancyBlocks());
    }
    return new ECBlockGroupStats(res.getLowRedundancy(),
        res.getCorruptBlocks(), res.getMissingBlocks(),
        res.getBlocksInFuture(), res.getPendingDeletionBlocks());
  }

  public static DatanodeReportTypeProto convert(DatanodeReportType t) {
    switch (t) {
    case ALL: return DatanodeReportTypeProto.ALL;
    case LIVE: return DatanodeReportTypeProto.LIVE;
    case DEAD: return DatanodeReportTypeProto.DEAD;
    case DECOMMISSIONING: return DatanodeReportTypeProto.DECOMMISSIONING;
    case ENTERING_MAINTENANCE:
      return DatanodeReportTypeProto.ENTERING_MAINTENANCE;
    case IN_MAINTENANCE:
      return DatanodeReportTypeProto.IN_MAINTENANCE;
    default:
      throw new IllegalArgumentException("Unexpected data type report:" + t);
    }
  }

  public static DirectoryListing convert(DirectoryListingProto dl) {
    if (dl == null)
      return null;
    List<HdfsFileStatusProto> partList =  dl.getPartialListingList();
    return new DirectoryListing(partList.isEmpty()
        ? new HdfsFileStatus[0]
        : convert(partList.toArray(new HdfsFileStatusProto[partList.size()])),
                  dl.getRemainingEntries());
  }

  public static HdfsFileStatus[] convert(HdfsFileStatusProto[] fs) {
    if (fs == null) return null;
    final int len = fs.length;
    HdfsFileStatus[] result = new HdfsFileStatus[len];
    for (int i = 0; i < len; ++i) {
      result[i] = convert(fs[i]);
    }
    return result;
  }

  public static List<HdfsFileStatus> convertHdfsFileStatus(
      List<HdfsFileStatusProto> fs) {
    if (fs == null) {
      return null;
    }
    List<HdfsFileStatus> result = Lists.newArrayListWithCapacity(fs.size());
    for (HdfsFileStatusProto proto : fs) {
      result.add(convert(proto));
    }
    return result;
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
    if (flag.contains(CreateFlag.SHOULD_REPLICATE)) {
      value |= CreateFlagProto.SHOULD_REPLICATE.getNumber();
    }
    return value;
  }

  public static FsServerDefaults convert(FsServerDefaultsProto fs) {
    if (fs == null) return null;
    return new FsServerDefaults(
        fs.getBlockSize(), fs.getBytesPerChecksum(),
        fs.getWritePacketSize(), (short) fs.getReplication(),
        fs.getFileBufferSize(),
        fs.getEncryptDataTransfer(),
        fs.getTrashInterval(),
        convert(fs.getChecksumType()),
        fs.hasKeyProviderUri() ? fs.getKeyProviderUri() : null,
        (byte) fs.getPolicyId(),
        fs.getSnapshotTrashRootEnabled());
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

  static List<StorageTypesProto> convert(StorageType[][] types) {
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

  public static BlockTypeProto convert(BlockType blockType) {
    switch (blockType) {
    case CONTIGUOUS: return BlockTypeProto.CONTIGUOUS;
    case STRIPED: return BlockTypeProto.STRIPED;
    default:
      throw new IllegalArgumentException("Unexpected block type: " + blockType);
    }
  }

  public static BlockType convert(BlockTypeProto blockType) {
    switch (blockType.getNumber()) {
    case BlockTypeProto.CONTIGUOUS_VALUE: return BlockType.CONTIGUOUS;
    case BlockTypeProto.STRIPED_VALUE: return BlockType.STRIPED;
    default:
      throw new IllegalArgumentException("Unexpected block type: " + blockType);
    }
  }

  static public DatanodeInfo[] convert(DatanodeInfoProto di[]) {
    if (di == null) return null;
    DatanodeInfo[] result = new DatanodeInfo[di.length];
    for (int i = 0; i < di.length; i++) {
      result[i] = convert(di[i]);
    }
    return result;
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
        = new ArrayList<>(reports.length);
    for (DatanodeStorageReport report : reports) {
      protos.add(convertDatanodeStorageReport(report));
    }
    return protos;
  }

  public static LocatedBlock[] convertLocatedBlock(LocatedBlockProto[] lb) {
    if (lb == null) return null;
    return convertLocatedBlock(Arrays.asList(lb)).toArray(
        new LocatedBlock[lb.length]);
  }

  public static LocatedBlocksProto convert(LocatedBlocks lb) {
    if (lb == null) {
      return null;
    }
    LocatedBlocksProto.Builder builder =
        LocatedBlocksProto.newBuilder();
    if (lb.getLastLocatedBlock() != null) {
      builder.setLastBlock(
          convertLocatedBlock(lb.getLastLocatedBlock()));
    }
    if (lb.getFileEncryptionInfo() != null) {
      builder.setFileEncryptionInfo(convert(lb.getFileEncryptionInfo()));
    }
    if (lb.getErasureCodingPolicy() != null) {
      builder.setEcPolicy(convertErasureCodingPolicy(
          lb.getErasureCodingPolicy()));
    }
    return builder.setFileLength(lb.getFileLength())
        .setUnderConstruction(lb.isUnderConstruction())
        .addAllBlocks(convertLocatedBlocks2(lb.getLocatedBlocks()))
        .setIsLastBlockComplete(lb.isLastBlockComplete()).build();
  }

  public static DataEncryptionKeyProto convert(DataEncryptionKey bet) {
    DataEncryptionKeyProto.Builder b = DataEncryptionKeyProto.newBuilder()
        .setKeyId(bet.keyId)
        .setBlockPoolId(bet.blockPoolId)
        .setNonce(getByteString(bet.nonce))
        .setEncryptionKey(getByteString(bet.encryptionKey))
        .setExpiryDate(bet.expiryDate);
    if (bet.encryptionAlgorithm != null) {
      b.setEncryptionAlgorithm(bet.encryptionAlgorithm);
    }
    return b.build();
  }

  public static FsServerDefaultsProto convert(FsServerDefaults fs) {
    if (fs == null) return null;
    FsServerDefaultsProto.Builder builder = FsServerDefaultsProto.newBuilder().
        setBlockSize(fs.getBlockSize()).
        setBytesPerChecksum(fs.getBytesPerChecksum()).
        setWritePacketSize(fs.getWritePacketSize())
        .setReplication(fs.getReplication())
        .setFileBufferSize(fs.getFileBufferSize())
        .setEncryptDataTransfer(fs.getEncryptDataTransfer())
        .setTrashInterval(fs.getTrashInterval())
        .setChecksumType(convert(fs.getChecksumType()))
        .setPolicyId(fs.getDefaultStoragePolicyId())
        .setSnapshotTrashRootEnabled(fs.getSnapshotTrashRootEnabled());
    if (fs.getKeyProviderUri() != null) {
      builder.setKeyProviderUri(fs.getKeyProviderUri());
    }
    return builder.build();
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
    if ((flag & CreateFlagProto.SHOULD_REPLICATE.getNumber())
        == CreateFlagProto.SHOULD_REPLICATE.getNumber()) {
      result.add(CreateFlag.SHOULD_REPLICATE);
    }
    return new EnumSetWritable<>(result, CreateFlag.class);
  }

  public static EnumSet<CacheFlag> convertCacheFlags(int flags) {
    EnumSet<CacheFlag> result = EnumSet.noneOf(CacheFlag.class);
    if ((flags & CacheFlagProto.FORCE_VALUE) == CacheFlagProto.FORCE_VALUE) {
      result.add(CacheFlag.FORCE);
    }
    return result;
  }

  public static HdfsFileStatusProto convert(HdfsFileStatus fs) {
    if (fs == null)
      return null;
    FileType fType = FileType.IS_FILE;
    if (fs.isDirectory()) {
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
            setPermission(convert(fs.getPermission())).
            setOwnerBytes(getFixedByteString(fs.getOwner())).
            setGroupBytes(getFixedByteString(fs.getGroup())).
            setFileId(fs.getFileId()).
            setChildrenNum(fs.getChildrenNum()).
            setPath(getByteString(fs.getLocalNameInBytes())).
            setStoragePolicy(fs.getStoragePolicy());
    if (fs.isSymlink())  {
      builder.setSymlink(getByteString(fs.getSymlinkInBytes()));
    }
    if (fs.getFileEncryptionInfo() != null) {
      builder.setFileEncryptionInfo(convert(fs.getFileEncryptionInfo()));
    }
    if (fs instanceof HdfsLocatedFileStatus) {
      final HdfsLocatedFileStatus lfs = (HdfsLocatedFileStatus) fs;
      LocatedBlocks locations = lfs.getLocatedBlocks();
      if (locations != null) {
        builder.setLocations(convert(locations));
      }
    }
    if(fs.getErasureCodingPolicy() != null) {
      builder.setEcPolicy(convertErasureCodingPolicy(
          fs.getErasureCodingPolicy()));
    }
    int flags = fs.hasAcl()   ? HdfsFileStatusProto.Flags.HAS_ACL_VALUE   : 0;
    flags |= fs.isEncrypted() ? HdfsFileStatusProto.Flags.HAS_CRYPT_VALUE : 0;
    flags |= fs.isErasureCoded() ? HdfsFileStatusProto.Flags.HAS_EC_VALUE : 0;
    flags |= fs.isSnapshotEnabled() ? HdfsFileStatusProto.Flags
        .SNAPSHOT_ENABLED_VALUE : 0;
    builder.setFlags(flags);
    if (fs.getNamespace() != null && !fs.getNamespace().isEmpty()) {
      builder.setNamespace(fs.getNamespace());
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
    ByteString parentFullPathBytes = getByteString(
        parentFullPath == null ? DFSUtilClient.EMPTY_BYTES : parentFullPath);
    HdfsFileStatusProto fs = convert(status.getDirStatus());
    SnapshottableDirectoryStatusProto.Builder builder =
        SnapshottableDirectoryStatusProto
            .newBuilder()
            .setSnapshotNumber(snapshotNumber)
            .setSnapshotQuota(snapshotQuota)
            .setParentFullpath(parentFullPathBytes)
            .setDirStatus(fs);
    return builder.build();
  }

  public static HdfsProtos.SnapshotStatusProto convert(SnapshotStatus status) {
    if (status == null) {
      return null;
    }
    byte[] parentFullPath = status.getParentFullPath();
    ByteString parentFullPathBytes = getByteString(
        parentFullPath == null ? DFSUtilClient.EMPTY_BYTES : parentFullPath);
    HdfsFileStatusProto fs = convert(status.getDirStatus());
    HdfsProtos.SnapshotStatusProto.Builder builder =
        HdfsProtos.SnapshotStatusProto
            .newBuilder()
            .setSnapshotID(status.getSnapshotID())
            .setParentFullpath(parentFullPathBytes)
            .setIsDeleted(status.isDeleted())
            .setDirStatus(fs);
    return builder.build();
  }

  public static HdfsFileStatusProto[] convert(HdfsFileStatus[] fs) {
    if (fs == null) return null;
    final int len = fs.length;
    HdfsFileStatusProto[] result = new HdfsFileStatusProto[len];
    for (int i = 0; i < len; ++i) {
      result[i] = convert(fs[i]);
    }
    return result;
  }

  public static DirectoryListingProto convert(DirectoryListing d) {
    if (d == null)
      return null;
    return DirectoryListingProto.newBuilder().
        addAllPartialListing(Arrays.asList(
            convert(d.getPartialListing()))).
        setRemainingEntries(d.getRemainingEntries()).
        build();
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
    if (fsStats.length >=
        ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX + 1)
      result.setMissingReplOneBlocks(
          fsStats[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX]);

    if (fsStats.length >=
        ClientProtocol.GET_STATS_BYTES_IN_FUTURE_BLOCKS_IDX + 1) {
      result.setBlocksInFuture(
          fsStats[ClientProtocol.GET_STATS_BYTES_IN_FUTURE_BLOCKS_IDX]);
    }
    if (fsStats.length >=
        ClientProtocol.GET_STATS_PENDING_DELETION_BLOCKS_IDX + 1) {
      result.setPendingDeletionBlocks(
          fsStats[ClientProtocol.GET_STATS_PENDING_DELETION_BLOCKS_IDX]);
    }
    return result.build();
  }

  public static GetFsReplicatedBlockStatsResponseProto convert(
      ReplicatedBlockStats replicatedBlockStats) {
    GetFsReplicatedBlockStatsResponseProto.Builder result =
        GetFsReplicatedBlockStatsResponseProto.newBuilder();
    result.setLowRedundancy(
        replicatedBlockStats.getLowRedundancyBlocks());
    result.setCorruptBlocks(
        replicatedBlockStats.getCorruptBlocks());
    result.setMissingBlocks(
        replicatedBlockStats.getMissingReplicaBlocks());
    result.setMissingReplOneBlocks(
        replicatedBlockStats.getMissingReplicationOneBlocks());
    result.setBlocksInFuture(
        replicatedBlockStats.getBytesInFutureBlocks());
    result.setPendingDeletionBlocks(
        replicatedBlockStats.getPendingDeletionBlocks());
    if (replicatedBlockStats.hasHighestPriorityLowRedundancyBlocks()) {
      result.setHighestPrioLowRedundancyBlocks(
          replicatedBlockStats.getHighestPriorityLowRedundancyBlocks());
    }
    return result.build();
  }

  public static GetFsECBlockGroupStatsResponseProto convert(
      ECBlockGroupStats ecBlockGroupStats) {
    GetFsECBlockGroupStatsResponseProto.Builder result =
        GetFsECBlockGroupStatsResponseProto.newBuilder();
    result.setLowRedundancy(
        ecBlockGroupStats.getLowRedundancyBlockGroups());
    result.setCorruptBlocks(ecBlockGroupStats.getCorruptBlockGroups());
    result.setMissingBlocks(ecBlockGroupStats.getMissingBlockGroups());
    result.setBlocksInFuture(
        ecBlockGroupStats.getBytesInFutureBlockGroups());
    result.setPendingDeletionBlocks(
        ecBlockGroupStats.getPendingDeletionBlocks());
    if (ecBlockGroupStats.hasHighestPriorityLowRedundancyBlocks()) {
      result.setHighestPrioLowRedundancyBlocks(
          ecBlockGroupStats.getHighestPriorityLowRedundancyBlocks());
    }
    return result.build();
  }

  public static DatanodeReportType convert(DatanodeReportTypeProto t) {
    switch (t) {
    case ALL: return DatanodeReportType.ALL;
    case LIVE: return DatanodeReportType.LIVE;
    case DEAD: return DatanodeReportType.DEAD;
    case DECOMMISSIONING: return DatanodeReportType.DECOMMISSIONING;
    case ENTERING_MAINTENANCE:
      return DatanodeReportType.ENTERING_MAINTENANCE;
    case IN_MAINTENANCE:
      return DatanodeReportType.IN_MAINTENANCE;
    default:
      throw new IllegalArgumentException("Unexpected data type report:" + t);
    }
  }

  public static SafeModeAction convert(
      SafeModeActionProto a) {
    switch (a) {
    case SAFEMODE_LEAVE:
      return SafeModeAction.SAFEMODE_LEAVE;
    case SAFEMODE_ENTER:
      return SafeModeAction.SAFEMODE_ENTER;
    case SAFEMODE_GET:
      return SafeModeAction.SAFEMODE_GET;
    case SAFEMODE_FORCE_EXIT:
      return  SafeModeAction.SAFEMODE_FORCE_EXIT;
    default:
      throw new IllegalArgumentException("Unexpected SafeModeAction :" + a);
    }
  }

  public static ReencryptAction convert(ReencryptActionProto a) {
    switch (a) {
    case CANCEL_REENCRYPT:
      return ReencryptAction.CANCEL;
    case START_REENCRYPT:
      return ReencryptAction.START;
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

  public static CorruptFileBlocksProto convert(CorruptFileBlocks c) {
    if (c == null)
      return null;
    return CorruptFileBlocksProto.newBuilder().
        addAllFiles(Arrays.asList(c.getFiles())).
        setCookie(c.getCookie()).
        build();
  }

  public static ContentSummaryProto convert(ContentSummary cs) {
    if (cs == null) return null;
    ContentSummaryProto.Builder builder = ContentSummaryProto.newBuilder();
    builder.setLength(cs.getLength()).
        setFileCount(cs.getFileCount()).
        setDirectoryCount(cs.getDirectoryCount()).
        setSnapshotLength(cs.getSnapshotLength()).
        setSnapshotFileCount(cs.getSnapshotFileCount()).
        setSnapshotDirectoryCount(cs.getSnapshotDirectoryCount()).
        setSnapshotSpaceConsumed(cs.getSnapshotSpaceConsumed()).
        setQuota(cs.getQuota()).
        setSpaceConsumed(cs.getSpaceConsumed()).
        setSpaceQuota(cs.getSpaceQuota()).
        setErasureCodingPolicy(cs.getErasureCodingPolicy());

    if (cs.isTypeQuotaSet() || cs.isTypeConsumedAvailable()) {
      builder.setTypeQuotaInfos(getBuilder(cs));
    }
    return builder.build();
  }

  private static void addStorageTypes(
      HdfsProtos.StorageTypeQuotaInfosProto typeQuotaInfos,
      QuotaUsage.Builder builder) {
    for (HdfsProtos.StorageTypeQuotaInfoProto info :
        typeQuotaInfos.getTypeQuotaInfoList()) {
      StorageType type = convertStorageType(info.getType());
      builder.typeConsumed(type, info.getConsumed());
      builder.typeQuota(type, info.getQuota());
    }
  }

  private static HdfsProtos.StorageTypeQuotaInfosProto.Builder getBuilder(
      QuotaUsage qu) {
    HdfsProtos.StorageTypeQuotaInfosProto.Builder isb =
            HdfsProtos.StorageTypeQuotaInfosProto.newBuilder();
    for (StorageType t: StorageType.getTypesSupportingQuota()) {
      HdfsProtos.StorageTypeQuotaInfoProto info =
          HdfsProtos.StorageTypeQuotaInfoProto.newBuilder().
              setType(convertStorageType(t)).
              setConsumed(qu.getTypeConsumed(t)).
              setQuota(qu.getTypeQuota(t)).
              build();
      isb.addTypeQuotaInfo(info);
    }
    return isb;
  }

  public static DatanodeStorageProto convert(DatanodeStorage s) {
    return DatanodeStorageProto.newBuilder()
        .setState(convertState(s.getState()))
        .setStorageType(convertStorageType(s.getStorageType()))
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

  public static StorageReportProto convert(StorageReport r) {
    StorageReportProto.Builder builder = StorageReportProto.newBuilder()
        .setBlockPoolUsed(r.getBlockPoolUsed()).setCapacity(r.getCapacity())
        .setDfsUsed(r.getDfsUsed()).setRemaining(r.getRemaining())
        .setStorageUuid(r.getStorage().getStorageID())
        .setStorage(convert(r.getStorage()))
        .setNonDfsUsed(r.getNonDfsUsed())
        .setMount(r.getMount());
    return builder.build();
  }

  public static List<StorageReportProto> convertStorageReports(
      StorageReport[] storages) {
    final List<StorageReportProto> protos = new ArrayList<>(storages.length);
    for (StorageReport storage : storages) {
      protos.add(convert(storage));
    }
    return protos;
  }

  public static SnapshottableDirectoryListingProto convert(
      SnapshottableDirectoryStatus[] status) {
    if (status == null)
      return null;
    SnapshottableDirectoryStatusProto[] protos =
        new SnapshottableDirectoryStatusProto[status.length];
    for (int i = 0; i < status.length; i++) {
      protos[i] = convert(status[i]);
    }
    List<SnapshottableDirectoryStatusProto> protoList = Arrays.asList(protos);
    return SnapshottableDirectoryListingProto.newBuilder()
        .addAllSnapshottableDirListing(protoList).build();
  }

  public static HdfsProtos.SnapshotListingProto convert(
      SnapshotStatus[] status) {
    if (status == null) {
      return null;
    }
    HdfsProtos.SnapshotStatusProto[] protos =
        new HdfsProtos.SnapshotStatusProto[status.length];
    for (int i = 0; i < status.length; i++) {
      protos[i] = convert(status[i]);
    }
    List<SnapshotStatusProto> protoList = Arrays.asList(protos);
    return SnapshotListingProto.newBuilder()
        .addAllSnapshotListing(protoList).build();
  }

  public static SnapshotDiffReportEntryProto convert(DiffReportEntry entry) {
    if (entry == null) {
      return null;
    }
    ByteString sourcePath = getByteString(entry.getSourcePath() == null ?
        DFSUtilClient.EMPTY_BYTES : entry.getSourcePath());
    String modification = entry.getType().getLabel();
    SnapshotDiffReportEntryProto.Builder builder = SnapshotDiffReportEntryProto
        .newBuilder().setFullpath(sourcePath)
        .setModificationLabel(modification);
    if (entry.getType() == DiffType.RENAME) {
      ByteString targetPath =
          getByteString(entry.getTargetPath() == null ?
              DFSUtilClient.EMPTY_BYTES : entry.getTargetPath());
      builder.setTargetPath(targetPath);
    }
    return builder.build();
  }

  public static SnapshotDiffReportListingEntryProto convert(
      DiffReportListingEntry entry) {
    if (entry == null) {
      return null;
    }
    ByteString sourcePath = getByteString(
        entry.getSourcePath() == null ? DFSUtilClient.EMPTY_BYTES :
            DFSUtilClient.byteArray2bytes(entry.getSourcePath()));
    long dirId = entry.getDirId();
    long fileId = entry.getFileId();
    boolean isReference = entry.isReference();
    ByteString targetPath = getByteString(
        entry.getTargetPath() == null ? DFSUtilClient.EMPTY_BYTES :
            DFSUtilClient.byteArray2bytes(entry.getTargetPath()));
    SnapshotDiffReportListingEntryProto.Builder builder =
        SnapshotDiffReportListingEntryProto.newBuilder().setFullpath(sourcePath)
            .setDirId(dirId).setFileId(fileId).setIsReference(isReference)
            .setTargetPath(targetPath);
    return builder.build();
  }

  public static SnapshotDiffReportListingProto convert(
      SnapshotDiffReportListing report) {
    if (report == null) {
      return null;
    }
    ByteString startPath = getByteString(
        report.getLastPath() == null ? DFSUtilClient.EMPTY_BYTES :
            report.getLastPath());
    List<DiffReportListingEntry> modifiedEntries = report.getModifyList();
    List<DiffReportListingEntry> createdEntries = report.getCreateList();
    List<DiffReportListingEntry> deletedEntries = report.getDeleteList();
    List<SnapshotDiffReportListingEntryProto> modifiedEntryProtos =
        new ChunkedArrayList<>();
    for (DiffReportListingEntry entry : modifiedEntries) {
      SnapshotDiffReportListingEntryProto entryProto = convert(entry);
      if (entryProto != null) {
        modifiedEntryProtos.add(entryProto);
      }
    }
    List<SnapshotDiffReportListingEntryProto> createdEntryProtos =
        new ChunkedArrayList<>();
    for (DiffReportListingEntry entry : createdEntries) {
      SnapshotDiffReportListingEntryProto entryProto = convert(entry);
      if (entryProto != null) {
        createdEntryProtos.add(entryProto);
      }
    }
    List<SnapshotDiffReportListingEntryProto> deletedEntryProtos =
        new ChunkedArrayList<>();
    for (DiffReportListingEntry entry : deletedEntries) {
      SnapshotDiffReportListingEntryProto entryProto = convert(entry);
      if (entryProto != null) {
        deletedEntryProtos.add(entryProto);
      }
    }

    return SnapshotDiffReportListingProto.newBuilder()
        .addAllModifiedEntries(modifiedEntryProtos)
        .addAllCreatedEntries(createdEntryProtos)
        .addAllDeletedEntries(deletedEntryProtos)
        .setIsFromEarlier(report.getIsFromEarlier())
        .setCursor(HdfsProtos.SnapshotDiffReportCursorProto.newBuilder()
        .setStartPath(startPath)
        .setIndex(report.getLastIndex()).build())
        .build();
  }

  public static SnapshotDiffReportProto convert(SnapshotDiffReport report) {
    if (report == null) {
      return null;
    }
    List<DiffReportEntry> entries = report.getDiffList();
    List<SnapshotDiffReportEntryProto> entryProtos = new ChunkedArrayList<>();
    for (DiffReportEntry entry : entries) {
      SnapshotDiffReportEntryProto entryProto = convert(entry);
      if (entryProto != null)
        entryProtos.add(entryProto);
    }

    return SnapshotDiffReportProto.newBuilder()
        .setSnapshotRoot(report.getSnapshotRoot())
        .setFromSnapshot(report.getFromSnapshot())
        .setToSnapshot(report.getLaterSnapshotName())
        .addAllDiffReportEntries(entryProtos).build();
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

  public static CacheDirectiveEntryProto convert(CacheDirectiveEntry entry) {
    CacheDirectiveEntryProto.Builder builder =
        CacheDirectiveEntryProto.newBuilder();
    builder.setInfo(convert(entry.getInfo()));
    builder.setStats(convert(entry.getStats()));
    return builder.build();
  }

  public static boolean[] convertBooleanList(
      List<Boolean> targetPinningsList) {
    final boolean[] targetPinnings = new boolean[targetPinningsList.size()];
    for (int i = 0; i < targetPinningsList.size(); i++) {
      targetPinnings[i] = targetPinningsList.get(i);
    }
    return targetPinnings;
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

  public static CachePoolEntryProto convert(CachePoolEntry entry) {
    CachePoolEntryProto.Builder builder = CachePoolEntryProto.newBuilder();
    builder.setInfo(convert(entry.getInfo()));
    builder.setStats(convert(entry.getStats()));
    return builder.build();
  }

  public static DatanodeLocalInfoProto convert(DatanodeLocalInfo info) {
    DatanodeLocalInfoProto.Builder builder =
        DatanodeLocalInfoProto.newBuilder();
    builder.setSoftwareVersion(info.getSoftwareVersion());
    builder.setConfigVersion(info.getConfigVersion());
    builder.setUptime(info.getUptime());
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

  public static GetXAttrsResponseProto convertXAttrsResponse(
      List<XAttr> xAttrs) {
    GetXAttrsResponseProto.Builder builder = GetXAttrsResponseProto
        .newBuilder();
    if (xAttrs != null) {
      builder.addAllXAttrs(convertXAttrProto(xAttrs));
    }
    return builder.build();
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

  public static SlotId convert(ShortCircuitShmSlotProto slotId) {
    return new SlotId(convert(slotId.getShmId()),
        slotId.getSlotIdx());
  }

  public static GetEditsFromTxidResponseProto convertEditsResponse(
      EventBatchList el) {
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
          InotifyProtos.CreateEventProto.Builder pB =
                  (InotifyProtos.CreateEventProto.newBuilder());
          pB.setType(createTypeConvert(ce2.getiNodeType()))
             .setPath(ce2.getPath())
             .setCtime(ce2.getCtime())
             .setOwnerName(ce2.getOwnerName())
             .setGroupName(ce2.getGroupName())
             .setPerms(convert(ce2.getPerms()))
             .setReplication(ce2.getReplication())
             .setSymlinkTarget(ce2.getSymlinkTarget() == null ?
                        "" : ce2.getSymlinkTarget())
             .setDefaultBlockSize(ce2.getDefaultBlockSize())
             .setOverwrite(ce2.getOverwrite());
          if (ce2.isErasureCoded().isPresent()) {
            pB.setErasureCoded(ce2.isErasureCoded().get());
          }
          events.add(InotifyProtos.EventProto.newBuilder()
              .setType(InotifyProtos.EventType.EVENT_CREATE)
              .setContents(pB.build().toByteString())
              .build());
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
        case TRUNCATE:
          Event.TruncateEvent te = (Event.TruncateEvent) e;
          events.add(InotifyProtos.EventProto.newBuilder()
              .setType(InotifyProtos.EventType.EVENT_TRUNCATE)
              .setContents(
                  InotifyProtos.TruncateEventProto.newBuilder()
                      .setPath(te.getPath())
                      .setFileSize(te.getFileSize())
                      .setTimestamp(te.getTimestamp()).build().toByteString()
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

  public static CryptoProtocolVersion[] convertCryptoProtocolVersions(
      List<CryptoProtocolVersionProto> protos) {
    List<CryptoProtocolVersion> versions =
        Lists.newArrayListWithCapacity(protos.size());
    for (CryptoProtocolVersionProto p: protos) {
      versions.add(convert(p));
    }
    return versions.toArray(new CryptoProtocolVersion[]{});
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

  public static ZoneEncryptionInfoProto convert(CipherSuite suite,
      CryptoProtocolVersion version, String keyName) {
    return convert(suite, version, keyName, null);
  }

  public static ZoneEncryptionInfoProto convert(CipherSuite suite,
      CryptoProtocolVersion version, String keyName,
      ReencryptionInfoProto proto) {
    if (suite == null || version == null || keyName == null) {
      return null;
    }
    ZoneEncryptionInfoProto.Builder builder =
        ZoneEncryptionInfoProto.newBuilder().setSuite(convert(suite))
            .setCryptoProtocolVersion(convert(version)).setKeyName(keyName);
    if (proto != null) {
      builder.setReencryptionProto(proto);
    }
    return builder.build();
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

  public static ReencryptionInfoProto convert(String ezkvn, Long submissionTime,
      boolean isCanceled, long numReencrypted, long numFailures,
      Long completionTime, String lastFile) {
    if (ezkvn == null || submissionTime == null) {
      return null;
    }
    ReencryptionInfoProto.Builder builder =
        ReencryptionInfoProto.newBuilder().setEzKeyVersionName(ezkvn)
            .setSubmissionTime(submissionTime).setCanceled(isCanceled)
            .setNumReencrypted(numReencrypted).setNumFailures(numFailures);
    if (completionTime != null) {
      builder.setCompletionTime(completionTime);
    }
    if (lastFile != null) {
      builder.setLastFile(lastFile);
    }
    return builder.build();
  }

  public static ZoneReencryptionStatusProto convert(ZoneReencryptionStatus zs) {
    ZoneReencryptionStatusProto.Builder builder =
        ZoneReencryptionStatusProto.newBuilder()
        .setId(zs.getId())
        .setPath(zs.getZoneName())
        .setEzKeyVersionName(zs.getEzKeyVersionName())
        .setSubmissionTime(zs.getSubmissionTime())
        .setCanceled(zs.isCanceled())
        .setNumReencrypted(zs.getFilesReencrypted())
        .setNumFailures(zs.getNumReencryptionFailures());
    switch (zs.getState()) {
    case Submitted:
      builder.setState(ReencryptionStateProto.SUBMITTED);
      break;
    case Processing:
      builder.setState(ReencryptionStateProto.PROCESSING);
      break;
    case Completed:
      builder.setState(ReencryptionStateProto.COMPLETED);
      break;
    default:
      throw new IllegalArgumentException("Unknown state " + zs.getState());
    }
    final long completion = zs.getCompletionTime();
    if (completion != 0) {
      builder.setCompletionTime(completion);
    }
    final String file = zs.getLastCheckpointFile();
    if (file != null) {
      builder.setLastFile(file);
    }
    return builder.build();
  }

  public static ZoneReencryptionStatus convert(
      ZoneReencryptionStatusProto proto) {
    ZoneReencryptionStatus.State state;
    switch (proto.getState()) {
    case SUBMITTED:
      state = ZoneReencryptionStatus.State.Submitted;
      break;
    case PROCESSING:
      state = ZoneReencryptionStatus.State.Processing;
      break;
    case COMPLETED:
      state = ZoneReencryptionStatus.State.Completed;
      break;
    default:
      throw new IllegalArgumentException("Unknown state " + proto.getState());
    }
    ZoneReencryptionStatus.Builder builder = new ZoneReencryptionStatus.
        Builder().
        id(proto.getId()).zoneName(proto.getPath()).state(state)
        .ezKeyVersionName(proto.getEzKeyVersionName())
        .submissionTime(proto.getSubmissionTime()).canceled(proto.getCanceled())
        .filesReencrypted(proto.getNumReencrypted())
        .fileReencryptionFailures(proto.getNumFailures());
    if (proto.hasCompletionTime()) {
      builder.completionTime(proto.getCompletionTime());
    }
    if (proto.hasLastFile()) {
      builder.lastCheckpointFile(proto.getLastFile());
    }
    return builder.build();
  }

  public static DatanodeInfo[] convert(DatanodeInfosProto datanodeInfosProto) {
    List<DatanodeInfoProto> proto = datanodeInfosProto.getDatanodesList();
    DatanodeInfo[] infos = new DatanodeInfo[proto.size()];
    for (int i = 0; i < infos.length; i++) {
      infos[i] = convert(proto.get(i));
    }
    return infos;
  }

  static List<DatanodeInfosProto> convert(DatanodeInfo[][] targets) {
    DatanodeInfosProto[] ret = new DatanodeInfosProto[targets.length];
    for (int i = 0; i < targets.length; i++) {
      ret[i] = DatanodeInfosProto.newBuilder()
          .addAllDatanodes(convert(targets[i])).build();
    }
    return Arrays.asList(ret);
  }

  public static ECSchema convertECSchema(HdfsProtos.ECSchemaProto schema) {
    List<HdfsProtos.ECSchemaOptionEntryProto> optionsList =
        schema.getOptionsList();
    Map<String, String> options = new HashMap<>(optionsList.size());
    for (HdfsProtos.ECSchemaOptionEntryProto option : optionsList) {
      options.put(option.getKey(), option.getValue());
    }
    return new ECSchema(schema.getCodecName(), schema.getDataUnits(),
        schema.getParityUnits(), options);
  }

  public static HdfsProtos.ECSchemaProto convertECSchema(ECSchema schema) {
    HdfsProtos.ECSchemaProto.Builder builder =
        HdfsProtos.ECSchemaProto.newBuilder()
        .setCodecName(schema.getCodecName())
        .setDataUnits(schema.getNumDataUnits())
        .setParityUnits(schema.getNumParityUnits());
    Set<Map.Entry<String, String>> entrySet =
        schema.getExtraOptions().entrySet();
    for (Map.Entry<String, String> entry : entrySet) {
      builder.addOptions(HdfsProtos.ECSchemaOptionEntryProto.newBuilder()
          .setKey(entry.getKey()).setValue(entry.getValue()).build());
    }
    return builder.build();
  }

  public static ErasureCodingPolicyState convertECState(
      HdfsProtos.ErasureCodingPolicyState state) {
    return ErasureCodingPolicyState.fromValue(state.getNumber());
  }

  public static HdfsProtos.ErasureCodingPolicyState convertECState(
      ErasureCodingPolicyState state) {
    return HdfsProtos.ErasureCodingPolicyState.forNumber(state.getValue());
  }

  /**
   * Convert the protobuf to a {@link ErasureCodingPolicy}.
   */
  public static ErasureCodingPolicy convertErasureCodingPolicy(
      ErasureCodingPolicyProto proto) {
    final byte id = (byte) (proto.getId() & 0xFF);
    ErasureCodingPolicy policy = SystemErasureCodingPolicies.getByID(id);
    if (policy == null) {
      // If it's not a built-in policy, populate from the optional PB fields.
      // The optional fields are required in this case.
      Preconditions.checkArgument(proto.hasName(),
          "Missing name field in ErasureCodingPolicy proto");
      Preconditions.checkArgument(proto.hasSchema(),
          "Missing schema field in ErasureCodingPolicy proto");
      Preconditions.checkArgument(proto.hasCellSize(),
          "Missing cellsize field in ErasureCodingPolicy proto");

      return new ErasureCodingPolicy(proto.getName(),
          convertECSchema(proto.getSchema()),
          proto.getCellSize(), id);
    }
    return policy;
  }

  /**
   * Convert the protobuf to a {@link ErasureCodingPolicyInfo}. This should only
   * be needed when the caller is interested in the state of the policy.
   */
  public static ErasureCodingPolicyInfo convertErasureCodingPolicyInfo(
      ErasureCodingPolicyProto proto) {
    ErasureCodingPolicy policy = convertErasureCodingPolicy(proto);
    ErasureCodingPolicyInfo info = new ErasureCodingPolicyInfo(policy);
    Preconditions.checkArgument(proto.hasState(),
        "Missing state field in ErasureCodingPolicy proto");
    info.setState(convertECState(proto.getState()));
    return info;
  }

  private static ErasureCodingPolicyProto.Builder createECPolicyProtoBuilder(
      ErasureCodingPolicy policy) {
    final ErasureCodingPolicyProto.Builder builder =
        ErasureCodingPolicyProto.newBuilder().setId(policy.getId());
    // If it's not a built-in policy, need to set the optional fields.
    if (SystemErasureCodingPolicies.getByID(policy.getId()) == null) {
      builder.setName(policy.getName())
          .setSchema(convertECSchema(policy.getSchema()))
          .setCellSize(policy.getCellSize());
    }
    return builder;
  }

  /**
   * Convert a {@link ErasureCodingPolicy} to protobuf.
   * This means no state of the policy will be set on the protobuf.
   */
  public static ErasureCodingPolicyProto convertErasureCodingPolicy(
      ErasureCodingPolicy policy) {
    return createECPolicyProtoBuilder(policy).build();
  }

  /**
   * Convert a {@link ErasureCodingPolicyInfo} to protobuf.
   * The protobuf will have the policy, and state. State is relevant when:
   * 1. Persisting a policy to fsimage
   * 2. Returning the policy to the RPC call
   * {@link DistributedFileSystem#getAllErasureCodingPolicies()}
   */
  public static ErasureCodingPolicyProto convertErasureCodingPolicy(
      ErasureCodingPolicyInfo info) {
    final ErasureCodingPolicyProto.Builder builder =
        createECPolicyProtoBuilder(info.getPolicy());
    builder.setState(convertECState(info.getState()));
    return builder.build();
  }

  public static CodecProto convertErasureCodingCodec(String codec,
      String coders) {
    CodecProto.Builder builder = CodecProto.newBuilder()
        .setCodec(codec).setCoders(coders);
    return builder.build();
  }

  public static AddErasureCodingPolicyResponseProto
      convertAddErasureCodingPolicyResponse(
          AddErasureCodingPolicyResponse response) {
    AddErasureCodingPolicyResponseProto.Builder builder =
        AddErasureCodingPolicyResponseProto.newBuilder()
        .setPolicy(convertErasureCodingPolicy(response.getPolicy()))
        .setSucceed(response.isSucceed());
    if (!response.isSucceed()) {
      builder.setErrorMsg(response.getErrorMsg());
    }
    return builder.build();
  }

  public static AddErasureCodingPolicyResponse
      convertAddErasureCodingPolicyResponse(
          AddErasureCodingPolicyResponseProto proto) {
    ErasureCodingPolicy policy = convertErasureCodingPolicy(proto.getPolicy());
    if (proto.getSucceed()) {
      return new AddErasureCodingPolicyResponse(policy);
    } else {
      return new AddErasureCodingPolicyResponse(policy, proto.getErrorMsg());
    }
  }

  public static HdfsProtos.DatanodeInfosProto convertToProto(
      DatanodeInfo[] datanodeInfos) {
    HdfsProtos.DatanodeInfosProto.Builder builder =
        HdfsProtos.DatanodeInfosProto.newBuilder();
    for (DatanodeInfo datanodeInfo : datanodeInfos) {
      builder.addDatanodes(PBHelperClient.convert(datanodeInfo));
    }
    return builder.build();
  }

  public static ECTopologyVerifierResult convertECTopologyVerifierResultProto(
      HdfsProtos.ECTopologyVerifierResultProto resp) {
    return new ECTopologyVerifierResult(resp.getIsSupported(),
        resp.getResultMessage());
  }

  public static HdfsProtos.ECTopologyVerifierResultProto convertECTopologyVerifierResult(
      ECTopologyVerifierResult resp) {
    final HdfsProtos.ECTopologyVerifierResultProto.Builder builder =
        HdfsProtos.ECTopologyVerifierResultProto.newBuilder()
            .setIsSupported(resp.isSupported())
            .setResultMessage(resp.getResultMessage());
    return builder.build();
  }

  public static EnumSet<AddBlockFlag> convertAddBlockFlags(
      List<AddBlockFlagProto> addBlockFlags) {
    EnumSet<AddBlockFlag> flags =
        EnumSet.noneOf(AddBlockFlag.class);
    for (AddBlockFlagProto af : addBlockFlags) {
      AddBlockFlag flag = AddBlockFlag.valueOf((short)af.getNumber());
      if (flag != null) {
        flags.add(flag);
      }
    }
    return flags;
  }

  public static List<AddBlockFlagProto> convertAddBlockFlags(
      EnumSet<AddBlockFlag> flags) {
    List<AddBlockFlagProto> ret = new ArrayList<>();
    for (AddBlockFlag flag : flags) {
      AddBlockFlagProto abfp = AddBlockFlagProto.forNumber(flag.getMode());
      if (abfp != null) {
        ret.add(abfp);
      }
    }
    return ret;
  }

  public static ProvidedStorageLocation convert(
      HdfsProtos.ProvidedStorageLocationProto providedStorageLocationProto) {
    if (providedStorageLocationProto == null) {
      return null;
    }
    String path = providedStorageLocationProto.getPath();
    long length = providedStorageLocationProto.getLength();
    long offset = providedStorageLocationProto.getOffset();
    ByteString nonce = providedStorageLocationProto.getNonce();

    if (path == null || length == -1 || offset == -1 || nonce == null) {
      return null;
    } else {
      return new ProvidedStorageLocation(new Path(path), offset, length,
          nonce.toByteArray());
    }
  }

  public static HdfsProtos.ProvidedStorageLocationProto convert(
      ProvidedStorageLocation providedStorageLocation) {
    String path = providedStorageLocation.getPath().toString();
    return HdfsProtos.ProvidedStorageLocationProto.newBuilder()
        .setPath(path)
        .setLength(providedStorageLocation.getLength())
        .setOffset(providedStorageLocation.getOffset())
        .setNonce(ByteString.copyFrom(providedStorageLocation.getNonce()))
        .build();
  }

  public static EnumSet<OpenFilesType> convertOpenFileTypes(
      List<OpenFilesTypeProto> openFilesTypeProtos) {
    EnumSet<OpenFilesType> types = EnumSet.noneOf(OpenFilesType.class);
    for (OpenFilesTypeProto af : openFilesTypeProtos) {
      OpenFilesType type = OpenFilesType.valueOf((short)af.getNumber());
      if (type != null) {
        types.add(type);
      }
    }
    return types;
  }

  public static List<OpenFilesTypeProto> convertOpenFileTypes(
      EnumSet<OpenFilesType> types) {
    List<OpenFilesTypeProto> typeProtos = new ArrayList<>();
    for (OpenFilesType type : types) {
      OpenFilesTypeProto typeProto = OpenFilesTypeProto
          .forNumber(type.getMode());
      if (typeProto != null) {
        typeProtos.add(typeProto);
      }
    }
    return typeProtos;
  }
}
