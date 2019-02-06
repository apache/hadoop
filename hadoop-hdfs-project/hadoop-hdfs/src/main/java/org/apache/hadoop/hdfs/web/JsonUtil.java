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
package org.apache.hadoop.hdfs.web;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.*;

/** JSON Utilities */
public class JsonUtil {
  private static final Object[] EMPTY_OBJECT_ARRAY = {};

  // Reuse ObjectMapper instance for improving performance.
  // ObjectMapper is thread safe as long as we always configure instance
  // before use. We don't have a re-entrant call pattern in WebHDFS,
  // so we just need to worry about thread-safety.
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Convert a token object to a Json string. */
  public static String toJsonString(final Token<? extends TokenIdentifier> token
      ) throws IOException {
    return toJsonString(Token.class, toJsonMap(token));
  }

  private static Map<String, Object> toJsonMap(
      final Token<? extends TokenIdentifier> token) throws IOException {
    if (token == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("urlString", token.encodeToUrlString());
    return m;
  }

  /** Convert an exception object to a Json string. */
  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("exception", e.getClass().getSimpleName());
    m.put("message", e.getMessage());
    m.put("javaClassName", e.getClass().getName());
    return toJsonString(RemoteException.class, m);
  }

  private static String toJsonString(final Class<?> clazz, final Object value) {
    return toJsonString(clazz.getSimpleName(), value);
  }

  /** Convert a key-value pair to a Json string. */
  public static String toJsonString(final String key, final Object value) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put(key, value);
    try {
      return MAPPER.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /** Convert a FsPermission object to a string. */
  private static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /** Convert a HdfsFileStatus object to a Json string. */
  public static String toJsonString(final HdfsFileStatus status,
      boolean includeType) {
    if (status == null) {
      return null;
    }
    final Map<String, Object> m = toJsonMap(status);
    try {
      return includeType ?
          toJsonString(FileStatus.class, m) : MAPPER.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  private static Map<String, Object> toJsonMap(HdfsFileStatus status) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("pathSuffix", status.getLocalName());
    m.put("type", WebHdfsConstants.PathType.valueOf(status));
    if (status.isSymlink()) {
      m.put("symlink", DFSUtilClient.bytes2String(status.getSymlinkInBytes()));
    }
    m.put("length", status.getLen());
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    FsPermission perm = status.getPermission();
    m.put("permission", toString(perm));
    if (status.hasAcl()) {
      m.put("aclBit", true);
    }
    if (status.isEncrypted()) {
      m.put("encBit", true);
    }
    if (status.isErasureCoded()) {
      m.put("ecBit", true);
      if (status.getErasureCodingPolicy() != null) {
        // to maintain backward comparability
        m.put("ecPolicy", status.getErasureCodingPolicy().getName());
        // to re-construct HdfsFileStatus object via WebHdfs
        m.put("ecPolicyObj", getEcPolicyAsMap(status.getErasureCodingPolicy()));
      }
    }
    if (status.isSnapshotEnabled()) {
      m.put("snapshotEnabled", status.isSnapshotEnabled());
    }

    m.put("accessTime", status.getAccessTime());
    m.put("modificationTime", status.getModificationTime());
    m.put("blockSize", status.getBlockSize());
    m.put("replication", status.getReplication());
    m.put("fileId", status.getFileId());
    m.put("childrenNum", status.getChildrenNum());
    m.put("storagePolicy", status.getStoragePolicy());
    return m;
  }

  private static Map<String, Object> getEcPolicyAsMap(
      final ErasureCodingPolicy ecPolicy) {
    /** Convert an ErasureCodingPolicy to a map. */
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("name", ecPolicy.getName())
        .put("cellSize", ecPolicy.getCellSize())
        .put("numDataUnits", ecPolicy.getNumDataUnits())
        .put("numParityUnits", ecPolicy.getNumParityUnits())
        .put("codecName", ecPolicy.getCodecName())
        .put("id", ecPolicy.getId())
        .put("extraOptions", ecPolicy.getSchema().getExtraOptions());
    return builder.build();

  }

  /** Convert an ExtendedBlock to a Json map. */
  private static Map<String, Object> toJsonMap(final ExtendedBlock extendedblock) {
    if (extendedblock == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("blockPoolId", extendedblock.getBlockPoolId());
    m.put("blockId", extendedblock.getBlockId());
    m.put("numBytes", extendedblock.getNumBytes());
    m.put("generationStamp", extendedblock.getGenerationStamp());
    return m;
  }

  /** Convert a DatanodeInfo to a Json map. */
  static Map<String, Object> toJsonMap(final DatanodeInfo datanodeinfo) {
    if (datanodeinfo == null) {
      return null;
    }

    // TODO: Fix storageID
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("ipAddr", datanodeinfo.getIpAddr());
    // 'name' is equivalent to ipAddr:xferPort. Older clients (1.x, 0.23.x) 
    // expects this instead of the two fields.
    m.put("name", datanodeinfo.getXferAddr());
    m.put("hostName", datanodeinfo.getHostName());
    m.put("storageID", datanodeinfo.getDatanodeUuid());
    m.put("xferPort", datanodeinfo.getXferPort());
    m.put("infoPort", datanodeinfo.getInfoPort());
    m.put("infoSecurePort", datanodeinfo.getInfoSecurePort());
    m.put("ipcPort", datanodeinfo.getIpcPort());

    m.put("capacity", datanodeinfo.getCapacity());
    m.put("dfsUsed", datanodeinfo.getDfsUsed());
    m.put("remaining", datanodeinfo.getRemaining());
    m.put("blockPoolUsed", datanodeinfo.getBlockPoolUsed());
    m.put("cacheCapacity", datanodeinfo.getCacheCapacity());
    m.put("cacheUsed", datanodeinfo.getCacheUsed());
    m.put("lastUpdate", datanodeinfo.getLastUpdate());
    m.put("lastUpdateMonotonic", datanodeinfo.getLastUpdateMonotonic());
    m.put("xceiverCount", datanodeinfo.getXceiverCount());
    m.put("networkLocation", datanodeinfo.getNetworkLocation());
    m.put("adminState", datanodeinfo.getAdminState().name());
    if (datanodeinfo.getUpgradeDomain() != null) {
      m.put("upgradeDomain", datanodeinfo.getUpgradeDomain());
    }
    m.put("lastBlockReportTime", datanodeinfo.getLastBlockReportTime());
    m.put("lastBlockReportMonotonic",
        datanodeinfo.getLastBlockReportMonotonic());
    return m;
  }

  /** Convert a DatanodeInfo[] to a Json array. */
  private static Object[] toJsonArray(final DatanodeInfo[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.length];
      for(int i = 0; i < array.length; i++) {
        a[i] = toJsonMap(array[i]);
      }
      return a;
    }
  }

  /** Convert a StorageType[] to a Json array. */
  private static Object[] toJsonArray(final StorageType[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.length];
      for(int i = 0; i < array.length; i++) {
        a[i] = array[i];
      }
      return a;
    }
  }

  /** Convert a LocatedBlock to a Json map. */
  private static Map<String, Object> toJsonMap(final LocatedBlock locatedblock
      ) throws IOException {
    if (locatedblock == null) {
      return null;
    }
 
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("blockToken", toJsonMap(locatedblock.getBlockToken()));
    m.put("isCorrupt", locatedblock.isCorrupt());
    m.put("startOffset", locatedblock.getStartOffset());
    m.put("block", toJsonMap(locatedblock.getBlock()));
    m.put("storageTypes", toJsonArray(locatedblock.getStorageTypes()));
    m.put("locations", toJsonArray(locatedblock.getLocations()));
    m.put("cachedLocations", toJsonArray(locatedblock.getCachedLocations()));
    return m;
  }

  private static Map<String, Object> toJson(final DirectoryListing listing)
      throws IOException {
    final Map<String, Object> m = new TreeMap<>();
    // Serialize FileStatus[] to a FileStatuses map
    m.put("partialListing", toJsonMap(listing.getPartialListing()));
    // Simple int
    m.put("remainingEntries", listing.getRemainingEntries());

    return m;
  }

  public static String toJsonString(final DirectoryListing listing) throws
      IOException {

    if (listing == null) {
      return null;
    }
    return toJsonString(DirectoryListing.class, toJson(listing));
  }

  private static Map<String, Object> toJsonMap(HdfsFileStatus[] statuses) throws
      IOException {
    if (statuses == null) {
      return null;
    }

    final Map<String, Object> fileStatuses = new TreeMap<>();
    final Map<String, Object> fileStatus = new TreeMap<>();
    fileStatuses.put("FileStatuses", fileStatus);
    final Object[] array = new Object[statuses.length];
    fileStatus.put("FileStatus", array);
    for (int i = 0; i < statuses.length; i++) {
      array[i] = toJsonMap(statuses[i]);
    }

    return fileStatuses;
  }

  /** Convert a LocatedBlock[] to a Json array. */
  private static Object[] toJsonArray(final List<LocatedBlock> array
      ) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.size()];
      for(int i = 0; i < array.size(); i++) {
        a[i] = toJsonMap(array.get(i));
      }
      return a;
    }
  }

  /** Convert LocatedBlocks to a Json string. */
  public static String toJsonString(final LocatedBlocks locatedblocks
      ) throws IOException {
    if (locatedblocks == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("fileLength", locatedblocks.getFileLength());
    m.put("isUnderConstruction", locatedblocks.isUnderConstruction());

    m.put("locatedBlocks", toJsonArray(locatedblocks.getLocatedBlocks()));
    m.put("lastLocatedBlock", toJsonMap(locatedblocks.getLastLocatedBlock()));
    m.put("isLastBlockComplete", locatedblocks.isLastBlockComplete());
    return toJsonString(LocatedBlocks.class, m);
  }

  /** Convert a ContentSummary to a Json string. */
  public static String toJsonString(final ContentSummary contentsummary) {
    if (contentsummary == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("length", contentsummary.getLength());
    m.put("fileCount", contentsummary.getFileCount());
    m.put("directoryCount", contentsummary.getDirectoryCount());
    m.put("quota", contentsummary.getQuota());
    m.put("spaceConsumed", contentsummary.getSpaceConsumed());
    m.put("spaceQuota", contentsummary.getSpaceQuota());
    final Map<String, Map<String, Long>> typeQuota =
        new TreeMap<String, Map<String, Long>>();
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      long tQuota = contentsummary.getTypeQuota(t);
      if (tQuota != HdfsConstants.QUOTA_RESET) {
        Map<String, Long> type = typeQuota.get(t.toString());
        if (type == null) {
          type = new TreeMap<String, Long>();
          typeQuota.put(t.toString(), type);
        }
        type.put("quota", contentsummary.getTypeQuota(t));
        type.put("consumed", contentsummary.getTypeConsumed(t));
      }
    }
    m.put("typeQuota", typeQuota);
    return toJsonString(ContentSummary.class, m);
  }

  /** Convert a MD5MD5CRC32FileChecksum to a Json string. */
  public static String toJsonString(final MD5MD5CRC32FileChecksum checksum) {
    if (checksum == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("algorithm", checksum.getAlgorithmName());
    m.put("length", checksum.getLength());
    m.put("bytes", StringUtils.byteToHexString(checksum.getBytes()));
    return toJsonString(FileChecksum.class, m);
  }

  /** Convert a AclStatus object to a Json string. */
  public static String toJsonString(final AclStatus status) {
    if (status == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    m.put("stickyBit", status.isStickyBit());

    final List<String> stringEntries = new ArrayList<>();
    for (AclEntry entry : status.getEntries()) {
      stringEntries.add(entry.toStringStable());
    }
    m.put("entries", stringEntries);

    FsPermission perm = status.getPermission();
    if (perm != null) {
      m.put("permission", toString(perm));
    }
    final Map<String, Map<String, Object>> finalMap =
        new TreeMap<String, Map<String, Object>>();
    finalMap.put(AclStatus.class.getSimpleName(), m);

    try {
      return MAPPER.writeValueAsString(finalMap);
    } catch (IOException ignored) {
    }
    return null;
  }

  private static Map<String, Object> toJsonMap(final XAttr xAttr,
      final XAttrCodec encoding) throws IOException {
    if (xAttr == null) {
      return null;
    }
 
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("name", XAttrHelper.getPrefixedName(xAttr));
    m.put("value", xAttr.getValue() != null ?
        XAttrCodec.encodeValue(xAttr.getValue(), encoding) : null);
    return m;
  }
  
  private static Object[] toJsonArray(final List<XAttr> array,
      final XAttrCodec encoding) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.size()];
      for(int i = 0; i < array.size(); i++) {
        a[i] = toJsonMap(array.get(i), encoding);
      }
      return a;
    }
  }
  
  public static String toJsonString(final List<XAttr> xAttrs, 
      final XAttrCodec encoding) throws IOException {
    final Map<String, Object> finalMap = new TreeMap<String, Object>();
    finalMap.put("XAttrs", toJsonArray(xAttrs, encoding));
    return MAPPER.writeValueAsString(finalMap);
  }
  
  public static String toJsonString(final List<XAttr> xAttrs)
    throws IOException {
    final List<String> names = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      names.add(XAttrHelper.getPrefixedName(xAttr));
    }
    String ret = MAPPER.writeValueAsString(names);
    final Map<String, Object> finalMap = new TreeMap<String, Object>();
    finalMap.put("XAttrNames", ret);
    return MAPPER.writeValueAsString(finalMap);
  }

  public static String toJsonString(Object obj) throws IOException {
    return MAPPER.writeValueAsString(obj);
  }

  public static String toJsonString(BlockStoragePolicy[] storagePolicies) {
    final Map<String, Object> blockStoragePolicies = new TreeMap<>();
    Object[] a = null;
    if (storagePolicies != null && storagePolicies.length > 0) {
      a = new Object[storagePolicies.length];
      for (int i = 0; i < storagePolicies.length; i++) {
        a[i] = toJsonMap(storagePolicies[i]);
      }
    }
    blockStoragePolicies.put("BlockStoragePolicy", a);
    return toJsonString("BlockStoragePolicies", blockStoragePolicies);
  }

  private static Object toJsonMap(BlockStoragePolicy blockStoragePolicy) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("id", blockStoragePolicy.getId());
    m.put("name", blockStoragePolicy.getName());
    m.put("storageTypes", blockStoragePolicy.getStorageTypes());
    m.put("creationFallbacks", blockStoragePolicy.getCreationFallbacks());
    m.put("replicationFallbacks", blockStoragePolicy.getReplicationFallbacks());
    m.put("copyOnCreateFile", blockStoragePolicy.isCopyOnCreateFile());
    return m;
  }

  public static String toJsonString(BlockStoragePolicy storagePolicy) {
    return toJsonString(BlockStoragePolicy.class, toJsonMap(storagePolicy));
  }

  public static String toJsonString(FsServerDefaults serverDefaults) {
    return toJsonString(FsServerDefaults.class, toJsonMap(serverDefaults));
  }

  private static Object toJsonMap(FsServerDefaults serverDefaults) {
    final Map<String, Object> m = new HashMap<String, Object>();
    m.put("blockSize", serverDefaults.getBlockSize());
    m.put("bytesPerChecksum", serverDefaults.getBytesPerChecksum());
    m.put("writePacketSize", serverDefaults.getWritePacketSize());
    m.put("replication", serverDefaults.getReplication());
    m.put("fileBufferSize", serverDefaults.getFileBufferSize());
    m.put("encryptDataTransfer", serverDefaults.getEncryptDataTransfer());
    m.put("trashInterval", serverDefaults.getTrashInterval());
    m.put("checksumType", serverDefaults.getChecksumType().id);
    m.put("keyProviderUri", serverDefaults.getKeyProviderUri());
    m.put("defaultStoragePolicyId", serverDefaults.getDefaultStoragePolicyId());
    return m;
  }

  public static String toJsonString(SnapshotDiffReport diffReport) {
    return toJsonString(SnapshotDiffReport.class.getSimpleName(),
        toJsonMap(diffReport));
  }

  private static Object toJsonMap(SnapshotDiffReport diffReport) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("snapshotRoot", diffReport.getSnapshotRoot());
    m.put("fromSnapshot", diffReport.getFromSnapshot());
    m.put("toSnapshot", diffReport.getLaterSnapshotName());
    Object[] diffList = new Object[diffReport.getDiffList().size()];
    for (int i = 0; i < diffReport.getDiffList().size(); i++) {
      diffList[i] = toJsonMap(diffReport.getDiffList().get(i));
    }
    m.put("diffList", diffList);
    return m;
  }

  private static Object toJsonMap(
      SnapshotDiffReport.DiffReportEntry diffReportEntry) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("type", diffReportEntry.getType());
    if (diffReportEntry.getSourcePath() != null) {
      m.put("sourcePath",
          DFSUtilClient.bytes2String(diffReportEntry.getSourcePath()));
    }
    if (diffReportEntry.getTargetPath() != null) {
      m.put("targetPath",
          DFSUtilClient.bytes2String(diffReportEntry.getTargetPath()));
    }
    return m;
  }

  public static String toJsonString(
      SnapshottableDirectoryStatus[] snapshottableDirectoryList) {
    if (snapshottableDirectoryList == null) {
      return toJsonString("SnapshottableDirectoryList", null);
    }
    Object[] a = new Object[snapshottableDirectoryList.length];
    for (int i = 0; i < snapshottableDirectoryList.length; i++) {
      a[i] = toJsonMap(snapshottableDirectoryList[i]);
    }
    return toJsonString("SnapshottableDirectoryList", a);
  }

  private static Object toJsonMap(
      SnapshottableDirectoryStatus snapshottableDirectoryStatus) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("snapshotNumber", snapshottableDirectoryStatus.getSnapshotNumber());
    m.put("snapshotQuota", snapshottableDirectoryStatus.getSnapshotQuota());
    m.put("parentFullPath", DFSUtilClient
        .bytes2String(snapshottableDirectoryStatus.getParentFullPath()));
    m.put("dirStatus", toJsonMap(snapshottableDirectoryStatus.getDirStatus()));
    return m;
  }

  private static Map<String, Object> toJsonMap(
      final BlockLocation blockLocation) throws IOException {
    if (blockLocation == null) {
      return null;
    }

    final Map<String, Object> m = new HashMap<>();
    m.put("length", blockLocation.getLength());
    m.put("offset", blockLocation.getOffset());
    m.put("corrupt", blockLocation.isCorrupt());
    m.put("storageTypes", toJsonArray(blockLocation.getStorageTypes()));
    m.put("cachedHosts", blockLocation.getCachedHosts());
    m.put("hosts", blockLocation.getHosts());
    m.put("names", blockLocation.getNames());
    m.put("topologyPaths", blockLocation.getTopologyPaths());
    return m;
  }

  public static String toJsonString(BlockLocation[] locations)
      throws IOException {
    if (locations == null) {
      return null;
    }
    final Map<String, Object> m = new HashMap<>();
    Object[] blockLocations = new Object[locations.length];
    for(int i=0; i<locations.length; i++) {
      blockLocations[i] = toJsonMap(locations[i]);
    }
    m.put(BlockLocation.class.getSimpleName(), blockLocations);
    return toJsonString("BlockLocations", m);
  }
}
