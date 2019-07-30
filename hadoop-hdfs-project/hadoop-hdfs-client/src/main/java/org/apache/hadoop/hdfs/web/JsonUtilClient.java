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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

class JsonUtilClient {
  static final DatanodeInfo[] EMPTY_DATANODE_INFO_ARRAY = {};
  static final String UNSUPPPORTED_EXCEPTION_STR =
      UnsupportedOperationException.class.getName();

  /** Convert a Json map to a RemoteException. */
  static RemoteException toRemoteException(final Map<?, ?> json) {
    final Map<?, ?> m = (Map<?, ?>)json.get(
        RemoteException.class.getSimpleName());
    final String message = (String)m.get("message");
    final String javaClassName = (String)m.get("javaClassName");
    if (UNSUPPPORTED_EXCEPTION_STR.equals(javaClassName)) {
      throw new UnsupportedOperationException(message);
    }
    return new RemoteException(javaClassName, message);
  }

  /** Convert a Json map to a Token. */
  static Token<? extends TokenIdentifier> toToken(
      final Map<?, ?> m) throws IOException {
    if (m == null) {
      return null;
    }

    final Token<DelegationTokenIdentifier> token
        = new Token<>();
    token.decodeFromUrlString((String)m.get("urlString"));
    return token;
  }

  /** Convert a Json map to a Token of BlockTokenIdentifier. */
  @SuppressWarnings("unchecked")
  static Token<BlockTokenIdentifier> toBlockToken(
      final Map<?, ?> m) throws IOException {
    return (Token<BlockTokenIdentifier>)toToken(m);
  }

  /** Convert a string to a FsPermission object. */
  static FsPermission toFsPermission(final String s) {
    return null == s ? null : new FsPermission(Short.parseShort(s, 8));
  }

  /** Convert a Json map to a HdfsFileStatus object. */
  static HdfsFileStatus toFileStatus(final Map<?, ?> json,
      boolean includesType) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = includesType ?
        (Map<?, ?>)json.get(FileStatus.class.getSimpleName()) : json;
    final String localName = (String) m.get("pathSuffix");
    final WebHdfsConstants.PathType type =
        WebHdfsConstants.PathType.valueOf((String) m.get("type"));
    final byte[] symlink = type != WebHdfsConstants.PathType.SYMLINK? null
        : DFSUtilClient.string2Bytes((String) m.get("symlink"));

    final long len = ((Number) m.get("length")).longValue();
    final String owner = (String) m.get("owner");
    final String group = (String) m.get("group");
    final FsPermission permission = toFsPermission((String)m.get("permission"));

    Boolean aclBit = (Boolean) m.get("aclBit");
    Boolean encBit = (Boolean) m.get("encBit");
    Boolean erasureBit  = (Boolean) m.get("ecBit");
    Boolean snapshotEnabledBit  = (Boolean) m.get("snapshotEnabled");
    EnumSet<HdfsFileStatus.Flags> f =
        EnumSet.noneOf(HdfsFileStatus.Flags.class);
    if (aclBit != null && aclBit) {
      f.add(HdfsFileStatus.Flags.HAS_ACL);
    }
    if (encBit != null && encBit) {
      f.add(HdfsFileStatus.Flags.HAS_CRYPT);
    }
    if (erasureBit != null && erasureBit) {
      f.add(HdfsFileStatus.Flags.HAS_EC);
    }
    if (snapshotEnabledBit != null && snapshotEnabledBit) {
      f.add(HdfsFileStatus.Flags.SNAPSHOT_ENABLED);
    }

    Map<String, Object> ecPolicyObj = (Map) m.get("ecPolicyObj");
    ErasureCodingPolicy ecPolicy = null;
    if (ecPolicyObj != null) {
      Map<String, String> extraOptions = (Map) ecPolicyObj.get("extraOptions");
      ECSchema ecSchema = new ECSchema((String) ecPolicyObj.get("codecName"),
          (int) ecPolicyObj.get("numDataUnits"),
          (int) ecPolicyObj.get("numParityUnits"), extraOptions);
      ecPolicy = new ErasureCodingPolicy((String) ecPolicyObj.get("name"),
          ecSchema, (int) ecPolicyObj.get("cellSize"),
          (byte) (int) ecPolicyObj.get("id"));

    }

    final long aTime = ((Number) m.get("accessTime")).longValue();
    final long mTime = ((Number) m.get("modificationTime")).longValue();
    final long blockSize = ((Number) m.get("blockSize")).longValue();
    final short replication = ((Number) m.get("replication")).shortValue();
    final long fileId = m.containsKey("fileId") ?
        ((Number) m.get("fileId")).longValue() :
        HdfsConstants.GRANDFATHER_INODE_ID;
    final int childrenNum = getInt(m, "childrenNum", -1);
    final byte storagePolicy = m.containsKey("storagePolicy") ?
        (byte) ((Number) m.get("storagePolicy")).longValue() :
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
    return new HdfsFileStatus.Builder()
      .length(len)
      .isdir(type == WebHdfsConstants.PathType.DIRECTORY)
      .replication(replication)
      .blocksize(blockSize)
      .mtime(mTime)
      .atime(aTime)
      .perm(permission)
      .flags(f)
      .owner(owner)
      .group(group)
      .symlink(symlink)
      .path(DFSUtilClient.string2Bytes(localName))
      .fileId(fileId)
      .children(childrenNum)
      .storagePolicy(storagePolicy)
      .ecPolicy(ecPolicy)
      .build();
  }

  static HdfsFileStatus[] toHdfsFileStatusArray(final Map<?, ?> json) {
    Preconditions.checkNotNull(json);
    final Map<?, ?> rootmap =
        (Map<?, ?>)json.get(FileStatus.class.getSimpleName() + "es");
    final List<?> array = JsonUtilClient.getList(rootmap,
        FileStatus.class.getSimpleName());

    // convert FileStatus
    Preconditions.checkNotNull(array);
    final HdfsFileStatus[] statuses = new HdfsFileStatus[array.size()];
    int i = 0;
    for (Object object : array) {
      final Map<?, ?> m = (Map<?, ?>) object;
      statuses[i++] = JsonUtilClient.toFileStatus(m, false);
    }
    return statuses;
  }

  static DirectoryListing toDirectoryListing(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }
    final Map<?, ?> listing = getMap(json, "DirectoryListing");
    final Map<?, ?> partialListing = getMap(listing, "partialListing");
    HdfsFileStatus[] fileStatuses = toHdfsFileStatusArray(partialListing);

    int remainingEntries = getInt(listing, "remainingEntries", -1);
    Preconditions.checkState(remainingEntries != -1,
        "remainingEntries was not set");
    return new DirectoryListing(fileStatuses, remainingEntries);
  }

  /** Convert a Json map to an ExtendedBlock object. */
  static ExtendedBlock toExtendedBlock(final Map<?, ?> m) {
    if (m == null) {
      return null;
    }

    final String blockPoolId = (String)m.get("blockPoolId");
    final long blockId = ((Number) m.get("blockId")).longValue();
    final long numBytes = ((Number) m.get("numBytes")).longValue();
    final long generationStamp =
        ((Number) m.get("generationStamp")).longValue();
    return new ExtendedBlock(blockPoolId, blockId, numBytes, generationStamp);
  }

  static int getInt(Map<?, ?> m, String key, final int defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return ((Number) value).intValue();
  }

  static long getLong(Map<?, ?> m, String key, final long defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return ((Number) value).longValue();
  }

  static String getString(
      Map<?, ?> m, String key, final String defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return (String) value;
  }

  static List<?> getList(Map<?, ?> m, String key) {
    Object list = m.get(key);
    if (list instanceof List<?>) {
      return (List<?>) list;
    } else {
      return null;
    }
  }

  static Map<?, ?> getMap(Map<?, ?> m, String key) {
    Object map = m.get(key);
    if (map instanceof Map<?, ?>) {
      return (Map<?, ?>) map;
    } else {
      return null;
    }
  }

  /** Convert a Json map to an DatanodeInfo object. */
  static DatanodeInfo toDatanodeInfo(final Map<?, ?> m)
      throws IOException {
    if (m == null) {
      return null;
    }

    // ipAddr and xferPort are the critical fields for accessing data.
    // If any one of the two is missing, an exception needs to be thrown.

    // Handle the case of old servers (1.x, 0.23.x) sending 'name' instead
    //  of ipAddr and xferPort.
    String ipAddr = getString(m, "ipAddr", null);
    int xferPort = getInt(m, "xferPort", -1);
    if (ipAddr == null) {
      String name = getString(m, "name", null);
      if (name != null) {
        int colonIdx = name.indexOf(':');
        if (colonIdx > 0) {
          ipAddr = name.substring(0, colonIdx);
          xferPort = Integer.parseInt(name.substring(colonIdx +1));
        } else {
          throw new IOException(
              "Invalid value in server response: name=[" + name + "]");
        }
      } else {
        throw new IOException(
            "Missing both 'ipAddr' and 'name' in server response.");
      }
      // ipAddr is non-null & non-empty string at this point.
    }

    // Check the validity of xferPort.
    if (xferPort == -1) {
      throw new IOException(
          "Invalid or missing 'xferPort' in server response.");
    }

    // TODO: Fix storageID
    return new DatanodeInfoBuilder().setIpAddr(ipAddr)
        .setHostName((String) m.get("hostName"))
        .setDatanodeUuid((String) m.get("storageID")).setXferPort(xferPort)
        .setInfoPort(((Number) m.get("infoPort")).intValue())
        .setInfoSecurePort(getInt(m, "infoSecurePort", 0))
        .setIpcPort(((Number) m.get("ipcPort")).intValue())
        .setCapacity(getLong(m, "capacity", 0L))
        .setDfsUsed(getLong(m, "dfsUsed", 0L))
        .setRemaining(getLong(m, "remaining", 0L))
        .setBlockPoolUsed(getLong(m, "blockPoolUsed", 0L))
        .setCacheCapacity(getLong(m, "cacheCapacity", 0L))
        .setCacheUsed(getLong(m, "cacheUsed", 0L))
        .setLastUpdate(getLong(m, "lastUpdate", 0L))
        .setLastUpdateMonotonic(getLong(m, "lastUpdateMonotonic", 0L))
        .setXceiverCount(getInt(m, "xceiverCount", 0))
        .setNetworkLocation(getString(m, "networkLocation", "")).setAdminState(
            DatanodeInfo.AdminStates
                .valueOf(getString(m, "adminState", "NORMAL")))
        .setUpgradeDomain(getString(m, "upgradeDomain", ""))
        .setLastBlockReportTime(getLong(m, "lastBlockReportTime", 0L))
        .setLastBlockReportMonotonic(getLong(m, "lastBlockReportMonotonic", 0L))
        .build();
  }

  /** Convert an Object[] to a DatanodeInfo[]. */
  static DatanodeInfo[] toDatanodeInfoArray(final List<?> objects)
      throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return EMPTY_DATANODE_INFO_ARRAY;
    } else {
      final DatanodeInfo[] array = new DatanodeInfo[objects.size()];
      int i = 0;
      for (Object object : objects) {
        array[i++] = toDatanodeInfo((Map<?, ?>) object);
      }
      return array;
    }
  }

  /** Convert an Object[] to a StorageType[]. */
  static StorageType[] toStorageTypeArray(final List<?> objects)
      throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return StorageType.EMPTY_ARRAY;
    } else {
      final StorageType[] array = new StorageType[objects.size()];
      int i = 0;
      for (Object object : objects) {
        array[i++] = StorageType.parseStorageType(object.toString());
      }
      return array;
    }
  }

  /** Convert a Json map to LocatedBlock. */
  static LocatedBlock toLocatedBlock(final Map<?, ?> m) throws IOException {
    if (m == null) {
      return null;
    }

    final ExtendedBlock b = toExtendedBlock((Map<?, ?>)m.get("block"));
    final DatanodeInfo[] locations = toDatanodeInfoArray(
        getList(m, "locations"));
    final long startOffset = ((Number) m.get("startOffset")).longValue();
    final boolean isCorrupt = (Boolean)m.get("isCorrupt");
    final DatanodeInfo[] cachedLocations = toDatanodeInfoArray(
        getList(m, "cachedLocations"));

    final StorageType[] storageTypes = toStorageTypeArray(
        getList(m, "storageTypes"));
    final LocatedBlock locatedblock = new LocatedBlock(b, locations,
        null, storageTypes, startOffset, isCorrupt, cachedLocations);
    locatedblock.setBlockToken(toBlockToken((Map<?, ?>)m.get("blockToken")));
    return locatedblock;
  }

  /** Convert an List of Object to a List of LocatedBlock. */
  static List<LocatedBlock> toLocatedBlockList(
      final List<?> objects) throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return Collections.emptyList();
    } else {
      final List<LocatedBlock> list = new ArrayList<>(objects.size());
      for (Object object : objects) {
        list.add(toLocatedBlock((Map<?, ?>) object));
      }
      return list;
    }
  }

  /** Convert a Json map to a ContentSummary. */
  static ContentSummary toContentSummary(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)
        json.get(ContentSummary.class.getSimpleName());
    final long length = ((Number) m.get("length")).longValue();
    final long fileCount = ((Number) m.get("fileCount")).longValue();
    final long directoryCount = ((Number) m.get("directoryCount")).longValue();
    ContentSummary.Builder builder = new ContentSummary.Builder()
        .length(length)
        .fileCount(fileCount)
        .directoryCount(directoryCount);
    builder = buildQuotaUsage(builder, m, ContentSummary.Builder.class);
    return builder.build();
  }

  /** Convert a JSON map to a QuotaUsage. */
  static QuotaUsage toQuotaUsage(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>) json.get(QuotaUsage.class.getSimpleName());
    QuotaUsage.Builder builder = new QuotaUsage.Builder();
    builder = buildQuotaUsage(builder, m, QuotaUsage.Builder.class);
    return builder.build();
  }

  /**
   * Given a builder for QuotaUsage, parse the provided map and
   * construct the relevant fields. Return the updated builder.
   */
  private static <T extends QuotaUsage.Builder> T buildQuotaUsage(
      T builder, Map<?, ?> m, Class<T> type) {
    final long quota = ((Number) m.get("quota")).longValue();
    final long spaceConsumed = ((Number) m.get("spaceConsumed")).longValue();
    final long spaceQuota = ((Number) m.get("spaceQuota")).longValue();
    final Map<?, ?> typem = (Map<?, ?>) m.get("typeQuota");

    T result = type.cast(builder
        .quota(quota)
        .spaceConsumed(spaceConsumed)
        .spaceQuota(spaceQuota));

    // ContentSummary doesn't set this so check before using it
    if (m.get("fileAndDirectoryCount") != null) {
      final long fileAndDirectoryCount =
          ((Number) m.get("fileAndDirectoryCount")).longValue();
      result = type.cast(result.fileAndDirectoryCount(fileAndDirectoryCount));
    }

    if (typem != null) {
      for (StorageType t : StorageType.getTypesSupportingQuota()) {
        Map<?, ?> typeQuota = (Map<?, ?>) typem.get(t.toString());
        if (typeQuota != null) {
          result = type.cast(result.typeQuota(t,
              ((Number) typeQuota.get("quota")).longValue()).typeConsumed(t,
              ((Number) typeQuota.get("consumed")).longValue()));
        }
      }
    }

    return result;
  }

  /** Convert a Json map to a MD5MD5CRC32FileChecksum. */
  static MD5MD5CRC32FileChecksum toMD5MD5CRC32FileChecksum(
      final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(FileChecksum.class.getSimpleName());
    final String algorithm = (String)m.get("algorithm");
    final int length = ((Number) m.get("length")).intValue();
    final byte[] bytes = StringUtils.hexStringToByte((String) m.get("bytes"));

    final DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(bytes));
    final DataChecksum.Type crcType =
        MD5MD5CRC32FileChecksum.getCrcTypeFromAlgorithmName(algorithm);
    final MD5MD5CRC32FileChecksum checksum;

    // Recreate what DFSClient would have returned.
    switch(crcType) {
    case CRC32:
      checksum = new MD5MD5CRC32GzipFileChecksum();
      break;
    case CRC32C:
      checksum = new MD5MD5CRC32CastagnoliFileChecksum();
      break;
    default:
      throw new IOException("Unknown algorithm: " + algorithm);
    }
    checksum.readFields(in);

    //check algorithm name
    if (!checksum.getAlgorithmName().equals(algorithm)) {
      throw new IOException("Algorithm not matched. Expected " + algorithm
          + ", Received " + checksum.getAlgorithmName());
    }
    //check length
    if (length != checksum.getLength()) {
      throw new IOException("Length not matched: length=" + length
          + ", checksum.getLength()=" + checksum.getLength());
    }

    return checksum;
  }

  /** Convert a Json map to a AclStatus object. */
  static AclStatus toAclStatus(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>) json.get(AclStatus.class.getSimpleName());

    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
    aclStatusBuilder.owner((String) m.get("owner"));
    aclStatusBuilder.group((String) m.get("group"));
    aclStatusBuilder.stickyBit((Boolean) m.get("stickyBit"));
    String permString = (String) m.get("permission");
    if (permString != null) {
      final FsPermission permission = toFsPermission(permString);
      aclStatusBuilder.setPermission(permission);
    }
    final List<?> entries = (List<?>) m.get("entries");

    List<AclEntry> aclEntryList = new ArrayList<>();
    for (Object entry : entries) {
      AclEntry aclEntry = AclEntry.parseAclEntry((String) entry, true);
      aclEntryList.add(aclEntry);
    }
    aclStatusBuilder.addEntries(aclEntryList);
    return aclStatusBuilder.build();
  }

  static String getPath(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }

    return (String) json.get("Path");
  }

  static byte[] getXAttr(final Map<?, ?> json, final String name)
      throws IOException {
    if (json == null) {
      return null;
    }

    Map<String, byte[]> xAttrs = toXAttrs(json);
    if (xAttrs != null) {
      return xAttrs.get(name);
    }

    return null;
  }

  /** Expecting only single XAttr in the map. return its value */
  static byte[] getXAttr(final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    Map<String, byte[]> xAttrs = toXAttrs(json);
    if (xAttrs != null && !xAttrs.values().isEmpty()) {
      return xAttrs.values().iterator().next();
    }

    return null;
  }

  static Map<String, byte[]> toXAttrs(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }
    return toXAttrMap(getList(json, "XAttrs"));
  }

  static List<String> toXAttrNames(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }

    final String namesInJson = (String) json.get("XAttrNames");
    ObjectReader reader = new ObjectMapper().readerFor(List.class);
    final List<Object> xattrs = reader.readValue(namesInJson);
    final List<String> names =
        Lists.newArrayListWithCapacity(json.keySet().size());

    for (Object xattr : xattrs) {
      names.add((String) xattr);
    }
    return names;
  }

  static Map<String, byte[]> toXAttrMap(final List<?> objects)
      throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return Maps.newHashMap();
    } else {
      final Map<String, byte[]> xAttrs = Maps.newHashMap();
      for (Object object : objects) {
        Map<?, ?> m = (Map<?, ?>) object;
        String name = (String) m.get("name");
        String value = (String) m.get("value");
        xAttrs.put(name, decodeXAttrValue(value));
      }
      return xAttrs;
    }
  }

  static byte[] decodeXAttrValue(String value) throws IOException {
    if (value != null) {
      return XAttrCodec.decodeValue(value);
    } else {
      return new byte[0];
    }
  }

  /** Convert a Json map to a Token of DelegationTokenIdentifier. */
  @SuppressWarnings("unchecked")
  static Token<DelegationTokenIdentifier> toDelegationToken(
      final Map<?, ?> json) throws IOException {
    final Map<?, ?> m = (Map<?, ?>)json.get(Token.class.getSimpleName());
    return (Token<DelegationTokenIdentifier>) toToken(m);
  }

  /** Convert a Json map to LocatedBlock. */
  static LocatedBlocks toLocatedBlocks(
      final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(
        LocatedBlocks.class.getSimpleName());
    final long fileLength = ((Number) m.get("fileLength")).longValue();
    final boolean isUnderConstruction = (Boolean)m.get("isUnderConstruction");
    final List<LocatedBlock> locatedBlocks = toLocatedBlockList(
        getList(m, "locatedBlocks"));
    final LocatedBlock lastLocatedBlock = toLocatedBlock(
        (Map<?, ?>) m.get("lastLocatedBlock"));
    final boolean isLastBlockComplete = (Boolean)m.get("isLastBlockComplete");
    return new LocatedBlocks(fileLength, isUnderConstruction, locatedBlocks,
        lastLocatedBlock, isLastBlockComplete, null, null);
  }

  public static Collection<BlockStoragePolicy> getStoragePolicies(
      Map<?, ?> json) {
    Map<?, ?> policiesJson = (Map<?, ?>) json.get("BlockStoragePolicies");
    if (policiesJson != null) {
      List<?> objs = (List<?>) policiesJson.get(BlockStoragePolicy.class
          .getSimpleName());
      if (objs != null) {
        BlockStoragePolicy[] storagePolicies = new BlockStoragePolicy[objs
            .size()];
        for (int i = 0; i < objs.size(); i++) {
          final Map<?, ?> m = (Map<?, ?>) objs.get(i);
          BlockStoragePolicy blockStoragePolicy = toBlockStoragePolicy(m);
          storagePolicies[i] = blockStoragePolicy;
        }
        return Arrays.asList(storagePolicies);
      }
    }
    return new ArrayList<BlockStoragePolicy>(0);
  }

  public static BlockStoragePolicy toBlockStoragePolicy(Map<?, ?> m) {
    byte id = ((Number) m.get("id")).byteValue();
    String name = (String) m.get("name");
    StorageType[] storageTypes = toStorageTypes((List<?>) m
        .get("storageTypes"));
    StorageType[] creationFallbacks = toStorageTypes((List<?>) m
        .get("creationFallbacks"));
    StorageType[] replicationFallbacks = toStorageTypes((List<?>) m
        .get("replicationFallbacks"));
    Boolean copyOnCreateFile = (Boolean) m.get("copyOnCreateFile");
    return new BlockStoragePolicy(id, name, storageTypes, creationFallbacks,
        replicationFallbacks, copyOnCreateFile.booleanValue());
  }

  private static StorageType[] toStorageTypes(List<?> list) {
    if (list == null) {
      return null;
    } else {
      StorageType[] storageTypes = new StorageType[list.size()];
      for (int i = 0; i < list.size(); i++) {
        storageTypes[i] = StorageType.parseStorageType((String) list.get(i));
      }
      return storageTypes;
    }
  }

  /*
   * The parameters which have default value -1 are required fields according
   * to hdfs.proto.
   * The default values for optional fields are taken from
   * hdfs.proto#FsServerDefaultsProto.
   */
  public static FsServerDefaults toFsServerDefaults(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }
    Map<?, ?> m =
        (Map<?, ?>) json.get(FsServerDefaults.class.getSimpleName());
    long blockSize =  getLong(m, "blockSize", -1);
    int bytesPerChecksum = getInt(m, "bytesPerChecksum", -1);
    int writePacketSize = getInt(m, "writePacketSize", -1);
    short replication = (short) getInt(m, "replication", -1);
    int fileBufferSize = getInt(m, "fileBufferSize", -1);
    boolean encryptDataTransfer = m.containsKey("encryptDataTransfer")
        ? (Boolean) m.get("encryptDataTransfer")
        : false;
    long trashInterval = getLong(m, "trashInterval", 0);
    DataChecksum.Type type =
        DataChecksum.Type.valueOf(getInt(m, "checksumType", 1));
    String keyProviderUri = (String) m.get("keyProviderUri");
    byte storagepolicyId = m.containsKey("defaultStoragePolicyId")
        ? ((Number) m.get("defaultStoragePolicyId")).byteValue()
        : HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
    return new FsServerDefaults(blockSize, bytesPerChecksum,
        writePacketSize, replication, fileBufferSize,
        encryptDataTransfer, trashInterval, type, keyProviderUri,
        storagepolicyId);
  }

  public static SnapshotDiffReport toSnapshotDiffReport(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }
    Map<?, ?> m =
        (Map<?, ?>) json.get(SnapshotDiffReport.class.getSimpleName());
    String snapshotRoot = (String) m.get("snapshotRoot");
    String fromSnapshot = (String) m.get("fromSnapshot");
    String toSnapshot = (String) m.get("toSnapshot");
    List<SnapshotDiffReport.DiffReportEntry> diffList =
        toDiffList(getList(m, "diffList"));
    return new SnapshotDiffReport(snapshotRoot, fromSnapshot, toSnapshot,
        diffList);
  }

  private static List<SnapshotDiffReport.DiffReportEntry> toDiffList(
      List<?> objs) {
    if (objs == null) {
      return null;
    }
    List<SnapshotDiffReport.DiffReportEntry> diffList =
        new ChunkedArrayList<>();
    for (int i = 0; i < objs.size(); i++) {
      diffList.add(toDiffReportEntry((Map<?, ?>) objs.get(i)));
    }
    return diffList;
  }

  private static SnapshotDiffReport.DiffReportEntry toDiffReportEntry(
      Map<?, ?> json) {
    if (json == null) {
      return null;
    }
    SnapshotDiffReport.DiffType type =
        SnapshotDiffReport.DiffType.parseDiffType((String) json.get("type"));
    byte[] sourcePath = toByteArray((String) json.get("sourcePath"));
    byte[] targetPath = toByteArray((String) json.get("targetPath"));
    return new SnapshotDiffReport.DiffReportEntry(type, sourcePath, targetPath);
  }

  private static byte[] toByteArray(String str) {
    if (str == null) {
      return null;
    }
    return DFSUtilClient.string2Bytes(str);
  }

  public static SnapshottableDirectoryStatus[] toSnapshottableDirectoryList(
      final Map<?, ?> json) {
    if (json == null) {
      return null;
    }
    List<?> list = (List<?>) json.get("SnapshottableDirectoryList");
    if (list == null) {
      return null;
    }
    SnapshottableDirectoryStatus[] statuses =
        new SnapshottableDirectoryStatus[list.size()];
    for (int i = 0; i < list.size(); i++) {
      statuses[i] = toSnapshottableDirectoryStatus((Map<?, ?>) list.get(i));
    }
    return statuses;
  }

  private static SnapshottableDirectoryStatus toSnapshottableDirectoryStatus(
      Map<?, ?> json) {
    if (json == null) {
      return null;
    }
    int snapshotNumber = getInt(json, "snapshotNumber", 0);
    int snapshotQuota = getInt(json, "snapshotQuota", 0);
    byte[] parentFullPath = toByteArray((String) json.get("parentFullPath"));
    HdfsFileStatus dirStatus =
        toFileStatus((Map<?, ?>) json.get("dirStatus"), false);
    SnapshottableDirectoryStatus snapshottableDirectoryStatus =
        new SnapshottableDirectoryStatus(dirStatus, snapshotNumber,
            snapshotQuota, parentFullPath);
    return snapshottableDirectoryStatus;
  }
}
