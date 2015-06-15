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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

/** JSON Utilities */
public class JsonUtil {
  private static final Object[] EMPTY_OBJECT_ARRAY = {};
  private static final DatanodeInfo[] EMPTY_DATANODE_INFO_ARRAY = {};

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

  /** Convert a Json map to a Token. */
  public static Token<? extends TokenIdentifier> toToken(
      final Map<?, ?> m) throws IOException {
    if (m == null) {
      return null;
    }

    final Token<DelegationTokenIdentifier> token
        = new Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString((String)m.get("urlString"));
    return token;
  }

  /** Convert a Json map to a Token of DelegationTokenIdentifier. */
  @SuppressWarnings("unchecked")
  public static Token<DelegationTokenIdentifier> toDelegationToken(
      final Map<?, ?> json) throws IOException {
    final Map<?, ?> m = (Map<?, ?>)json.get(Token.class.getSimpleName());
    return (Token<DelegationTokenIdentifier>)toToken(m);
  }

  /** Convert a Json map to a Token of BlockTokenIdentifier. */
  @SuppressWarnings("unchecked")
  private static Token<BlockTokenIdentifier> toBlockToken(
      final Map<?, ?> m) throws IOException {
    return (Token<BlockTokenIdentifier>)toToken(m);
  }

  /** Convert an exception object to a Json string. */
  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("exception", e.getClass().getSimpleName());
    m.put("message", e.getMessage());
    m.put("javaClassName", e.getClass().getName());
    return toJsonString(RemoteException.class, m);
  }

  /** Convert a Json map to a RemoteException. */
  public static RemoteException toRemoteException(final Map<?, ?> json) {
    final Map<?, ?> m = (Map<?, ?>)json.get(RemoteException.class.getSimpleName());
    final String message = (String)m.get("message");
    final String javaClassName = (String)m.get("javaClassName");
    return new RemoteException(javaClassName, message);
  }

  private static String toJsonString(final Class<?> clazz, final Object value) {
    return toJsonString(clazz.getSimpleName(), value);
  }

  /** Convert a key-value pair to a Json string. */
  public static String toJsonString(final String key, final Object value) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put(key, value);
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /** Convert a FsPermission object to a string. */
  private static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /** Convert a string to a FsPermission object. */
  private static FsPermission toFsPermission(final String s, Boolean aclBit,
      Boolean encBit) {
    FsPermission perm = new FsPermission(Short.parseShort(s, 8));
    final boolean aBit = (aclBit != null) ? aclBit : false;
    final boolean eBit = (encBit != null) ? encBit : false;
    if (aBit || eBit) {
      return new FsPermissionExtension(perm, aBit, eBit);
    } else {
      return perm;
    }
  }

  static enum PathType {
    FILE, DIRECTORY, SYMLINK;
    
    static PathType valueOf(HdfsFileStatus status) {
      return status.isDir()? DIRECTORY: status.isSymlink()? SYMLINK: FILE;
    }
  }

  /** Convert a HdfsFileStatus object to a Json string. */
  public static String toJsonString(final HdfsFileStatus status,
      boolean includeType) {
    if (status == null) {
      return null;
    }
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("pathSuffix", status.getLocalName());
    m.put("type", PathType.valueOf(status));
    if (status.isSymlink()) {
      m.put("symlink", status.getSymlink());
    }

    m.put("length", status.getLen());
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    FsPermission perm = status.getPermission();
    m.put("permission", toString(perm));
    if (perm.getAclBit()) {
      m.put("aclBit", true);
    }
    if (perm.getEncryptedBit()) {
      m.put("encBit", true);
    }
    m.put("accessTime", status.getAccessTime());
    m.put("modificationTime", status.getModificationTime());
    m.put("blockSize", status.getBlockSize());
    m.put("replication", status.getReplication());
    m.put("fileId", status.getFileId());
    m.put("childrenNum", status.getChildrenNum());
    m.put("storagePolicy", status.getStoragePolicy());
    ObjectMapper mapper = new ObjectMapper();
    try {
      return includeType ?
          toJsonString(FileStatus.class, m) : mapper.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /** Convert a Json map to a HdfsFileStatus object. */
  public static HdfsFileStatus toFileStatus(final Map<?, ?> json, boolean includesType) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = includesType ? 
        (Map<?, ?>)json.get(FileStatus.class.getSimpleName()) : json;
    final String localName = (String) m.get("pathSuffix");
    final PathType type = PathType.valueOf((String) m.get("type"));
    final byte[] symlink = type != PathType.SYMLINK? null
        : DFSUtil.string2Bytes((String)m.get("symlink"));

    final long len = ((Number) m.get("length")).longValue();
    final String owner = (String) m.get("owner");
    final String group = (String) m.get("group");
    final FsPermission permission = toFsPermission((String) m.get("permission"),
      (Boolean)m.get("aclBit"), (Boolean)m.get("encBit"));
    final long aTime = ((Number) m.get("accessTime")).longValue();
    final long mTime = ((Number) m.get("modificationTime")).longValue();
    final long blockSize = ((Number) m.get("blockSize")).longValue();
    final short replication = ((Number) m.get("replication")).shortValue();
    final long fileId = m.containsKey("fileId") ?
        ((Number) m.get("fileId")).longValue() : INodeId.GRANDFATHER_INODE_ID;
    final int childrenNum = getInt(m, "childrenNum", -1);
    final byte storagePolicy = m.containsKey("storagePolicy") ?
        (byte) ((Number) m.get("storagePolicy")).longValue() :
        BlockStoragePolicySuite.ID_UNSPECIFIED;
    return new HdfsFileStatus(len, type == PathType.DIRECTORY, replication,
        blockSize, mTime, aTime, permission, owner, group, symlink,
        DFSUtil.string2Bytes(localName), fileId, childrenNum, null, storagePolicy);
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

  /** Convert a Json map to an ExtendedBlock object. */
  private static ExtendedBlock toExtendedBlock(final Map<?, ?> m) {
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
    return m;
  }

  private static int getInt(Map<?, ?> m, String key, final int defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return ((Number) value).intValue();
  }

  private static long getLong(Map<?, ?> m, String key, final long defaultValue) {
    Object value = m.get(key);
    if (value == null) {
      return defaultValue;
    }
    return ((Number) value).longValue();
  }

  private static String getString(Map<?, ?> m, String key,
      final String defaultValue) {
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

  /** Convert a Json map to an DatanodeInfo object. */
  static DatanodeInfo toDatanodeInfo(final Map<?, ?> m)
      throws IOException {
    if (m == null) {
      return null;
    }

    // ipAddr and xferPort are the critical fields for accessing data.
    // If any one of the two is missing, an exception needs to be thrown.

    // Handle the case of old servers (1.x, 0.23.x) sending 'name' instead
    // of ipAddr and xferPort.
    int xferPort = getInt(m, "xferPort", -1);
    Object tmpValue = m.get("ipAddr");
    String ipAddr = (tmpValue == null) ? null : (String)tmpValue;
    if (ipAddr == null) {
      tmpValue = m.get("name");
      if (tmpValue != null) {
        String name = (String)tmpValue;
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
    return new DatanodeInfo(
        ipAddr,
        (String)m.get("hostName"),
        (String)m.get("storageID"),
        xferPort,
        ((Number) m.get("infoPort")).intValue(),
        getInt(m, "infoSecurePort", 0),
        ((Number) m.get("ipcPort")).intValue(),

        getLong(m, "capacity", 0l),
        getLong(m, "dfsUsed", 0l),
        getLong(m, "remaining", 0l),
        getLong(m, "blockPoolUsed", 0l),
        getLong(m, "cacheCapacity", 0l),
        getLong(m, "cacheUsed", 0l),
        getLong(m, "lastUpdate", 0l),
        getLong(m, "lastUpdateMonotonic", 0l),
        getInt(m, "xceiverCount", 0),
        getString(m, "networkLocation", ""),
        AdminStates.valueOf(getString(m, "adminState", "NORMAL")));
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

  /** Convert an Object[] to a DatanodeInfo[]. */
  private static DatanodeInfo[] toDatanodeInfoArray(final List<?> objects)
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
    m.put("locations", toJsonArray(locatedblock.getLocations()));
    m.put("cachedLocations", toJsonArray(locatedblock.getCachedLocations()));
    return m;
  }

  /** Convert a Json map to LocatedBlock. */
  private static LocatedBlock toLocatedBlock(final Map<?, ?> m) throws IOException {
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

    final LocatedBlock locatedblock = new LocatedBlock(b, locations,
        null, null, startOffset, isCorrupt, cachedLocations);
    locatedblock.setBlockToken(toBlockToken((Map<?, ?>)m.get("blockToken")));
    return locatedblock;
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

  /** Convert an List of Object to a List of LocatedBlock. */
  private static List<LocatedBlock> toLocatedBlockList(
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

  /** Convert a Json map to LocatedBlock. */
  public static LocatedBlocks toLocatedBlocks(final Map<?, ?> json
      ) throws IOException {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(LocatedBlocks.class.getSimpleName());
    final long fileLength = ((Number) m.get("fileLength")).longValue();
    final boolean isUnderConstruction = (Boolean)m.get("isUnderConstruction");
    final List<LocatedBlock> locatedBlocks = toLocatedBlockList(
        getList(m, "locatedBlocks"));
    final LocatedBlock lastLocatedBlock = toLocatedBlock(
        (Map<?, ?>)m.get("lastLocatedBlock"));
    final boolean isLastBlockComplete = (Boolean)m.get("isLastBlockComplete");
    return new LocatedBlocks(fileLength, isUnderConstruction, locatedBlocks,
        lastLocatedBlock, isLastBlockComplete, null);
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
    return toJsonString(ContentSummary.class, m);
  }

  /** Convert a Json map to a ContentSummary. */
  public static ContentSummary toContentSummary(final Map<?, ?> json) {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(ContentSummary.class.getSimpleName());
    final long length = ((Number) m.get("length")).longValue();
    final long fileCount = ((Number) m.get("fileCount")).longValue();
    final long directoryCount = ((Number) m.get("directoryCount")).longValue();
    final long quota = ((Number) m.get("quota")).longValue();
    final long spaceConsumed = ((Number) m.get("spaceConsumed")).longValue();
    final long spaceQuota = ((Number) m.get("spaceQuota")).longValue();

    return new ContentSummary.Builder().length(length).fileCount(fileCount).
        directoryCount(directoryCount).quota(quota).spaceConsumed(spaceConsumed).
        spaceQuota(spaceQuota).build();
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

  /** Convert a Json map to a MD5MD5CRC32FileChecksum. */
  public static MD5MD5CRC32FileChecksum toMD5MD5CRC32FileChecksum(
      final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(FileChecksum.class.getSimpleName());
    final String algorithm = (String)m.get("algorithm");
    final int length = ((Number) m.get("length")).intValue();
    final byte[] bytes = StringUtils.hexStringToByte((String)m.get("bytes"));

    final DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
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
      stringEntries.add(entry.toString());
    }
    m.put("entries", stringEntries);

    FsPermission perm = status.getPermission();
    if (perm != null) {
      m.put("permission", toString(perm));
      if (perm.getAclBit()) {
        m.put("aclBit", true);
      }
      if (perm.getEncryptedBit()) {
        m.put("encBit", true);
      }
    }
    final Map<String, Map<String, Object>> finalMap =
        new TreeMap<String, Map<String, Object>>();
    finalMap.put(AclStatus.class.getSimpleName(), m);

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(finalMap);
    } catch (IOException ignored) {
    }
    return null;
  }

  /** Convert a Json map to a AclStatus object. */
  public static AclStatus toAclStatus(final Map<?, ?> json) {
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
      final FsPermission permission = toFsPermission(permString,
          (Boolean) m.get("aclBit"), (Boolean) m.get("encBit"));
      aclStatusBuilder.setPermission(permission);
    }
    final List<?> entries = (List<?>) m.get("entries");

    List<AclEntry> aclEntryList = new ArrayList<AclEntry>();
    for (Object entry : entries) {
      AclEntry aclEntry = AclEntry.parseAclEntry((String) entry, true);
      aclEntryList.add(aclEntry);
    }
    aclStatusBuilder.addEntries(aclEntryList);
    return aclStatusBuilder.build();
  }
  
  private static Map<String, Object> toJsonMap(final XAttr xAttr,
      final XAttrCodec encoding) throws IOException {
    if (xAttr == null) {
      return null;
    }
 
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("name", XAttrHelper.getPrefixName(xAttr));
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
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(finalMap);
  }
  
  public static String toJsonString(final List<XAttr> xAttrs)
      throws IOException {
    final List<String> names = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      names.add(XAttrHelper.getPrefixName(xAttr));
    }
    ObjectMapper mapper = new ObjectMapper();
    String ret = mapper.writeValueAsString(names);
    final Map<String, Object> finalMap = new TreeMap<String, Object>();
    finalMap.put("XAttrNames", ret);
    return mapper.writeValueAsString(finalMap);
  }
  
  public static byte[] getXAttr(final Map<?, ?> json, final String name) 
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

  public static Map<String, byte[]> toXAttrs(final Map<?, ?> json) 
      throws IOException {
    if (json == null) {
      return null;
    }
    return toXAttrMap(getList(json, "XAttrs"));
  }
  
  public static List<String> toXAttrNames(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }

    final String namesInJson = (String) json.get("XAttrNames");
    ObjectReader reader = new ObjectMapper().reader(List.class);
    final List<Object> xattrs = reader.readValue(namesInJson);
    final List<String> names =
      Lists.newArrayListWithCapacity(json.keySet().size());

    for (Object xattr : xattrs) {
      names.add((String) xattr);
    }
    return names;
  }

  private static Map<String, byte[]> toXAttrMap(final List<?> objects)
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
  
  private static byte[] decodeXAttrValue(String value) throws IOException {
    if (value != null) {
      return XAttrCodec.decodeValue(value);
    } else {
      return new byte[0];
    }
  }
}
