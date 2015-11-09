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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.ContentSummary.Builder;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class JsonUtilClient {
  static final DatanodeInfo[] EMPTY_DATANODE_INFO_ARRAY = {};

  /** Convert a Json map to a RemoteException. */
  static RemoteException toRemoteException(final Map<?, ?> json) {
    final Map<?, ?> m = (Map<?, ?>)json.get(
        RemoteException.class.getSimpleName());
    final String message = (String)m.get("message");
    final String javaClassName = (String)m.get("javaClassName");
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
  static FsPermission toFsPermission(
      final String s, Boolean aclBit, Boolean encBit) {
    FsPermission perm = new FsPermission(Short.parseShort(s, 8));
    final boolean aBit = (aclBit != null) ? aclBit : false;
    final boolean eBit = (encBit != null) ? encBit : false;
    if (aBit || eBit) {
      return new FsPermissionExtension(perm, aBit, eBit);
    } else {
      return perm;
    }
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
    final FsPermission permission = toFsPermission((String) m.get("permission"),
        (Boolean) m.get("aclBit"),
        (Boolean) m.get("encBit"));
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
    return new HdfsFileStatus(len, type == WebHdfsConstants.PathType.DIRECTORY,
        replication, blockSize, mTime, aTime, permission, owner, group,
        symlink, DFSUtilClient.string2Bytes(localName),
        fileId, childrenNum, null,
        storagePolicy, null);
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
        DatanodeInfo.AdminStates.valueOf(getString(m, "adminState", "NORMAL")),
        getString(m, "upgradeDomain", ""));
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

    final LocatedBlock locatedblock = new LocatedBlock(b, locations,
        null, null, startOffset, isCorrupt, cachedLocations);
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

    final Map<?, ?> m = (Map<?, ?>)json.get(
        ContentSummary.class.getSimpleName());
    final long length = ((Number) m.get("length")).longValue();
    final long fileCount = ((Number) m.get("fileCount")).longValue();
    final long directoryCount = ((Number) m.get("directoryCount")).longValue();
    final long quota = ((Number) m.get("quota")).longValue();
    final long spaceConsumed = ((Number) m.get("spaceConsumed")).longValue();
    final long spaceQuota = ((Number) m.get("spaceQuota")).longValue();
    final Map<?, ?> typem = (Map<?, ?>) m.get("typeQuota");

    Builder contentSummaryBuilder = new ContentSummary.Builder().length(length)
        .fileCount(fileCount).directoryCount(directoryCount).quota(quota)
        .spaceConsumed(spaceConsumed).spaceQuota(spaceQuota);
    if (typem != null) {
      for (StorageType t : StorageType.getTypesSupportingQuota()) {
        Map<?, ?> type = (Map<?, ?>) typem.get(t.toString());
        if (type != null) {
          contentSummaryBuilder = contentSummaryBuilder.typeQuota(t,
              ((Number) type.get("quota")).longValue()).typeConsumed(t,
              ((Number) type.get("consumed")).longValue());
        }
      }
    }
    return contentSummaryBuilder.build();
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
      final FsPermission permission = toFsPermission(permString,
          (Boolean) m.get("aclBit"), (Boolean) m.get("encBit"));
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
    ObjectReader reader = new ObjectMapper().reader(List.class);
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

}
