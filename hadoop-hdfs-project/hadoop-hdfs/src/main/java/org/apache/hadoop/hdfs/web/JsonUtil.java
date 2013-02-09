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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
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
import org.mortbay.util.ajax.JSON;

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

  /** Convert a Token[] to a JSON array. */
  private static Object[] toJsonArray(final Token<? extends TokenIdentifier>[] array
      ) throws IOException {
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

  /** Convert a token object to a JSON string. */
  public static String toJsonString(final Token<? extends TokenIdentifier>[] tokens
      ) throws IOException {
    if (tokens == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put(Token.class.getSimpleName(), toJsonArray(tokens));
    return toJsonString(Token.class.getSimpleName() + "s", m);
  }

  /** Convert an Object[] to a List<Token<?>>.  */
  private static List<Token<?>> toTokenList(final Object[] objects) throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.length == 0) {
      return Collections.emptyList();
    } else {
      final List<Token<?>> list = new ArrayList<Token<?>>(objects.length);
      for(int i = 0; i < objects.length; i++) {
        list.add(toToken((Map<?, ?>)objects[i]));
      }
      return list;
    }
  }

  /** Convert a JSON map to a List<Token<?>>. */
  public static List<Token<?>> toTokenList(final Map<?, ?> json) throws IOException {
    if (json == null) {
      return null;
    }

    final Map<?, ?> m = (Map<?, ?>)json.get(Token.class.getSimpleName() + "s");
    return toTokenList((Object[])m.get(Token.class.getSimpleName()));
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
    return JSON.toString(m);
  }

  /** Convert a FsPermission object to a string. */
  private static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /** Convert a string to a FsPermission object. */
  private static FsPermission toFsPermission(final String s) {
    return new FsPermission(Short.parseShort(s, 8));
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
    m.put("permission", toString(status.getPermission()));
    m.put("accessTime", status.getAccessTime());
    m.put("modificationTime", status.getModificationTime());
    m.put("blockSize", status.getBlockSize());
    m.put("replication", status.getReplication());
    m.put("fileId", status.getFileId());
    return includeType ? toJsonString(FileStatus.class, m): JSON.toString(m);
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

    final long len = (Long) m.get("length");
    final String owner = (String) m.get("owner");
    final String group = (String) m.get("group");
    final FsPermission permission = toFsPermission((String) m.get("permission"));
    final long aTime = (Long) m.get("accessTime");
    final long mTime = (Long) m.get("modificationTime");
    final long blockSize = (Long) m.get("blockSize");
    final short replication = (short) (long) (Long) m.get("replication");
    final long fileId = (Long) m.get("fileId");
    return new HdfsFileStatus(len, type == PathType.DIRECTORY, replication,
        blockSize, mTime, aTime, permission, owner, group,
        symlink, DFSUtil.string2Bytes(localName), fileId);
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
    final long blockId = (Long)m.get("blockId");
    final long numBytes = (Long)m.get("numBytes");
    final long generationStamp = (Long)m.get("generationStamp");
    return new ExtendedBlock(blockPoolId, blockId, numBytes, generationStamp);
  }
  
  /** Convert a DatanodeInfo to a Json map. */
  private static Map<String, Object> toJsonMap(final DatanodeInfo datanodeinfo) {
    if (datanodeinfo == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("ipAddr", datanodeinfo.getIpAddr());
    m.put("hostName", datanodeinfo.getHostName());
    m.put("storageID", datanodeinfo.getStorageID());
    m.put("xferPort", datanodeinfo.getXferPort());
    m.put("infoPort", datanodeinfo.getInfoPort());
    m.put("ipcPort", datanodeinfo.getIpcPort());

    m.put("capacity", datanodeinfo.getCapacity());
    m.put("dfsUsed", datanodeinfo.getDfsUsed());
    m.put("remaining", datanodeinfo.getRemaining());
    m.put("blockPoolUsed", datanodeinfo.getBlockPoolUsed());
    m.put("lastUpdate", datanodeinfo.getLastUpdate());
    m.put("xceiverCount", datanodeinfo.getXceiverCount());
    m.put("networkLocation", datanodeinfo.getNetworkLocation());
    m.put("adminState", datanodeinfo.getAdminState().name());
    return m;
  }

  /** Convert a Json map to an DatanodeInfo object. */
  private static DatanodeInfo toDatanodeInfo(final Map<?, ?> m) {
    if (m == null) {
      return null;
    }

    return new DatanodeInfo(
        (String)m.get("ipAddr"),
        (String)m.get("hostName"),
        (String)m.get("storageID"),
        (int)(long)(Long)m.get("xferPort"),
        (int)(long)(Long)m.get("infoPort"),
        (int)(long)(Long)m.get("ipcPort"),

        (Long)m.get("capacity"),
        (Long)m.get("dfsUsed"),
        (Long)m.get("remaining"),
        (Long)m.get("blockPoolUsed"),
        (Long)m.get("lastUpdate"),
        (int)(long)(Long)m.get("xceiverCount"),
        (String)m.get("networkLocation"),
        AdminStates.valueOf((String)m.get("adminState")));
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
  private static DatanodeInfo[] toDatanodeInfoArray(final Object[] objects) {
    if (objects == null) {
      return null;
    } else if (objects.length == 0) {
      return EMPTY_DATANODE_INFO_ARRAY;
    } else {
      final DatanodeInfo[] array = new DatanodeInfo[objects.length];
      for(int i = 0; i < array.length; i++) {
        array[i] = toDatanodeInfo((Map<?, ?>) objects[i]);
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
    return m;
  }

  /** Convert a Json map to LocatedBlock. */
  private static LocatedBlock toLocatedBlock(final Map<?, ?> m) throws IOException {
    if (m == null) {
      return null;
    }

    final ExtendedBlock b = toExtendedBlock((Map<?, ?>)m.get("block"));
    final DatanodeInfo[] locations = toDatanodeInfoArray(
        (Object[])m.get("locations"));
    final long startOffset = (Long)m.get("startOffset");
    final boolean isCorrupt = (Boolean)m.get("isCorrupt");

    final LocatedBlock locatedblock = new LocatedBlock(b, locations, startOffset, isCorrupt);
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

  /** Convert an Object[] to a List of LocatedBlock. */
  private static List<LocatedBlock> toLocatedBlockList(final Object[] objects
      ) throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.length == 0) {
      return Collections.emptyList();
    } else {
      final List<LocatedBlock> list = new ArrayList<LocatedBlock>(objects.length);
      for(int i = 0; i < objects.length; i++) {
        list.add(toLocatedBlock((Map<?, ?>)objects[i]));
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
    final long fileLength = (Long)m.get("fileLength");
    final boolean isUnderConstruction = (Boolean)m.get("isUnderConstruction");
    final List<LocatedBlock> locatedBlocks = toLocatedBlockList(
        (Object[])m.get("locatedBlocks"));
    final LocatedBlock lastLocatedBlock = toLocatedBlock(
        (Map<?, ?>)m.get("lastLocatedBlock"));
    final boolean isLastBlockComplete = (Boolean)m.get("isLastBlockComplete");
    return new LocatedBlocks(fileLength, isUnderConstruction, locatedBlocks,
        lastLocatedBlock, isLastBlockComplete);
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
    final long length = (Long)m.get("length");
    final long fileCount = (Long)m.get("fileCount");
    final long directoryCount = (Long)m.get("directoryCount");
    final long quota = (Long)m.get("quota");
    final long spaceConsumed = (Long)m.get("spaceConsumed");
    final long spaceQuota = (Long)m.get("spaceQuota");

    return new ContentSummary(length, fileCount, directoryCount,
        quota, spaceConsumed, spaceQuota);
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
    final int length = (int)(long)(Long)m.get("length");
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
}
