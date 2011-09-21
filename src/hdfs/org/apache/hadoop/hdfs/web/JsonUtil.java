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

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.mortbay.util.ajax.JSON;

/** JSON Utilities */
public class JsonUtil {
  private static class ThreadLocalMap extends ThreadLocal<Map<String, Object>> {
    @Override
    protected Map<String, Object> initialValue() {
      return new TreeMap<String, Object>();
    }

    @Override
    public Map<String, Object> get() {
      final Map<String, Object> m = super.get();
      m.clear();
      return m;
    }
  }

  private static final ThreadLocalMap jsonMap = new ThreadLocalMap();
  private static final ThreadLocalMap tokenMap = new ThreadLocalMap();
  private static final ThreadLocalMap datanodeInfoMap = new ThreadLocalMap();
  private static final ThreadLocalMap BlockMap = new ThreadLocalMap();
  private static final ThreadLocalMap locatedBlockMap = new ThreadLocalMap();

  private static final DatanodeInfo[] EMPTY_DATANODE_INFO_ARRAY = {};

  /** Convert a token object to a Json string. */
  public static String toJsonString(final Token<? extends TokenIdentifier> token
      ) throws IOException {
    if (token == null) {
      return null;
    }

    final Map<String, Object> m = tokenMap.get();
    m.put("urlString", token.encodeToUrlString());
    return JSON.toString(m);
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
      final Map<?, ?> m) throws IOException {
    return (Token<DelegationTokenIdentifier>)toToken(m);
  }

  /** Convert a Json map to a Token of BlockTokenIdentifier. */
  @SuppressWarnings("unchecked")
  public static Token<BlockTokenIdentifier> toBlockToken(
      final Map<?, ?> m) throws IOException {
    return (Token<BlockTokenIdentifier>)toToken(m);
  }

  /** Convert an exception object to a Json string. */
  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = jsonMap.get();
    m.put("className", e.getClass().getName());
    m.put("message", e.getMessage());
    return JSON.toString(m);
  }

  /** Convert a Json map to a RemoteException. */
  public static RemoteException toRemoteException(final Map<String, Object> m) {
    final String className = (String)m.get("className");
    final String message = (String)m.get("message");
    return new RemoteException(className, message);
  }

  /** Convert a key-value pair to a Json string. */
  public static String toJsonString(final Object key, final Object value) {
    final Map<String, Object> m = jsonMap.get();
    m.put(key instanceof String ? (String) key : key.toString(), value);
    return JSON.toString(m);
  }

  /** Convert a FsPermission object to a string. */
  public static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /** Convert a string to a FsPermission object. */
  public static FsPermission toFsPermission(final String s) {
    return new FsPermission(Short.parseShort(s, 8));
  }

  /** Convert a HdfsFileStatus object to a Json string. */
  public static String toJsonString(final HdfsFileStatus status) {
    if (status == null) {
      return null;
    } else {
      final Map<String, Object> m = jsonMap.get();
      m.put("localName", status.getLocalName());
      m.put("isDir", status.isDir());
      m.put("len", status.getLen());
      m.put("owner", status.getOwner());
      m.put("group", status.getGroup());
      m.put("permission", toString(status.getPermission()));
      m.put("accessTime", status.getAccessTime());
      m.put("modificationTime", status.getModificationTime());
      m.put("blockSize", status.getBlockSize());
      m.put("replication", status.getReplication());
      return JSON.toString(m);
    }
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> parse(String jsonString) {
    return (Map<String, Object>) JSON.parse(jsonString);
  }

  /** Convert a Json map to a HdfsFileStatus object. */
  public static HdfsFileStatus toFileStatus(final Map<String, Object> m) {
    if (m == null) {
      return null;
    }

    final String localName = (String) m.get("localName");
    final boolean isDir = (Boolean) m.get("isDir");
    final long len = (Long) m.get("len");
    final String owner = (String) m.get("owner");
    final String group = (String) m.get("group");
    final FsPermission permission = toFsPermission((String) m.get("permission"));
    final long aTime = (Long) m.get("accessTime");
    final long mTime = (Long) m.get("modificationTime");
    final long blockSize = (Long) m.get("blockSize");
    final short replication = (short) (long) (Long) m.get("replication");
    return new HdfsFileStatus(len, isDir, replication, blockSize, mTime, aTime,
        permission, owner, group, DFSUtil.string2Bytes(localName));
  }

  /** Convert a LocatedBlock to a Json string. */
  public static String toJsonString(final Block Block) {
    if (Block == null) {
      return null;
    }

    final Map<String, Object> m = BlockMap.get();
    m.put("blockId", Block.getBlockId());
    m.put("numBytes", Block.getNumBytes());
    m.put("generationStamp", Block.getGenerationStamp());
    return JSON.toString(m);
  }

  /** Convert a Json map to an Block object. */
  public static Block toBlock(final Map<?, ?> m) {
    if (m == null) {
      return null;
    }
    
    final long blockId = (Long)m.get("blockId");
    final long numBytes = (Long)m.get("numBytes");
    final long generationStamp = (Long)m.get("generationStamp");
    return new Block(blockId, numBytes, generationStamp);
  }
  
  /** Convert a DatanodeInfo to a Json string. */
  public static String toJsonString(final DatanodeInfo datanodeinfo) {
    if (datanodeinfo == null) {
      return null;
    }

    final Map<String, Object> m = datanodeInfoMap.get();
    m.put("name", datanodeinfo.getName());
    m.put("storageID", datanodeinfo.getStorageID());
    m.put("infoPort", datanodeinfo.getInfoPort());

    m.put("ipcPort", datanodeinfo.getIpcPort());

    m.put("capacity", datanodeinfo.getCapacity());
    m.put("dfsUsed", datanodeinfo.getDfsUsed());
    m.put("remaining", datanodeinfo.getRemaining());
    m.put("lastUpdate", datanodeinfo.getLastUpdate());
    m.put("xceiverCount", datanodeinfo.getXceiverCount());
    m.put("networkLocation", datanodeinfo.getNetworkLocation());
    m.put("hostName", datanodeinfo.getHostName());
    m.put("adminState", datanodeinfo.getAdminState().name());
    return JSON.toString(m);
  }

  /** Convert a Json map to an DatanodeInfo object. */
  public static DatanodeInfo toDatanodeInfo(final Map<?, ?> m) {
    if (m == null) {
      return null;
    }

    return new DatanodeInfo(
        (String)m.get("name"),
        (String)m.get("storageID"),
        (int)(long)(Long)m.get("infoPort"),
        (int)(long)(Long)m.get("ipcPort"),

        (Long)m.get("capacity"),
        (Long)m.get("dfsUsed"),
        (Long)m.get("remaining"),
        (Long)m.get("lastUpdate"),
        (int)(long)(Long)m.get("xceiverCount"),
        (String)m.get("networkLocation"),
        (String)m.get("hostName"),
        AdminStates.valueOf((String)m.get("adminState")));
  }

  /** Convert a DatanodeInfo[] to a Json string. */
  public static String toJsonString(final DatanodeInfo[] array
      ) throws IOException {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return "[]";
    } else {
      final StringBuilder b = new StringBuilder().append('[').append(
          toJsonString(array[0]));
      for(int i = 1; i < array.length; i++) {
        b.append(", ").append(toJsonString(array[i]));
      }
      return b.append(']').toString();
    }
  }

  /** Convert an Object[] to a DatanodeInfo[]. */
  public static DatanodeInfo[] toDatanodeInfoArray(final Object[] objects) {
    if (objects == null) {
      return null;
    } else if (objects.length == 0) {
      return EMPTY_DATANODE_INFO_ARRAY;
    } else {
      final DatanodeInfo[] array = new DatanodeInfo[objects.length];
      for(int i = 0; i < array.length; i++) {
        array[i] = (DatanodeInfo)toDatanodeInfo((Map<?, ?>) objects[i]);
      }
      return array;
    }
  }

  /** Convert a LocatedBlock to a Json string. */
  public static String toJsonString(final LocatedBlock locatedblock
      ) throws IOException {
    if (locatedblock == null) {
      return null;
    }
 
    final Map<String, Object> m = locatedBlockMap.get();
    m.put("blockToken", toJsonString(locatedblock.getBlockToken()));
    m.put("isCorrupt", locatedblock.isCorrupt());
    m.put("startOffset", locatedblock.getStartOffset());
    m.put("block", toJsonString(locatedblock.getBlock()));

    m.put("locations", toJsonString(locatedblock.getLocations()));
    return JSON.toString(m);
  }

  /** Convert a Json map to LocatedBlock. */
  public static LocatedBlock toLocatedBlock(final Map<?, ?> m) throws IOException {
    if (m == null) {
      return null;
    }

    final Block b = toBlock((Map<?, ?>)JSON.parse((String)m.get("block")));
    final DatanodeInfo[] locations = toDatanodeInfoArray(
        (Object[])JSON.parse((String)m.get("locations")));
    final long startOffset = (Long)m.get("startOffset");
    final boolean isCorrupt = (Boolean)m.get("isCorrupt");

    final LocatedBlock locatedblock = new LocatedBlock(b, locations, startOffset, isCorrupt);
    locatedblock.setBlockToken(toBlockToken((Map<?, ?>)JSON.parse((String)m.get("blockToken"))));
    return locatedblock;
  }

  /** Convert a LocatedBlock[] to a Json string. */
  public static String toJsonString(final List<LocatedBlock> array
      ) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return "[]";
    } else {
      final StringBuilder b = new StringBuilder().append('[').append(
          toJsonString(array.get(0)));
      for(int i = 1; i < array.size(); i++) {
        b.append(",\n  ").append(toJsonString(array.get(i)));
      }
      return b.append(']').toString();
    }
  }

  /** Convert an Object[] to a List of LocatedBlock. 
   * @throws IOException */
  public static List<LocatedBlock> toLocatedBlockList(final Object[] objects
      ) throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.length == 0) {
      return Collections.emptyList();
    } else {
      final List<LocatedBlock> list = new ArrayList<LocatedBlock>(objects.length);
      for(int i = 0; i < objects.length; i++) {
        list.add((LocatedBlock)toLocatedBlock((Map<?, ?>)objects[i]));
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

    final Map<String, Object> m = jsonMap.get();
    m.put("fileLength", locatedblocks.getFileLength());
    m.put("isUnderConstruction", locatedblocks.isUnderConstruction());

    m.put("locatedBlocks", toJsonString(locatedblocks.getLocatedBlocks()));
    return JSON.toString(m);
  }

  /** Convert a Json map to LocatedBlock. */
  public static LocatedBlocks toLocatedBlocks(final Map<String, Object> m
      ) throws IOException {
    if (m == null) {
      return null;
    }
    
    final long fileLength = (Long)m.get("fileLength");
    final boolean isUnderConstruction = (Boolean)m.get("isUnderConstruction");
    final List<LocatedBlock> locatedBlocks = toLocatedBlockList(
        (Object[])JSON.parse((String) m.get("locatedBlocks")));
    return new LocatedBlocks(fileLength, locatedBlocks, isUnderConstruction);
  }
}