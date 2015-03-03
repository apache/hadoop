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
package org.apache.hadoop.hdfs.protocol;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.Lists;

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, a location is cached in memory,
 * security token, etc).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock {

  private final ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file
  private final DatanodeInfoWithStorage[] locs;
  /** Cached storage ID for each replica */
  private String[] storageIDs;
  /** Cached storage type for each replica, if reported. */
  private StorageType[] storageTypes;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken = new Token<BlockTokenIdentifier>();
  /**
   * List of cached datanode locations
   */
  private DatanodeInfo[] cachedLocs;

  // Used when there are no locations
  private static final DatanodeInfoWithStorage[] EMPTY_LOCS =
      new DatanodeInfoWithStorage[0];

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    this(b, locs, -1, false); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset, 
                      boolean corrupt) {
    this(b, locs, null, null, startOffset, corrupt, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages) {
    this(b, storages, -1, false); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs,
                      String[] storageIDs, StorageType[] storageTypes) {
    this(b, locs, storageIDs, storageTypes, -1, false, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages,
      long startOffset, boolean corrupt) {
    this(b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        startOffset, corrupt, EMPTY_LOCS); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs,
                      StorageType[] storageTypes, long startOffset,
                      boolean corrupt, DatanodeInfo[] cachedLocs) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs==null) {
      this.locs = EMPTY_LOCS;
    } else {
      this.locs = new DatanodeInfoWithStorage[locs.length];
      for(int i = 0; i < locs.length; i++) {
        DatanodeInfo di = locs[i];
        DatanodeInfoWithStorage storage = new DatanodeInfoWithStorage(di,
            storageIDs != null ? storageIDs[i] : null,
            storageTypes != null ? storageTypes[i] : null);
        this.locs[i] = storage;
      }
    }
    this.storageIDs = storageIDs;
    this.storageTypes = storageTypes;

    if (cachedLocs == null || cachedLocs.length == 0) {
      this.cachedLocs = EMPTY_LOCS;
    } else {
      this.cachedLocs = cachedLocs;
    }
  }

  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  public ExtendedBlock getBlock() {
    return b;
  }

  /**
   * Returns the locations associated with this block. The returned array is not
   * expected to be modified. If it is, caller must immediately invoke
   * {@link org.apache.hadoop.hdfs.protocol.LocatedBlock#updateCachedStorageInfo}
   * to update the cached Storage ID/Type arrays.
   */
  public DatanodeInfo[] getLocations() {
    return locs;
  }

  public StorageType[] getStorageTypes() {
    return storageTypes;
  }
  
  public String[] getStorageIDs() {
    return storageIDs;
  }

  /**
   * Updates the cached StorageID and StorageType information. Must be
   * called when the locations array is modified.
   */
  public void updateCachedStorageInfo() {
    if (storageIDs != null) {
      for(int i = 0; i < locs.length; i++) {
        storageIDs[i] = locs[i].getStorageID();
      }
    }
    if (storageTypes != null) {
      for(int i = 0; i < locs.length; i++) {
        storageTypes[i] = locs[i].getStorageType();
      }
    }
  }

  public long getStartOffset() {
    return offset;
  }
  
  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }

  void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }
  
  public boolean isCorrupt() {
    return this.corrupt;
  }

  /**
   * Add a the location of a cached replica of the block.
   * 
   * @param loc of datanode with the cached replica
   */
  public void addCachedLoc(DatanodeInfo loc) {
    List<DatanodeInfo> cachedList = Lists.newArrayList(cachedLocs);
    if (cachedList.contains(loc)) {
      return;
    }
    // Try to re-use a DatanodeInfo already in loc
    for (DatanodeInfoWithStorage di : locs) {
      if (loc.equals(di)) {
        cachedList.add(di);
        cachedLocs = cachedList.toArray(cachedLocs);
        return;
      }
    }
    // Not present in loc, add it and go
    cachedList.add(loc);
    cachedLocs = cachedList.toArray(cachedLocs);
  }

  /**
   * @return Datanodes with a cached block replica
   */
  public DatanodeInfo[] getCachedLocations() {
    return cachedLocs;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b
        + "; getBlockSize()=" + getBlockSize()
        + "; corrupt=" + corrupt
        + "; offset=" + offset
        + "; locs=" + Arrays.asList(locs)
        + "}";
  }
}

