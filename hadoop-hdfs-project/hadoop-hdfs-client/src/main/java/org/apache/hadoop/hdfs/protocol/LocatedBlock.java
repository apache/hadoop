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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, a location is cached in memory,
 * security token, etc).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock {

  /**
   * Comparator that ensures that a PROVIDED storage type is greater than any
   * other storage type. Any other storage types are considered equal.
   */
  private static class ProvidedLastComparator
      implements Comparator<DatanodeInfoWithStorage>, Serializable {

    private static final long serialVersionUID = 6441720011443190984L;

    @Override
    public int compare(DatanodeInfoWithStorage dns1,
        DatanodeInfoWithStorage dns2) {
      if (StorageType.PROVIDED.equals(dns1.getStorageType())
          && !StorageType.PROVIDED.equals(dns2.getStorageType())) {
        return 1;
      }
      if (!StorageType.PROVIDED.equals(dns1.getStorageType())
          && StorageType.PROVIDED.equals(dns2.getStorageType())) {
        return -1;
      }
      // Storage types of dns1 and dns2 are now both provided or not provided;
      // thus, are essentially equal for the purpose of this comparator.
      return 0;
    }
  }

  private final ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file
  private final DatanodeInfoWithStorage[] locs;
  /** Cached storage ID for each replica */
  private final String[] storageIDs;
  /** Cached storage type for each replica, if reported. */
  private final StorageType[] storageTypes;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken = new Token<>();

  // use one instance of the Provided comparator as it uses no state.
  private static ProvidedLastComparator providedLastComparator =
      new ProvidedLastComparator();
  /**
   * List of cached datanode locations
   */
  private DatanodeInfo[] cachedLocs;

  // Used when there are no locations
  static final DatanodeInfoWithStorage[] EMPTY_LOCS =
      new DatanodeInfoWithStorage[0];

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    // By default, startOffset is unknown(-1) and corrupt is false.
    this(b, convert(locs, null, null), null, null, -1, false, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs,
      String[] storageIDs, StorageType[] storageTypes) {
    this(b, convert(locs, storageIDs, storageTypes),
         storageIDs, storageTypes, -1, false, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs,
      String[] storageIDs, StorageType[] storageTypes, long startOffset,
      boolean corrupt, DatanodeInfo[] cachedLocs) {
    this(b, convert(locs, storageIDs, storageTypes),
        storageIDs, storageTypes, startOffset, corrupt,
        null == cachedLocs || 0 == cachedLocs.length ? EMPTY_LOCS : cachedLocs);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfoWithStorage[] locs,
      String[] storageIDs, StorageType[] storageTypes, long startOffset,
      boolean corrupt, DatanodeInfo[] cachedLocs) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    this.locs = null == locs ? EMPTY_LOCS : locs;
    this.storageIDs = storageIDs;
    this.storageTypes = storageTypes;
    this.cachedLocs = null == cachedLocs || 0 == cachedLocs.length
      ? EMPTY_LOCS
      : cachedLocs;
  }

  private static DatanodeInfoWithStorage[] convert(
      DatanodeInfo[] infos, String[] storageIDs, StorageType[] storageTypes) {
    if (null == infos) {
      return EMPTY_LOCS;
    }

    DatanodeInfoWithStorage[] ret = new DatanodeInfoWithStorage[infos.length];
    for(int i = 0; i < infos.length; i++) {
      ret[i] = new DatanodeInfoWithStorage(infos[i],
          storageIDs   != null ? storageIDs[i]   : null,
          storageTypes != null ? storageTypes[i] : null);
    }
    return ret;
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
  public DatanodeInfoWithStorage[] getLocations() {
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

  /**
   * Moves all locations that have {@link StorageType}
   * {@code PROVIDED} to the end of the locations array without
   * changing the relative ordering of the remaining locations
   * Only the first {@code activeLen} locations are considered.
   * The caller must immediately invoke {@link
   * org.apache.hadoop.hdfs.protocol.LocatedBlock#updateCachedStorageInfo}
   * to update the cached Storage ID/Type arrays.
   * @param activeLen
   */
  public void moveProvidedToEnd(int activeLen) {

    if (activeLen <= 0) {
      return;
    }
    // as this is a stable sort, for elements that are equal,
    // the current order of the elements is maintained
    Arrays.sort(locs, 0, (activeLen < locs.length) ? activeLen : locs.length,
        providedLastComparator);
  }

  public long getStartOffset() {
    return offset;
  }

  public long getBlockSize() {
    return b.getNumBytes();
  }

  public void setStartOffset(long value) {
    this.offset = value;
  }

  public void setCorrupt(boolean corrupt) {
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
    Preconditions.checkArgument(cachedLocs != EMPTY_LOCS,
        "Cached locations should only be added when having a backing"
            + " disk replica!", loc, locs.length, Arrays.toString(locs));
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
        + "; cachedLocs=" + Arrays.asList(cachedLocs)
        + "}";
  }

  public boolean isStriped() {
    return false;
  }

  public BlockType getBlockType() {
    return BlockType.CONTIGUOUS;
  }
}
