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
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A block storage policy describes how to select the storage types
 * for the replicas of a block.
 */
@InterfaceAudience.Private
public class BlockStoragePolicy implements BlockStoragePolicySpi {
  public static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicy
      .class);

  /** A 4-bit policy ID */
  private final byte id;
  /** Policy name */
  private final String name;

  /** The storage types to store the replicas of a new block. */
  private final StorageType[] storageTypes;
  /** The fallback storage type for block creation. */
  private final StorageType[] creationFallbacks;
  /** The fallback storage type for replication. */
  private final StorageType[] replicationFallbacks;
  /**
   * Whether the policy is inherited during file creation.
   * If set then the policy cannot be changed after file creation.
   */
  private boolean copyOnCreateFile;

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks) {
    this(id, name, storageTypes, creationFallbacks, replicationFallbacks,
         false);
  }

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks,
      boolean copyOnCreateFile) {
    this.id = id;
    this.name = name;
    this.storageTypes = storageTypes;
    this.creationFallbacks = creationFallbacks;
    this.replicationFallbacks = replicationFallbacks;
    this.copyOnCreateFile = copyOnCreateFile;
  }

  /**
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  public List<StorageType> chooseStorageTypes(final short replication) {
    final List<StorageType> types = new LinkedList<>();
    int i = 0, j = 0;

    // Do not return transient storage types. We will not have accurate
    // usage information for transient types.
    for (;i < replication && j < storageTypes.length; ++j) {
      if (!storageTypes[j].isTransient()) {
        types.add(storageTypes[j]);
        ++i;
      }
    }

    final StorageType last = storageTypes[storageTypes.length - 1];
    if (!last.isTransient()) {
      for (; i < replication; i++) {
        types.add(last);
      }
    }
    return types;
  }

  /**
   * Choose the storage types for storing the remaining replicas, given the
   * replication number and the storage types of the chosen replicas.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen) {
    return chooseStorageTypes(replication, chosen, null);
  }

  private List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen, final List<StorageType> excess) {
    final List<StorageType> types = chooseStorageTypes(replication);
    diff(types, chosen, excess);
    return types;
  }

  /**
   * Choose the storage types for storing the remaining replicas, given the
   * replication number, the storage types of the chosen replicas and
   * the unavailable storage types. It uses fallback storage in case that
   * the desired storage type is unavailable.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @param unavailables the unavailable storage types.
   * @param isNewBlock Is it for new block creation?
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen,
      final EnumSet<StorageType> unavailables,
      final boolean isNewBlock) {
    final List<StorageType> excess = new LinkedList<>();
    final List<StorageType> storageTypes = chooseStorageTypes(
        replication, chosen, excess);
    final int expectedSize = storageTypes.size() - excess.size();
    final List<StorageType> removed = new LinkedList<>();
    for(int i = storageTypes.size() - 1; i >= 0; i--) {
      // replace/remove unavailable storage types.
      final StorageType t = storageTypes.get(i);
      if (unavailables.contains(t)) {
        final StorageType fallback = isNewBlock?
            getCreationFallback(unavailables)
            : getReplicationFallback(unavailables);
        if (fallback == null) {
          removed.add(storageTypes.remove(i));
        } else {
          storageTypes.set(i, fallback);
        }
      }
    }
    // remove excess storage types after fallback replacement.
    diff(storageTypes, excess, null);
    if (storageTypes.size() < expectedSize) {
      LOG.warn("Failed to place enough replicas: expected size is {}"
          + " but only {} storage types can be selected (replication={},"
          + " selected={}, unavailable={}" + ", removed={}" + ", policy={}"
          + ")", expectedSize, storageTypes.size(), replication, storageTypes,
          unavailables, removed, this);
    }
    return storageTypes;
  }

  /**
   * Compute the difference between two lists t and c so that after the diff
   * computation we have: t = t - c;
   * Further, if e is not null, set e = e + c - t;
   */
  private static void diff(List<StorageType> t, Iterable<StorageType> c,
      List<StorageType> e) {
    for(StorageType storagetype : c) {
      final int i = t.indexOf(storagetype);
      if (i >= 0) {
        t.remove(i);
      } else if (e != null) {
        e.add(storagetype);
      }
    }
  }

  /**
   * Choose excess storage types for deletion, given the
   * replication number and the storage types of the chosen replicas.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @return a list of {@link StorageType}s for deletion.
   */
  public List<StorageType> chooseExcess(final short replication,
      final Iterable<StorageType> chosen) {
    final List<StorageType> types = chooseStorageTypes(replication);
    final List<StorageType> excess = new LinkedList<>();
    diff(types, chosen, excess);
    return excess;
  }

  /** @return the fallback {@link StorageType} for creation. */
  public StorageType getCreationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, creationFallbacks);
  }

  /** @return the fallback {@link StorageType} for replication. */
  public StorageType getReplicationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, replicationFallbacks);
  }

  @Override
  public int hashCode() {
    return Byte.valueOf(id).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof BlockStoragePolicy)) {
      return false;
    }
    final BlockStoragePolicy that = (BlockStoragePolicy)obj;
    return this.id == that.id;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + name + ":" + id
        + ", storageTypes=" + Arrays.asList(storageTypes)
        + ", creationFallbacks=" + Arrays.asList(creationFallbacks)
        + ", replicationFallbacks=" + Arrays.asList(replicationFallbacks) + "}";
  }

  public byte getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StorageType[] getStorageTypes() {
    return this.storageTypes;
  }

  @Override
  public StorageType[] getCreationFallbacks() {
    return this.creationFallbacks;
  }

  @Override
  public StorageType[] getReplicationFallbacks() {
    return this.replicationFallbacks;
  }

  private static StorageType getFallback(EnumSet<StorageType> unavailables,
      StorageType[] fallbacks) {
    for(StorageType fb : fallbacks) {
      if (!unavailables.contains(fb)) {
        return fb;
      }
    }
    return null;
  }

  @Override
  public boolean isCopyOnCreateFile() {
    return copyOnCreateFile;
  }
}
