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

package org.apache.hadoop.hdfs;

import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * A block storage policy describes how to select the storage types
 * for the replicas of a block.
 */
@InterfaceAudience.Private
public class BlockStoragePolicy {
  public static final Log LOG = LogFactory.getLog(BlockStoragePolicy.class);

  public static final String DFS_BLOCK_STORAGE_POLICIES_KEY
      = "dfs.block.storage.policies";
  public static final String DFS_BLOCK_STORAGE_POLICY_KEY_PREFIX
      = "dfs.block.storage.policy.";
  public static final String DFS_BLOCK_STORAGE_POLICY_CREATION_FALLBACK_KEY_PREFIX
      = "dfs.block.storage.policy.creation-fallback.";
  public static final String DFS_BLOCK_STORAGE_POLICY_REPLICATION_FALLBACK_KEY_PREFIX
      = "dfs.block.storage.policy.replication-fallback.";

  public static final int ID_BIT_LENGTH = 4;
  public static final int ID_MAX = (1 << ID_BIT_LENGTH) - 1;

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

  BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks) {
    this.id = id;
    this.name = name;
    this.storageTypes = storageTypes;
    this.creationFallbacks = creationFallbacks;
    this.replicationFallbacks = replicationFallbacks;
  }

  /**
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  StorageType[] getStoragteTypes(short replication) {
    final StorageType[] types = new StorageType[replication];
    int i = 0;
    for(; i < types.length && i < storageTypes.length; i++) {
      types[i] = storageTypes[i];
    }
    final StorageType last = storageTypes[storageTypes.length - 1];
    for(; i < types.length; i++) {
      types[i] = last;
    }
    return types;
  }

  /** @return the fallback {@link StorageType} for creation. */
  StorageType getCreationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, creationFallbacks);
  }
  
  /** @return the fallback {@link StorageType} for replication. */
  StorageType getReplicationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, replicationFallbacks);
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + name + ":" + id
        + ", storageTypes=" + Arrays.asList(storageTypes)
        + ", creationFallbacks=" + Arrays.asList(creationFallbacks)
        + ", replicationFallbacks=" + Arrays.asList(replicationFallbacks);
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
  
  private static byte parseID(String string) {
    final byte id = Byte.parseByte(string);
    if (id < 1) {
      throw new IllegalArgumentException(
          "Invalid block storage policy ID: id = " + id + " < 1");
    }
    if (id > 15) {
      throw new IllegalArgumentException(
          "Invalid block storage policy ID: id = " + id + " > MAX = " + ID_MAX);
    }
    return id;
  }

  private static StorageType[] parseStorageTypes(String[] strings) {
    if (strings == null) {
      return StorageType.EMPTY_ARRAY;
    }
    final StorageType[] types = new StorageType[strings.length];
    for(int i = 0; i < types.length; i++) {
      types[i] = StorageType.valueOf(strings[i].trim().toUpperCase());
    }
    return types;
  }
  
  private static StorageType[] readStorageTypes(byte id, String keyPrefix,
      Configuration conf) {
    final String[] values = conf.getStrings(keyPrefix + id);
    return parseStorageTypes(values);
  }

  public static BlockStoragePolicy readBlockStoragePolicy(byte id, String name,
      Configuration conf) {
    final StorageType[] storageTypes = readStorageTypes(id, 
        DFS_BLOCK_STORAGE_POLICY_KEY_PREFIX, conf);
    final StorageType[] creationFallbacks = readStorageTypes(id, 
        DFS_BLOCK_STORAGE_POLICY_CREATION_FALLBACK_KEY_PREFIX, conf);
    final StorageType[] replicationFallbacks = readStorageTypes(id, 
        DFS_BLOCK_STORAGE_POLICY_REPLICATION_FALLBACK_KEY_PREFIX, conf);
    return new BlockStoragePolicy(id, name, storageTypes, creationFallbacks,
        replicationFallbacks);
  }

  public static BlockStoragePolicy[] readBlockStoragePolicies(
      Configuration conf) {
    final BlockStoragePolicy[] policies = new BlockStoragePolicy[ID_MAX + 1];
    final String[] values = conf.getStrings(DFS_BLOCK_STORAGE_POLICIES_KEY);
    for(String v : values) {
      v = v.trim();
      final int i = v.indexOf(':');
      final String name = v.substring(0, i);
      final byte id = parseID(v.substring(i + 1));
      if (policies[id] != null) {
        throw new IllegalArgumentException(
            "Policy duplication: ID " + id + " appears more than once in "
            + DFS_BLOCK_STORAGE_POLICIES_KEY);
      }
      policies[id] = readBlockStoragePolicy(id, name, conf);
      LOG.info(policies[id]);
    }
    return policies;
  }
}