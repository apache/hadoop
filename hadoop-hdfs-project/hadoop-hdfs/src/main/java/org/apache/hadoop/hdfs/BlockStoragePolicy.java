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
import java.util.LinkedList;
import java.util.List;

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

  /** A block storage policy suite. */
  public static class Suite {
    private final byte defaultPolicyID;
    private final BlockStoragePolicy[] policies;
    
    private Suite(byte defaultPolicyID, BlockStoragePolicy[] policies) {
      this.defaultPolicyID = defaultPolicyID;
      this.policies = policies;
    }
    
    /** @return the corresponding policy. */
    public BlockStoragePolicy getPolicy(byte id) {
      // id == 0 means policy not specified. 
      return id == 0? getDefaultPolicy(): policies[id];
    }

    /** @return the default policy. */
    public BlockStoragePolicy getDefaultPolicy() {
      return getPolicy(defaultPolicyID);
    }
  }

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
  public List<StorageType> chooseStorageTypes(final short replication) {
    final List<StorageType> types = new LinkedList<StorageType>();
    int i = 0;
    for(; i < replication && i < storageTypes.length; i++) {
      types.add(storageTypes[i]);
    }
    final StorageType last = storageTypes[storageTypes.length - 1];
    for(; i < replication; i++) {
      types.add(last);
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
    final List<StorageType> types = chooseStorageTypes(replication);

    //remove the chosen storage types
    for(StorageType c : chosen) {
      final int i = types.indexOf(c);
      if (i >= 0) {
        types.remove(i);
      }
    }
    return types;
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
  
  private static byte parseID(String idString, String element, Configuration conf) {
    Byte id = null;
    try {
      id = Byte.parseByte(idString);
    } catch(NumberFormatException nfe) {
      throwIllegalArgumentException("Failed to parse policy ID \"" + idString
          + "\" to a " + ID_BIT_LENGTH + "-bit integer", conf);
    }
    if (id < 0) {
      throwIllegalArgumentException("Invalid policy ID: id = " + id
          + " < 1 in \"" + element + "\"", conf);
    } else if (id == 0) {
      throw new IllegalArgumentException("Policy ID 0 is reserved: " + element);
    } else if (id > ID_MAX) {
      throwIllegalArgumentException("Invalid policy ID: id = " + id
          + " > MAX = " + ID_MAX + " in \"" + element + "\"", conf);
    }
    return id;
  }

  private static StorageType[] parseStorageTypes(String[] strings) {
    if (strings == null || strings.length == 0) {
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
    final String key = keyPrefix + id;
    final String[] values = conf.getStrings(key);
    try {
      return parseStorageTypes(values);
    } catch(Exception e) {
      throw new IllegalArgumentException("Failed to parse " + key
          + " \"" + conf.get(key), e);
    }
  }

  private static BlockStoragePolicy readBlockStoragePolicy(byte id, String name,
      Configuration conf) {
    final StorageType[] storageTypes = readStorageTypes(id, 
        DFS_BLOCK_STORAGE_POLICY_KEY_PREFIX, conf);
    if (storageTypes.length == 0) {
      throw new IllegalArgumentException(
          DFS_BLOCK_STORAGE_POLICY_KEY_PREFIX + id + " is missing or is empty.");
    }
    final StorageType[] creationFallbacks = readStorageTypes(id, 
        DFS_BLOCK_STORAGE_POLICY_CREATION_FALLBACK_KEY_PREFIX, conf);
    final StorageType[] replicationFallbacks = readStorageTypes(id, 
        DFS_BLOCK_STORAGE_POLICY_REPLICATION_FALLBACK_KEY_PREFIX, conf);
    return new BlockStoragePolicy(id, name, storageTypes, creationFallbacks,
        replicationFallbacks);
  }

  /** Read {@link Suite} from conf. */
  public static Suite readBlockStorageSuite(Configuration conf) {
    final BlockStoragePolicy[] policies = new BlockStoragePolicy[1 << ID_BIT_LENGTH];
    final String[] values = conf.getStrings(DFS_BLOCK_STORAGE_POLICIES_KEY);
    byte firstID = -1;
    for(String v : values) {
      v = v.trim();
      final int i = v.indexOf(':');
      if (i < 0) {
        throwIllegalArgumentException("Failed to parse element \"" + v
            + "\" (expected format is NAME:ID)", conf);
      } else if (i == 0) {
        throwIllegalArgumentException("Policy name is missing in \"" + v + "\"", conf);
      } else if (i == v.length() - 1) {
        throwIllegalArgumentException("Policy ID is missing in \"" + v + "\"", conf);
      }
      final String name = v.substring(0, i).trim();
      for(int j = 1; j < policies.length; j++) {
        if (policies[j] != null && policies[j].name.equals(name)) {
          throwIllegalArgumentException("Policy name duplication: \""
              + name + "\" appears more than once", conf);
        }
      }
      
      final byte id = parseID(v.substring(i + 1).trim(), v, conf);
      if (policies[id] != null) {
        throwIllegalArgumentException("Policy duplication: ID " + id
            + " appears more than once", conf);
      }
      policies[id] = readBlockStoragePolicy(id, name, conf);
      String prefix = "";
      if (firstID == -1) {
        firstID = id;
        prefix = "(default) ";
      }
      LOG.info(prefix + policies[id]);
    }
    if (firstID == -1) {
      throwIllegalArgumentException("Empty list is not allowed", conf);
    }
    return new Suite(firstID, policies);
  }

  private static void throwIllegalArgumentException(String message,
      Configuration conf) {
    throw new IllegalArgumentException(message + " in "
        + DFS_BLOCK_STORAGE_POLICIES_KEY + " \""
        + conf.get(DFS_BLOCK_STORAGE_POLICIES_KEY) + "\".");
 }
}