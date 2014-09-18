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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttr.NameSpace;

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
  public static final String STORAGE_POLICY_XATTR_NAME = "bsp";
  /** set the namespace to TRUSTED so that only privilege users can access */
  public static final NameSpace XAttrNS = NameSpace.TRUSTED;

  public static final int ID_BIT_LENGTH = 4;
  public static final int ID_MAX = (1 << ID_BIT_LENGTH) - 1;
  public static final byte ID_UNSPECIFIED = 0;

  private static final Suite DEFAULT_SUITE = createDefaultSuite();

  private static Suite createDefaultSuite() {
    final BlockStoragePolicy[] policies = new BlockStoragePolicy[1 << ID_BIT_LENGTH];
    final StorageType[] storageTypes = {StorageType.DISK};
    final byte defaultPolicyId = 12;
    policies[defaultPolicyId] = new BlockStoragePolicy(defaultPolicyId, "HOT",
        storageTypes, StorageType.EMPTY_ARRAY, StorageType.EMPTY_ARRAY);
    return new Suite(defaultPolicyId, policies);
  }

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

    public BlockStoragePolicy getPolicy(String policyName) {
      if (policies != null) {
        for (BlockStoragePolicy policy : policies) {
          if (policy != null && policy.name.equals(policyName)) {
            return policy;
          }
        }
      }
      return null;
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

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
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
   * the unavailable storage types.  It uses fallback storage in case that
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
    final List<StorageType> excess = new LinkedList<StorageType>();
    final List<StorageType> storageTypes = chooseStorageTypes(
        replication, chosen, excess);
    final int expectedSize = storageTypes.size() - excess.size();
    final List<StorageType> removed = new LinkedList<StorageType>();
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
      LOG.warn("Failed to place enough replicas: expected size is " + expectedSize 
          + " but only " + storageTypes.size() + " storage types can be selected "
          + "(replication=" + replication
          + ", selected=" + storageTypes
          + ", unavailable=" + unavailables
          + ", removed=" + removed
          + ", policy=" + this + ")");
    }
    return storageTypes;
  }

  /**
   * Compute the list difference t = t - c.
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
    final List<StorageType> excess = new LinkedList<StorageType>();
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
        + ", replicationFallbacks=" + Arrays.asList(replicationFallbacks);
  }

  public byte getId() {
    return id;
  }

  public String getName() {
    return name;
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
    byte id = 0;
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
    if (values == null) {
      // conf property is missing, use default suite.
      return DEFAULT_SUITE;
    }
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

  public static String buildXAttrName() {
    return XAttrNS.toString().toLowerCase() + "." + STORAGE_POLICY_XATTR_NAME;
  }

  public static XAttr buildXAttr(byte policyId) {
    final String name = buildXAttrName();
    return XAttrHelper.buildXAttr(name, new byte[] { policyId });
  }

  public static boolean isStoragePolicyXAttr(XAttr xattr) {
    return xattr != null && xattr.getNameSpace() == BlockStoragePolicy.XAttrNS
        && xattr.getName().equals(BlockStoragePolicy.STORAGE_POLICY_XATTR_NAME);
  }

  private static void throwIllegalArgumentException(String message,
      Configuration conf) {
    throw new IllegalArgumentException(message + " in "
        + DFS_BLOCK_STORAGE_POLICIES_KEY + " \""
        + conf.get(DFS_BLOCK_STORAGE_POLICIES_KEY) + "\".");
  }
}