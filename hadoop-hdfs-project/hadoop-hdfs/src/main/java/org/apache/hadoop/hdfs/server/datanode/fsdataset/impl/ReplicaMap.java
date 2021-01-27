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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.util.FoldedTreeSet;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;

/**
 * Maintains the mapping from volumes to {@link VolumeReplicaMap}s.
 * All replica objects are maintained in {@link VolumeReplicaMap}s. This class
 * serves as a container for different {@link VolumeReplicaMap}, one per
 * volume of the Datanode.
 */
class ReplicaMap {
  // Lock object to synchronize this instance.
  private final AutoCloseableLock readLock;
  private final AutoCloseableLock writeLock;
  
  // Map of block pool Id to a set of ReplicaInfo.
  private final Map<String, FoldedTreeSet<ReplicaInfo>> map = new HashMap<>();

  // Special comparator used to compare Long to Block ID in the TreeSet.
  private static final Comparator<Object> LONG_AND_BLOCK_COMPARATOR
      = new Comparator<Object>() {
    @Override
    public int compare(Object o1, Object o2) {
      long lookup = (long) o1;
      long stored = ((Block) o2).getBlockId();
      return lookup > stored ? 1 : lookup < stored ? -1 : 0;
    }
  };

  protected Map<FsVolumeImpl, VolumeReplicaMap> innerReplicaMaps;

  ReplicaMap(AutoCloseableLock readLock, AutoCloseableLock writeLock) {
    if (readLock == null || writeLock == null) {
      throw new HadoopIllegalArgumentException(
          "Lock to synchronize on cannot be null");
    }
    innerReplicaMaps = new HashMap<>();
    this.readLock = readLock;
    this.writeLock = writeLock;
  }

  ReplicaMap(ReadWriteLock lock) {
    this(new AutoCloseableLock(lock.readLock()),
        new AutoCloseableLock(lock.writeLock()));
  }

  String[] getBlockPoolList() {
    try (AutoCloseableLock l = readLock.acquire()) {
      HashSet<String> blockPoolList = new HashSet<>();
      for (VolumeReplicaMap replicaMap : innerReplicaMaps.values()) {
        blockPoolList.addAll(Arrays.asList(replicaMap.getBlockPoolList()));
      }
      return blockPoolList.toArray(new String[blockPoolList.size()]);
    }
  }
  
  protected void checkBlockPool(String bpid) {
    if (bpid == null) {
      throw new IllegalArgumentException("Block Pool Id is null");
    }
  }

  protected void checkBlock(Block b) {
    if (b == null) {
      throw new IllegalArgumentException("Block is null");
    }
  }
  
  /**
   * Get the meta information of the replica that matches both block id 
   * and generation stamp
   * @param bpid block pool id
   * @param block block with its id as the key
   * @return the replica's meta information
   * @throws IllegalArgumentException if the input block or block pool is null
   */
  ReplicaInfo get(String bpid, Block block) {
    checkBlockPool(bpid);
    checkBlock(block);
    try (AutoCloseableLock l = readLock.acquire()) {
      ReplicaInfo replicaInfo = get(bpid, block.getBlockId());
      if (replicaInfo != null &&
          block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
        return replicaInfo;
      }
    }
    return null;
  }
  

  /**
   * Get the meta information of the replica that matches the block id
   * @param bpid block pool id
   * @param blockId a block's id
   * @return the replica's meta information
   */
  ReplicaInfo get(String bpid, long blockId) {
    checkBlockPool(bpid);
    try (AutoCloseableLock l = readLock.acquire()) {
      // check inner-maps; each of them will have their own synchronization.
      for (VolumeReplicaMap inner : innerReplicaMaps.values()) {
        ReplicaInfo info = inner.get(bpid, blockId);
        if (info != null) {
          return info;
        }
      }
    }
    return null;
  }

  /**
   * Add a replica's meta information into the map 
   * 
   * @param bpid block pool id
   * @param replicaInfo a replica's meta information
   * @return previous meta information of the replica
   * @throws IllegalArgumentException if the input parameter is null
   */
  ReplicaInfo add(String bpid, ReplicaInfo replicaInfo) {
    checkBlockPool(bpid);
    checkBlock(replicaInfo);
    try (AutoCloseableLock l = writeLock.acquire()) {
      // check if the replica already exists
      ReplicaInfo existingReplica = get(bpid, replicaInfo.getBlockId());
      if (existingReplica != null) {
        // the replica being added is for a different volume; remove it.
        if (!existingReplica.getVolume().equals(replicaInfo.getVolume())) {
          remove(bpid, replicaInfo.getBlockId());
        }
      }
      VolumeReplicaMap innerMap =
          innerReplicaMaps.get(replicaInfo.getVolume());
      if (innerMap == null) {
        innerMap = new VolumeReplicaMap(new AutoCloseableLock());
        innerReplicaMaps.put((FsVolumeImpl) replicaInfo.getVolume(), innerMap);
      }
      return innerMap.add(bpid, replicaInfo);
    }
  }

  @VisibleForTesting
  void addAll(ReplicaMap other) {
    try (AutoCloseableLock l = writeLock.acquire()) {
      for (FsVolumeImpl volume : other.innerReplicaMaps.keySet()) {
        if (innerReplicaMaps.containsKey(volume)) {
          innerReplicaMaps.get(volume).addAll(
              other.innerReplicaMaps.get(volume));
        } else {
          VolumeReplicaMap innerMap = new VolumeReplicaMap(
              new AutoCloseableLock());
          innerMap.addAll(other.innerReplicaMaps.get(volume));
          innerReplicaMaps.put(volume, innerMap);
        }
      }
    }
  }

  /**
   * Add a replica's meta information into the map, if already exist
   * return the old replicaInfo.
   */
  ReplicaInfo addAndGet(String bpid, ReplicaInfo replicaInfo) {
    checkBlockPool(bpid);
    checkBlock(replicaInfo);
    try (AutoCloseableLock l = writeLock.acquire()) {
      FsVolumeSpi volume = replicaInfo.getVolume();
      VolumeReplicaMap innerMap = innerReplicaMaps.get(volume);
      if (innerMap == null) {
        innerMap = new VolumeReplicaMap(new AutoCloseableLock());
        innerReplicaMaps.put((FsVolumeImpl) volume, innerMap);
      }

      ReplicaInfo oldReplicaInfo =
          innerMap.get(bpid, replicaInfo.getBlockId());
      if (oldReplicaInfo != null) {
        return oldReplicaInfo;
      } else {
        innerMap.add(bpid, replicaInfo);
      }
      return replicaInfo;
    }
  }

  /**
   * Add all entries from the given replica map into the local replica map.
   */
  void addAll(FsVolumeImpl volume, VolumeReplicaMap other) {
    if (volume != null && other != null) {
      try (AutoCloseableLock l = writeLock.acquire()) {
        if (innerReplicaMaps.containsKey(volume)) {
          innerReplicaMaps.get(volume).addAll(other);
        } else {
          innerReplicaMaps.put(volume, other);
        }
      }
    }
  }

  /**
   * Merge all entries from the given replica map into the local replica map.
   */
  void mergeAll(ReplicaMap other) {
    try (AutoCloseableLock l = writeLock.acquire()) {
      for (FsVolumeImpl volume : other.innerReplicaMaps.keySet()) {
        this.mergeAll(volume, other.innerReplicaMaps.get(volume));
      }
    }
  }

  /**
   * Merge all entries from the given volume replica map into the local replica
   * map.
   */
  void mergeAll(FsVolumeImpl volume, VolumeReplicaMap other) {
    if (volume != null && other != null) {
      try (AutoCloseableLock l = writeLock.acquire()) {
        if (innerReplicaMaps.containsKey(volume)) {
          //AA-TODO is addAll below valid
          innerReplicaMaps.get(volume).addAll(other);
        } else {
          innerReplicaMaps.put(volume, other);
        }
      }
    }
  }
  
  /**
   * Remove the replica's meta information from the map that matches
   * the input block's id and generation stamp
   * @param bpid block pool id
   * @param block block with its id as the key
   * @return the removed replica's meta information
   * @throws IllegalArgumentException if the input block is null
   */
  ReplicaInfo remove(String bpid, Block block) {
    checkBlockPool(bpid);
    checkBlock(block);
    // check the inner maps.
    return removeFromInnerMaps(bpid, block);
  }

  private ReplicaInfo removeFromInnerMaps(String bpid, Block block) {
    try (AutoCloseableLock l = writeLock.acquire()) {
      for (VolumeReplicaMap inner : innerReplicaMaps.values()) {
        ReplicaInfo info = inner.remove(bpid, block);
        if (info != null) {
          return info;
        }
      }
    }
    return null;
  }

  /**
   * Remove the replica's meta information from the map if present
   * @param bpid block pool id
   * @param blockId block id of the replica to be removed
   * @return the removed replica's meta information
   */
  ReplicaInfo remove(String bpid, long blockId) {
    checkBlockPool(bpid);
    return removeFromInnerMaps(bpid, blockId);
  }

  private ReplicaInfo removeFromInnerMaps(String bpid, long blockId) {
    try (AutoCloseableLock l = writeLock.acquire()) {
      for (VolumeReplicaMap inner : innerReplicaMaps.values()) {
        ReplicaInfo info = inner.remove(bpid, blockId);
        if (info != null) {
          return info;
        }
      }
    }
    return null;
  }

  /**
   * Get the size of the map for given block pool
   * @param bpid block pool id
   * @return the number of replicas in the map
   */
  int size(String bpid) {
    int numReplicas = 0;
    try (AutoCloseableLock l = readLock.acquire()) {
      for (VolumeReplicaMap inner : innerReplicaMaps.values()) {
        numReplicas += inner.size(bpid);
      }
    }
    return numReplicas;
  }
  
  /**
   * Get a collection of the replicas for given block pool
   * This method is <b>not synchronized</b>. It needs to be synchronized
   * externally using the lock, both for getting the replicas
   * values from the map and iterating over it. Mutex can be accessed using
   * {@link #getLock()} method.
   * 
   * @param bpid block pool id
   * @return a collection of the replicas belonging to the block pool
   */
  Collection<ReplicaInfo> replicas(String bpid) {
    try (AutoCloseableLock l = readLock.acquire()) {
      Collection<ReplicaInfo> allReplicas = new ArrayList<>();
      for (VolumeReplicaMap inner : innerReplicaMaps.values()) {
        Collection<ReplicaInfo> replicas = inner.replicas(bpid);
        if (replicas != null) {
          allReplicas.addAll(replicas);
        }
      }
      return allReplicas;
    }
  }

  void initBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseableLock l =  writeLock.acquire()) {
      for (VolumeReplicaMap inner : innerReplicaMaps.values()) {
        inner.initBlockPool(bpid);
      }
    }
  }
  
  void cleanUpBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseableLock l =  writeLock.acquire()) {
      for (VolumeReplicaMap inner : innerReplicaMaps.values()) {
        inner.cleanUpBlockPool(bpid);
      }
    }
  }
  
  /**
   * Get the lock object used for synchronizing ReplicasMap
   * @return lock object
   */
  AutoCloseableLock getLock() {
    return writeLock;
  }

  /**
   * Get the lock object used for synchronizing the ReplicasMap for read only
   * operations.
   * @return The read lock object
   */
  AutoCloseableLock getReadLock() {
    return readLock;
  }

  public VolumeReplicaMap get(FsVolumeImpl vol) {
    try (AutoCloseableLock l = readLock.acquire()) {
      return innerReplicaMaps.get(vol);
    }
  }

  /**
   * Remove all replicas that belong for a particular volume.
   * @param sdLocation the Storage location of the volume to remove
   *                   replicas from.
   */
  public void removeAll(StorageLocation sdLocation) {
    try (AutoCloseableLock l = writeLock.acquire()) {
      for (FsVolumeImpl volume : innerReplicaMaps.keySet()) {
        if (volume.getStorageLocation().equals(sdLocation)) {
          innerReplicaMaps.remove(volume);
          return;
        }
      }
    }
  }
}
