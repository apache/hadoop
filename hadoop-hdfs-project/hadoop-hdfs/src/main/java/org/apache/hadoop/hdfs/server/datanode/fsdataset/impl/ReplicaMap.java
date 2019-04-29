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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.util.FoldedTreeSet;
import org.apache.hadoop.util.AutoCloseableLock;

/**
 * Maintains the replica map. 
 */
class ReplicaMap {
  // Lock object to synchronize this instance.
  private final AutoCloseableLock lock;
  
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

  ReplicaMap(AutoCloseableLock lock) {
    if (lock == null) {
      throw new HadoopIllegalArgumentException(
          "Lock to synchronize on cannot be null");
    }
    this.lock = lock;
  }
  
  String[] getBlockPoolList() {
    try (AutoCloseableLock l = lock.acquire()) {
      return map.keySet().toArray(new String[map.keySet().size()]);   
    }
  }
  
  private void checkBlockPool(String bpid) {
    if (bpid == null) {
      throw new IllegalArgumentException("Block Pool Id is null");
    }
  }
  
  private void checkBlock(Block b) {
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
    ReplicaInfo replicaInfo = get(bpid, block.getBlockId());
    if (replicaInfo != null && 
        block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
      return replicaInfo;
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
    try (AutoCloseableLock l = lock.acquire()) {
      FoldedTreeSet<ReplicaInfo> set = map.get(bpid);
      if (set == null) {
        return null;
      }
      return set.get(blockId, LONG_AND_BLOCK_COMPARATOR);
    }
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
    try (AutoCloseableLock l = lock.acquire()) {
      FoldedTreeSet<ReplicaInfo> set = map.get(bpid);
      if (set == null) {
        // Add an entry for block pool if it does not exist already
        set = new FoldedTreeSet<>();
        map.put(bpid, set);
      }
      return set.addOrReplace(replicaInfo);
    }
  }

  /**
   * Add a replica's meta information into the map, if already exist
   * return the old replicaInfo.
   */
  ReplicaInfo addAndGet(String bpid, ReplicaInfo replicaInfo) {
    checkBlockPool(bpid);
    checkBlock(replicaInfo);
    try (AutoCloseableLock l = lock.acquire()) {
      FoldedTreeSet<ReplicaInfo> set = map.get(bpid);
      if (set == null) {
        // Add an entry for block pool if it does not exist already
        set = new FoldedTreeSet<>();
        map.put(bpid, set);
      }
      ReplicaInfo oldReplicaInfo = set.get(replicaInfo.getBlockId(),
          LONG_AND_BLOCK_COMPARATOR);
      if (oldReplicaInfo != null) {
        return oldReplicaInfo;
      } else {
        set.addOrReplace(replicaInfo);
      }
      return replicaInfo;
    }
  }

  /**
   * Add all entries from the given replica map into the local replica map.
   */
  void addAll(ReplicaMap other) {
    map.putAll(other.map);
  }


  /**
   * Merge all entries from the given replica map into the local replica map.
   */
  void mergeAll(ReplicaMap other) {
    other.map.forEach(
        (bp, replicaInfos) -> {
          replicaInfos.forEach(
              replicaInfo -> add(bp, replicaInfo)
          );
        }
    );
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
    try (AutoCloseableLock l = lock.acquire()) {
      FoldedTreeSet<ReplicaInfo> set = map.get(bpid);
      if (set != null) {
        ReplicaInfo replicaInfo =
            set.get(block.getBlockId(), LONG_AND_BLOCK_COMPARATOR);
        if (replicaInfo != null &&
            block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
          return set.removeAndGet(replicaInfo);
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
    try (AutoCloseableLock l = lock.acquire()) {
      FoldedTreeSet<ReplicaInfo> set = map.get(bpid);
      if (set != null) {
        return set.removeAndGet(blockId, LONG_AND_BLOCK_COMPARATOR);
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
    try (AutoCloseableLock l = lock.acquire()) {
      FoldedTreeSet<ReplicaInfo> set = map.get(bpid);
      return set != null ? set.size() : 0;
    }
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
    return map.get(bpid);
  }

  void initBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseableLock l = lock.acquire()) {
      FoldedTreeSet<ReplicaInfo> set = map.get(bpid);
      if (set == null) {
        // Add an entry for block pool if it does not exist already
        set = new FoldedTreeSet<>();
        map.put(bpid, set);
      }
    }
  }
  
  void cleanUpBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseableLock l = lock.acquire()) {
      map.remove(bpid);
    }
  }
  
  /**
   * Get the lock object used for synchronizing ReplicasMap
   * @return lock object
   */
  AutoCloseableLock getLock() {
    return lock;
  }
}
