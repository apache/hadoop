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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.common.NoLockManager;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.util.LightWeightResizableGSet;

/**
 * Maintains the replica map. 
 */
class ReplicaMap {
  // Lock object to synchronize this instance.
  private DataNodeLockManager<AutoCloseDataSetLock> lockManager;

  // Map of block pool Id to another map of block Id to ReplicaInfo.
  private final Map<String, LightWeightResizableGSet<Block, ReplicaInfo>> map =
      new ConcurrentHashMap<>();

  ReplicaMap(DataNodeLockManager<AutoCloseDataSetLock> manager) {
    if (manager == null) {
      throw new HadoopIllegalArgumentException(
          "Object to synchronize on cannot be null");
    }
    this.lockManager = manager;
  }

  // Used for ut or temp replicaMap that no need to protected by lock.
  ReplicaMap() {
    this.lockManager = new NoLockManager();
  }
  
  String[] getBlockPoolList() {
    Set<String> bpset = map.keySet();
    return bpset.toArray(new String[bpset.size()]);
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
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      return m != null ? m.get(new Block(blockId)) : null;
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
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // Add an entry for block pool if it does not exist already
        map.putIfAbsent(bpid, new LightWeightResizableGSet<Block, ReplicaInfo>());
        m = map.get(bpid);
      }
      return  m.put(replicaInfo);
    }
  }

  /**
   * Add a replica's meta information into the map, if already exist
   * return the old replicaInfo.
   */
  ReplicaInfo addAndGet(String bpid, ReplicaInfo replicaInfo) {
    checkBlockPool(bpid);
    checkBlock(replicaInfo);
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // Add an entry for block pool if it does not exist already
        map.putIfAbsent(bpid, new LightWeightResizableGSet<Block, ReplicaInfo>());
        m = map.get(bpid);
      }
      ReplicaInfo oldReplicaInfo = m.get(replicaInfo);
      if (oldReplicaInfo != null) {
        return oldReplicaInfo;
      } else {
        m.put(replicaInfo);
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
    Set<String> bplist = other.map.keySet();
    for (String bp : bplist) {
      checkBlockPool(bp);
      try (AutoCloseDataSetLock l = lockManager.writeLock(LockLevel.BLOCK_POOl, bp)) {
        LightWeightResizableGSet<Block, ReplicaInfo> replicaInfos = other.map.get(bp);
        LightWeightResizableGSet<Block, ReplicaInfo> curSet = map.get(bp);
        HashSet<ReplicaInfo> replicaSet = new HashSet<>();
        //Can't add to GSet while in another GSet iterator may cause endlessLoop
        for (ReplicaInfo replicaInfo : replicaInfos) {
          replicaSet.add(replicaInfo);
        }
        if (curSet == null && !replicaSet.isEmpty()) {
          // Add an entry for block pool if it does not exist already
          curSet = new LightWeightResizableGSet<>();
          map.put(bp, curSet);
        }
        for (ReplicaInfo replicaInfo : replicaSet) {
          checkBlock(replicaInfo);
          curSet.put(replicaInfo);
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
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m != null) {
        ReplicaInfo replicaInfo = m.get(block);
        if (replicaInfo != null &&
            block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
          return m.remove(block);
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
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m != null) {
        return m.remove(new Block(blockId));
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
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      return m != null ? m.size() : 0;
    }
  }
  
  /**
   * Get a collection of the replicas for given block pool
   * This method is <b>not synchronized</b>. If you want to keep thread safe
   * Use method {@link #replicas(String, Consumer<Iterator<ReplicaInfo>>)}.
   *
   * @param bpid block pool id
   * @return a collection of the replicas belonging to the block pool
   */
  Collection<ReplicaInfo> replicas(String bpid) {
    LightWeightResizableGSet<Block, ReplicaInfo> m = null;
    m = map.get(bpid);
    return m != null ? m.values() : null;
  }

  /**
   * execute function for one block pool and protect by LockManager.
   * This method is <b>synchronized</b>.
   *
   * @param bpid block pool id
   */
  void replicas(String bpid, Consumer<Iterator<ReplicaInfo>> consumer) {
    LightWeightResizableGSet<Block, ReplicaInfo> m = null;
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      m = map.get(bpid);
      if (m !=null) {
        m.getIterator(consumer);
      }
    }
  }

  void initBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseDataSetLock l = lockManager.writeLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // Add an entry for block pool if it does not exist already
        m = new LightWeightResizableGSet<Block, ReplicaInfo>();
        map.put(bpid, m);
      }
    }
  }
  
  void cleanUpBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseDataSetLock l = lockManager.writeLock(LockLevel.BLOCK_POOl, bpid)) {
      map.remove(bpid);
    }
  }
}
