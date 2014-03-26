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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;

/**
 * Maintains the replica map. 
 */
class ReplicaMap {
  // Object using which this class is synchronized
  private final Object mutex;
  
  // Map of block pool Id to another map of block Id to ReplicaInfo.
  private final Map<String, Map<Long, ReplicaInfo>> map =
    new HashMap<String, Map<Long, ReplicaInfo>>();
  
  ReplicaMap(Object mutex) {
    if (mutex == null) {
      throw new HadoopIllegalArgumentException(
          "Object to synchronize on cannot be null");
    }
    this.mutex = mutex;
  }
  
  String[] getBlockPoolList() {
    synchronized(mutex) {
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
    synchronized(mutex) {
      Map<Long, ReplicaInfo> m = map.get(bpid);
      return m != null ? m.get(blockId) : null;
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
    synchronized(mutex) {
      Map<Long, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // Add an entry for block pool if it does not exist already
        m = new HashMap<Long, ReplicaInfo>();
        map.put(bpid, m);
      }
      return  m.put(replicaInfo.getBlockId(), replicaInfo);
    }
  }

  /**
   * Add all entries from the given replica map into the local replica map.
   */
  void addAll(ReplicaMap other) {
    map.putAll(other.map);
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
    synchronized(mutex) {
      Map<Long, ReplicaInfo> m = map.get(bpid);
      if (m != null) {
        Long key = Long.valueOf(block.getBlockId());
        ReplicaInfo replicaInfo = m.get(key);
        if (replicaInfo != null &&
            block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
          return m.remove(key);
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
    synchronized(mutex) {
      Map<Long, ReplicaInfo> m = map.get(bpid);
      if (m != null) {
        return m.remove(blockId);
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
    Map<Long, ReplicaInfo> m = null;
    synchronized(mutex) {
      m = map.get(bpid);
      return m != null ? m.size() : 0;
    }
  }
  
  /**
   * Get a collection of the replicas for given block pool
   * This method is <b>not synchronized</b>. It needs to be synchronized
   * externally using the mutex, both for getting the replicas
   * values from the map and iterating over it. Mutex can be accessed using
   * {@link #getMutext()} method.
   * 
   * @param bpid block pool id
   * @return a collection of the replicas belonging to the block pool
   */
  Collection<ReplicaInfo> replicas(String bpid) {
    Map<Long, ReplicaInfo> m = null;
    m = map.get(bpid);
    return m != null ? m.values() : null;
  }

  void initBlockPool(String bpid) {
    checkBlockPool(bpid);
    synchronized(mutex) {
      Map<Long, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // Add an entry for block pool if it does not exist already
        m = new HashMap<Long, ReplicaInfo>();
        map.put(bpid, m);
      }
    }
  }
  
  void cleanUpBlockPool(String bpid) {
    checkBlockPool(bpid);
    synchronized(mutex) {
      map.remove(bpid);
    }
  }
  
  /**
   * Give access to mutex used for synchronizing ReplicasMap
   * @return object used as lock
   */
  Object getMutext() {
    return mutex;
  }
}
