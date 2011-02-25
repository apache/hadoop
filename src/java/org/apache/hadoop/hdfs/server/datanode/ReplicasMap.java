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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;

class ReplicasMap {
  // Map of block pool Id to another map of block Id to ReplicaInfo.
  private Map<String, Map<Long, ReplicaInfo>> map = 
    new HashMap<String, Map<Long, ReplicaInfo>>();
  
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
    Map<Long, ReplicaInfo> m = map.get(bpid);
    return m != null ? m.get(blockId) : null;
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
    Map<Long, ReplicaInfo> m = map.get(bpid);
    if (m == null) {
      // Add an entry for block pool if it does not exist already
      m = new HashMap<Long, ReplicaInfo>();
      map.put(bpid, m);
    }
    return  m.put(replicaInfo.getBlockId(), replicaInfo);
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
    Map<Long, ReplicaInfo> m = map.get(bpid);
    if (m != null) {
      Long key = Long.valueOf(block.getBlockId());
      ReplicaInfo replicaInfo = m.get(key);
      if (replicaInfo != null &&
          block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
        return m.remove(key);
      } 
    }
    
    return null;
  }
  
  /**
   * Remove the replica's meta information from the map if present
   * @param bpid block pool id
   * @param the block id of the replica to be removed
   * @return the removed replica's meta information
   */
  ReplicaInfo remove(String bpid, long blockId) {
    checkBlockPool(bpid);
    Map<Long, ReplicaInfo> m = map.get(bpid);
    if (m != null) {
      return m.remove(blockId);
    }
    return null;
  }
 
  /**
   * Get the size of the map for given block pool
   * @param bpid block pool id
   * @return the number of replicas in the map
   */
  int size(String bpid) {
    Map<Long, ReplicaInfo> m = map.get(bpid);
    return m != null ? m.size() : 0;
  }
  
  /**
   * Get a collection of the replicas for given block pool
   * @param bpid block pool id
   * @return a collection of the replicas
   */
  Collection<ReplicaInfo> replicas(String bpid) {
    Map<Long, ReplicaInfo> m = map.get(bpid);
    return m != null ? m.values() : null;
  }
}