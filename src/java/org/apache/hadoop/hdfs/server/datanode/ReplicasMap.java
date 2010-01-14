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

import org.apache.hadoop.hdfs.protocol.Block;

class ReplicasMap {
  // HashMap: maps a block id to the replica's meta info
  private HashMap<Long, ReplicaInfo> map = new HashMap<Long, ReplicaInfo>();
  /**
   * Get the meta information of the replica that matches both block id 
   * and generation stamp
   * @param block block with its id as the key
   * @return the replica's meta information
   * @throws IllegalArgumentException if the input block is null
   */
  ReplicaInfo get(Block block) {
    if (block == null) {
      throw new IllegalArgumentException("Do not expect null block");
    }
    ReplicaInfo replicaInfo = get(block.getBlockId());
    if (replicaInfo != null && 
        block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
      return replicaInfo;
    }
    return null;
  }
  
  /**
   * Get the meta information of the replica that matches the block id
   * @param blockId a block's id
   * @return the replica's meta information
   */
  ReplicaInfo get(long blockId) {
    return map.get(blockId);
  }
  
  /**
   * Add a replica's meta information into the map 
   * 
   * @param replicaInfo a replica's meta information
   * @return previous meta information of the replica
   * @throws IllegalArgumentException if the input parameter is null
   */
  ReplicaInfo add(ReplicaInfo replicaInfo) {
    if (replicaInfo == null) {
      throw new IllegalArgumentException("Do not expect null block");
    }
    return  map.put(replicaInfo.getBlockId(), replicaInfo);
  }
  
  /**
   * Remove the replica's meta information from the map that matches
   * the input block's id and generation stamp
   * @param block block with its id as the key
   * @return the removed replica's meta information
   * @throws IllegalArgumentException if the input block is null
   */
  ReplicaInfo remove(Block block) {
    if (block == null) {
      throw new IllegalArgumentException("Do not expect null block");
    }
    Long key = Long.valueOf(block.getBlockId());
    ReplicaInfo replicaInfo = map.get(key);
    if (replicaInfo != null &&
        block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
      return remove(key);
    } 
    
    return null;
  }
  
  /**
   * Remove the replica's meta information from the map if present
   * @param the block id of the replica to be removed
   * @return the removed replica's meta information
   */
  ReplicaInfo remove(long blockId) {
    return map.remove(blockId);
  }
 
  /**
   * Get the size of the map
   * @return the number of replicas in the map
   */
  int size() {
    return map.size();
  }
  
  /**
   * Get a collection of the replicas
   * @return a collection of the replicas
   */
  Collection<ReplicaInfo> replicas() {
    return map.values();
  }
}
