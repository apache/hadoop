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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;

/**
 * Keeps a Collection for every named machine containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
abstract class InvalidateBlocks {
  /** Mapping: StorageID -> Collection of Blocks */
  private final Map<String, LightWeightHashSet<Block>> node2blocks =
      new TreeMap<String, LightWeightHashSet<Block>>();
  /** The total number of blocks in the map. */
  private long numBlocks = 0L;

  /** @return the number of blocks to be invalidated . */
  synchronized long numBlocks() {
    return numBlocks;
  }

  synchronized int numStorages() {
    return node2blocks.size();
  }

  /**
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   * @param storageID the storage to check
   * @param the block to look for
   * 
   */
  synchronized boolean contains(final String storageID, final Block block) {
    final LightWeightHashSet<Block> s = node2blocks.get(storageID);
    if (s == null) {
      return false; // no invalidate blocks for this storage ID
    }
    Block blockInSet = s.getElement(block);
    return blockInSet != null &&
        block.getGenerationStamp() == blockInSet.getGenerationStamp();
  }

  /**
   * Add a block to the block collection
   * which will be invalidated on the specified datanode.
   */
  synchronized void add(final Block block, final DatanodeInfo datanode,
      final boolean log) {
    LightWeightHashSet<Block> set = node2blocks.get(datanode.getStorageID());
    if (set == null) {
      set = new LightWeightHashSet<Block>();
      node2blocks.put(datanode.getStorageID(), set);
    }
    if (set.add(block)) {
      numBlocks++;
      if (log) {
        NameNode.blockStateChangeLog.info("BLOCK* " + getClass().getSimpleName()
            + ": add " + block + " to " + datanode);
      }
    }
  }

  /** Remove a storage from the invalidatesSet */
  synchronized void remove(final String storageID) {
    final LightWeightHashSet<Block> blocks = node2blocks.remove(storageID);
    if (blocks != null) {
      numBlocks -= blocks.size();
    }
  }

  /** Remove the block from the specified storage. */
  synchronized void remove(final String storageID, final Block block) {
    final LightWeightHashSet<Block> v = node2blocks.get(storageID);
    if (v != null && v.remove(block)) {
      numBlocks--;
      if (v.isEmpty()) {
        node2blocks.remove(storageID);
      }
    }
  }

  /**
   * Polls up to <i>limit</i> blocks from the list of to-be-invalidated Blocks
   * for a storage.
   */
  synchronized List<Block> pollNumBlocks(final String storageId, final int limit) {
    final LightWeightHashSet<Block> set = node2blocks.get(storageId);
    if (set == null) {
      return null;
    }
    List<Block> polledBlocks = set.pollN(limit);
    // Remove the storage if the set is now empty
    if (set.isEmpty()) {
      remove(storageId);
    }
    numBlocks -= polledBlocks.size();
    return polledBlocks;
  }

  /** @return a list of the storage IDs. */
  synchronized List<String> getStorageIDs() {
    return new ArrayList<String>(node2blocks.keySet());
  }

  /**
   * Return the set of to-be-invalidated blocks for a storage.
   */
  synchronized LightWeightHashSet<Block> getBlocks(String storageId) {
    return node2blocks.get(storageId);
  }

  /**
   * Schedules invalidation work associated with a storage at the corresponding
   * datanode.
   * @param storageId Storage of blocks to be invalidated
   * @param dn Datanode where invalidation work will be scheduled
   * @return List of blocks scheduled for invalidation at the datanode
   */
  abstract List<Block> invalidateWork(final String storageId,
      final DatanodeDescriptor dn);
  
  synchronized void clear() {
    node2blocks.clear();
    numBlocks = 0;
  }
}
