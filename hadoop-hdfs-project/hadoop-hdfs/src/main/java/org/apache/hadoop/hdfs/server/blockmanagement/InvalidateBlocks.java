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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
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
class InvalidateBlocks {
  /** Mapping: StorageID -> Collection of Blocks */
  private final Map<String, LightWeightHashSet<Block>> node2blocks =
      new TreeMap<String, LightWeightHashSet<Block>>();
  /** The total number of blocks in the map. */
  private long numBlocks = 0L;

  private final DatanodeManager datanodeManager;

  InvalidateBlocks(final DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  /** @return the number of blocks to be invalidated . */
  synchronized long numBlocks() {
    return numBlocks;
  }

  /** Does this contain the block which is associated with the storage? */
  synchronized boolean contains(final String storageID, final Block block) {
    final Collection<Block> s = node2blocks.get(storageID);
    return s != null && s.contains(block);
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
            + ": add " + block + " to " + datanode.getName());
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

  /** Print the contents to out. */
  synchronized void dump(final PrintWriter out) {
    final int size = node2blocks.values().size();
    out.println("Metasave: Blocks " + numBlocks 
        + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }

    for(Map.Entry<String,LightWeightHashSet<Block>> entry : node2blocks.entrySet()) {
      final LightWeightHashSet<Block> blocks = entry.getValue();
      if (blocks.size() > 0) {
        out.println(datanodeManager.getDatanode(entry.getKey()).getName() + blocks);
      }
    }
  }

  /** @return a list of the storage IDs. */
  synchronized List<String> getStorageIDs() {
    return new ArrayList<String>(node2blocks.keySet());
  }

  /** Invalidate work for the storage. */
  int invalidateWork(final String storageId) {
    final DatanodeDescriptor dn = datanodeManager.getDatanode(storageId);
    if (dn == null) {
      remove(storageId);
      return 0;
    }
    final List<Block> toInvalidate = invalidateWork(storageId, dn);
    if (toInvalidate == null) {
      return 0;
    }

    if (NameNode.stateChangeLog.isInfoEnabled()) {
      NameNode.blockStateChangeLog.info("BLOCK* " + getClass().getSimpleName()
          + ": ask " + dn.getName() + " to delete " + toInvalidate);
    }
    return toInvalidate.size();
  }

  private synchronized List<Block> invalidateWork(
      final String storageId, final DatanodeDescriptor dn) {
    final LightWeightHashSet<Block> set = node2blocks.get(storageId);
    if (set == null) {
      return null;
    }

    // # blocks that can be sent in one message is limited
    final int limit = datanodeManager.blockInvalidateLimit;
    final List<Block> toInvalidate = set.pollN(limit);

    // If we send everything in this message, remove this node entry
    if (set.isEmpty()) {
      remove(storageId);
    }

    dn.addBlocksToBeInvalidated(toInvalidate);
    numBlocks -= toInvalidate.size();
    return toInvalidate;
  }
}
