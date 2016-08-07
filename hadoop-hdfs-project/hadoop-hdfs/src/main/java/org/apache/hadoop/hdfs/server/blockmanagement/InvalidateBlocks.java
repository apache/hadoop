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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSUtil;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

/**
 * Keeps a Collection for every named machine containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
class InvalidateBlocks {
  /** Mapping: DatanodeInfo -> Collection of Blocks */
  private final Map<DatanodeInfo, LightWeightHashSet<Block>> node2blocks =
      new TreeMap<DatanodeInfo, LightWeightHashSet<Block>>();
  /** The total number of blocks in the map. */
  private long numBlocks = 0L;

  private final int blockInvalidateLimit;

  /**
   * The period of pending time for block invalidation since the NameNode
   * startup
   */
  private final long pendingPeriodInMs;
  /** the startup time */
  private final long startupTime = Time.monotonicNow();

  InvalidateBlocks(final int blockInvalidateLimit, long pendingPeriodInMs) {
    this.blockInvalidateLimit = blockInvalidateLimit;
    this.pendingPeriodInMs = pendingPeriodInMs;
    printBlockDeletionTime(BlockManager.LOG);
  }

  private void printBlockDeletionTime(final Logger log) {
    log.info(DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY
        + " is set to " + DFSUtil.durationToString(pendingPeriodInMs));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.SECOND, (int) (this.pendingPeriodInMs / 1000));
    log.info("The block deletion will start around "
        + sdf.format(calendar.getTime()));
  }

  /** @return the number of blocks to be invalidated . */
  synchronized long numBlocks() {
    return numBlocks;
  }

  /**
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   */
  synchronized boolean contains(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<Block> s = node2blocks.get(dn);
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
    LightWeightHashSet<Block> set = node2blocks.get(datanode);
    if (set == null) {
      set = new LightWeightHashSet<Block>();
      node2blocks.put(datanode, set);
    }
    if (set.add(block)) {
      numBlocks++;
      if (log) {
        NameNode.blockStateChangeLog.info("BLOCK* {}: add {} to {}",
            getClass().getSimpleName(), block, datanode);
      }
    }
  }

  /** Remove a storage from the invalidatesSet */
  synchronized void remove(final DatanodeInfo dn) {
    final LightWeightHashSet<Block> blocks = node2blocks.remove(dn);
    if (blocks != null) {
      numBlocks -= blocks.size();
    }
  }

  /** Remove the block from the specified storage. */
  synchronized void remove(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<Block> v = node2blocks.get(dn);
    if (v != null && v.remove(block)) {
      numBlocks--;
      if (v.isEmpty()) {
        node2blocks.remove(dn);
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

    for(Map.Entry<DatanodeInfo, LightWeightHashSet<Block>> entry : node2blocks.entrySet()) {
      final LightWeightHashSet<Block> blocks = entry.getValue();
      if (blocks.size() > 0) {
        out.println(entry.getKey());
        out.println(StringUtils.join(",", blocks));
      }
    }
  }

  /** @return a list of the storage IDs. */
  synchronized List<DatanodeInfo> getDatanodes() {
    return new ArrayList<DatanodeInfo>(node2blocks.keySet());
  }

  /**
   * @return the remianing pending time
   */
  @VisibleForTesting
  long getInvalidationDelay() {
    return pendingPeriodInMs - (Time.monotonicNow() - startupTime);
  }

  synchronized List<Block> invalidateWork(final DatanodeDescriptor dn) {
    final long delay = getInvalidationDelay();
    if (delay > 0) {
      if (BlockManager.LOG.isDebugEnabled()) {
        BlockManager.LOG
            .debug("Block deletion is delayed during NameNode startup. "
                       + "The deletion will start after " + delay + " ms.");
      }
      return null;
    }
    final LightWeightHashSet<Block> set = node2blocks.get(dn);
    if (set == null) {
      return null;
    }

    // # blocks that can be sent in one message is limited
    final int limit = blockInvalidateLimit;
    final List<Block> toInvalidate = set.pollN(limit);

    // If we send everything in this message, remove this node entry
    if (set.isEmpty()) {
      remove(dn);
    }

    dn.addBlocksToBeInvalidated(toInvalidate);
    numBlocks -= toInvalidate.size();
    return toInvalidate;
  }

  synchronized void clear() {
    node2blocks.clear();
    numBlocks = 0;
  }
}
