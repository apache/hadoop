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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Keeps a Collection for every named machine containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
class InvalidateBlocks {
  private final Map<DatanodeInfo, LightWeightHashSet<BlockWithDeletionTime>>
      nodeToBlocks = new HashMap<>();
  private final Map<DatanodeInfo, LightWeightHashSet<BlockWithDeletionTime>>
      nodeToECBlocks = new HashMap<>();
  private final LongAdder numBlocks = new LongAdder();
  private final LongAdder numECBlocks = new LongAdder();
  private final int blockInvalidateLimit;
  private final BlockIdManager blockIdManager;

  /**
   * The period of pending time for block invalidation since the NameNode
   * startup
   */
  private final long pendingPeriodInMs;
  /** the startup time */
  private final long startupTime = Time.monotonicNow();

  InvalidateBlocks(final int blockInvalidateLimit, long pendingPeriodInMs,
                   final BlockIdManager blockIdManager) {
    this.blockInvalidateLimit = blockInvalidateLimit;
    this.pendingPeriodInMs = pendingPeriodInMs;
    this.blockIdManager = blockIdManager;
    printBlockDeletionTime();
  }

  private void printBlockDeletionTime() {
    BlockManager.LOG.info("{} is set to {}",
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSUtil.durationToString(pendingPeriodInMs));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.SECOND, (int) (this.pendingPeriodInMs / 1000));
    BlockManager.LOG.info("The block deletion will start around {}",
        sdf.format(calendar.getTime()));
  }

  /**
   * @return The total number of blocks to be invalidated.
   */
  long numBlocks() {
    return getECBlocks() + getBlocks();
  }

  /**
   * @return The total number of blocks of type
   * {@link org.apache.hadoop.hdfs.protocol.BlockType#CONTIGUOUS}
   * to be invalidated.
   */
  long getBlocks() {
    return numBlocks.longValue();
  }

  /**
   * @return The total number of blocks of type
   * {@link org.apache.hadoop.hdfs.protocol.BlockType#STRIPED}
   * to be invalidated.
   */
  long getECBlocks() {
    return numECBlocks.longValue();
  }

  private LightWeightHashSet<BlockWithDeletionTime> getBlocksSet(final DatanodeInfo dn) {
    return nodeToBlocks.get(dn);
  }

  private LightWeightHashSet<BlockWithDeletionTime> getECBlocksSet(final DatanodeInfo dn) {
    return nodeToECBlocks.get(dn);
  }

  private LightWeightHashSet<BlockWithDeletionTime> getBlocksSet(final DatanodeInfo dn,
      final Block block) {
    if (blockIdManager.isStripedBlock(block)) {
      return getECBlocksSet(dn);
    } else {
      return getBlocksSet(dn);
    }
  }

  private void putBlocksSet(final DatanodeInfo dn, final Block block,
      final LightWeightHashSet set) {
    if (blockIdManager.isStripedBlock(block)) {
      assert getECBlocksSet(dn) == null;
      nodeToECBlocks.put(dn, set);
    } else {
      assert getBlocksSet(dn) == null;
      nodeToBlocks.put(dn, set);
    }
  }

  private long getBlockSetsSize(final DatanodeInfo dn) {
    LightWeightHashSet<BlockWithDeletionTime> replicaBlocks = getBlocksSet(dn);
    LightWeightHashSet<BlockWithDeletionTime> stripedBlocks = getECBlocksSet(dn);
    return ((replicaBlocks == null ? 0 : replicaBlocks.size()) +
        (stripedBlocks == null ? 0 : stripedBlocks.size()));
  }


  /**
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   */
  synchronized boolean contains(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<BlockWithDeletionTime> s = getBlocksSet(dn, block);
    if (s == null) {
      return false; // no invalidate blocks for this storage ID
    }
    BlockWithDeletionTime key = new BlockWithDeletionTime(block);
    BlockWithDeletionTime blockInSet = s.getElement(key);
    return blockInSet != null &&
        block.getGenerationStamp() == blockInSet.block.getGenerationStamp();
  }

  synchronized void add(final Block block, final DatanodeInfo datanodeInfo,
      final boolean log) {
    addWithGracePeriod(block, datanodeInfo, 0, log);
  }

  /**
   * Add a block to the block collection which will be
   * invalidated on the specified datanode.
   */
  synchronized void addWithGracePeriod(final Block block, final DatanodeInfo datanode,
      final long gracePeriodMs, final boolean log) {
    LightWeightHashSet<BlockWithDeletionTime> set = getBlocksSet(datanode, block);
    if (set == null) {
      set = new LightWeightHashSet<>();
      putBlocksSet(datanode, block, set);
    }
    BlockWithDeletionTime key = new BlockWithDeletionTime(block, gracePeriodMs);
    if (set.add(key)) {
      if (blockIdManager.isStripedBlock(block)) {
        numECBlocks.increment();
      } else {
        numBlocks.increment();
      }
      if (log) {
        NameNode.blockStateChangeLog.debug("BLOCK* {}: add {} to {} with grace period {}ms",
            getClass().getSimpleName(), block, datanode, gracePeriodMs);
      }
    }
  }

  /** Remove a storage from the invalidatesSet */
  synchronized void remove(final DatanodeInfo dn) {
    LightWeightHashSet<BlockWithDeletionTime> replicaBlockSets = nodeToBlocks.remove(dn);
    if (replicaBlockSets != null) {
      numBlocks.add(replicaBlockSets.size() * -1);
    }
    LightWeightHashSet<BlockWithDeletionTime> ecBlocksSet = nodeToECBlocks.remove(dn);
    if (ecBlocksSet != null) {
      numECBlocks.add(ecBlocksSet.size() * -1);
    }
  }

  /** Remove the block from the specified storage. */
  synchronized void remove(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<BlockWithDeletionTime> v = getBlocksSet(dn, block);
    BlockWithDeletionTime key = new BlockWithDeletionTime(block);
    if (v != null && v.remove(key)) {
      if (blockIdManager.isStripedBlock(block)) {
        numECBlocks.decrement();
      } else {
        numBlocks.decrement();
      }
      if (v.isEmpty() && getBlockSetsSize(dn) == 0) {
        remove(dn);
      }
    }
  }

  private void dumpBlockSet(final Map<DatanodeInfo,
      LightWeightHashSet<BlockWithDeletionTime>> nodeToBlocksMap, final PrintWriter out) {
    for(Entry<DatanodeInfo, LightWeightHashSet<BlockWithDeletionTime>> entry :
        nodeToBlocksMap.entrySet()) {
      final LightWeightHashSet<BlockWithDeletionTime> blocks = entry.getValue();
      if (blocks != null && blocks.size() > 0) {
        out.println(entry.getKey());
        out.println(StringUtils.join(',', blocks));
      }
    }
  }
  /** Print the contents to out. */
  synchronized void dump(final PrintWriter out) {
    final int size = nodeToBlocks.values().size() +
        nodeToECBlocks.values().size();
    out.println("Metasave: Blocks " + numBlocks()
        + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }
    dumpBlockSet(nodeToBlocks, out);
    dumpBlockSet(nodeToECBlocks, out);
  }

  /** @return a list of the storage IDs. */
  synchronized List<DatanodeInfo> getDatanodes() {
    HashSet<DatanodeInfo> set = new HashSet<>();
    set.addAll(nodeToBlocks.keySet());
    set.addAll(nodeToECBlocks.keySet());
    return new ArrayList<>(set);
  }

  /**
   * @return the remianing pending time
   */
  @VisibleForTesting
  long getInvalidationDelay() {
    return pendingPeriodInMs - (Time.monotonicNow() - startupTime);
  }

  /**
   * Get blocks to invalidate by limit as blocks that can be sent in one
   * message is limited.
   * @return the remaining limit
   */
  private int getBlocksToInvalidateByLimit(LightWeightHashSet<BlockWithDeletionTime> blockSet,
      List<Block> toInvalidate, LongAdder statsAdder, int limit) {
    assert blockSet != null;
    int remainingLimit = limit;
    long now = Time.monotonicNow();
    List<BlockWithDeletionTime> polledBlocks = blockSet.pollNWithFilter(limit,
        block -> now >= block.readyForDeleteAt);
    remainingLimit -= polledBlocks.size();
    polledBlocks.stream().map(block -> block.block).forEach(toInvalidate::add);
    statsAdder.add(polledBlocks.size() * -1);
    return remainingLimit;
  }

  synchronized List<Block> invalidateWork(final DatanodeDescriptor dn) {
    final long delay = getInvalidationDelay();
    if (delay > 0) {
      BlockManager.LOG
          .debug("Block deletion is delayed during NameNode startup. "
              + "The deletion will start after {} ms.", delay);
      return null;
    }

    int remainingLimit = blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<>();

    if (nodeToBlocks.get(dn) != null) {
      remainingLimit = getBlocksToInvalidateByLimit(nodeToBlocks.get(dn),
          toInvalidate, numBlocks, remainingLimit);
    }
    if ((remainingLimit > 0) && (nodeToECBlocks.get(dn) != null)) {
      getBlocksToInvalidateByLimit(nodeToECBlocks.get(dn),
          toInvalidate, numECBlocks, remainingLimit);
    }
    if (toInvalidate.size() > 0) {
      if (getBlockSetsSize(dn) == 0) {
        remove(dn);
      }
      dn.addBlocksToBeInvalidated(toInvalidate);
    }
    return toInvalidate;
  }
  
  synchronized void clear() {
    nodeToBlocks.clear();
    nodeToECBlocks.clear();
    numBlocks.reset();
    numECBlocks.reset();
  }

  /**
   * Wraps a Block with a deletion time. Only the Block is counted
   * for hashCode and equals, as the deletion time is considered non-identifying
   * metadata. The deletion time is used for filtering blocks on read, such that
   * we can only return blocks which are ready for deletion.
   */
  static class BlockWithDeletionTime {
    private final Block block;
    private final long readyForDeleteAt;

    /**
     * Creates a BlockWithDeletionTime with a timestamp of 0, which
     * ensures the block will be considered immediately ready for deletion.
     */
    private BlockWithDeletionTime(Block block) {
      this(block, 0);
    }

    /**
     * Creates a BlockWithDeletionTime with a timestamp in the future,
     * based on the passed gracePeriodMs. The block will be considered
     * ready for deletion once time surpasses now + grace period.
     */
    private BlockWithDeletionTime(Block block, long gracePeriodMs) {
      this.block = block;
      this.readyForDeleteAt = Time.monotonicNow() + gracePeriodMs;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BlockWithDeletionTime that = (BlockWithDeletionTime) o;
      return block.equals(that.block);
    }

    @Override
    public int hashCode() {
      return Objects.hash(block);
    }

    @Override
    public String toString() {
      return block.toString();
    }
  }
}
