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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

/**
 * Keep prioritized queues of low redundant blocks.
 * Blocks have redundancy priority, with priority
 * {@link #QUEUE_HIGHEST_PRIORITY} indicating the highest priority.
 * </p>
 * Having a prioritised queue allows the {@link BlockManager} to select
 * which blocks to replicate first -it tries to give priority to data
 * that is most at risk or considered most valuable.
 *
 * <p/>
 * The policy for choosing which priority to give added blocks
 * is implemented in {@link #getPriority(BlockInfo, int, int, int, int)}.
 * </p>
 * <p>The queue order is as follows:</p>
 * <ol>
 *   <li>{@link #QUEUE_HIGHEST_PRIORITY}: the blocks that should be redundant
 *   first. That is blocks with only one copy, or blocks with zero live
 *   copies but a copy in a node being decommissioned. These blocks
 *   are at risk of loss if the disk or server on which they
 *   remain fails.</li>
 *   <li>{@link #QUEUE_VERY_LOW_REDUNDANCY}: blocks that are very
 *   under-replicated compared to their expected values. Currently
 *   that means the ratio of the ratio of actual:expected means that
 *   there is <i>less than</i> 1:3.</li>. These blocks may not be at risk,
 *   but they are clearly considered "important".
 *   <li>{@link #QUEUE_LOW_REDUNDANCY}: blocks that are also under
 *   replicated, and the ratio of actual:expected is good enough that
 *   they do not need to go into the {@link #QUEUE_VERY_LOW_REDUNDANCY}
 *   queue.</li>
 *   <li>{@link #QUEUE_REPLICAS_BADLY_DISTRIBUTED}: there are as least as
 *   many copies of a block as required, but the blocks are not adequately
 *   distributed. Loss of a rack/switch could take all copies off-line.</li>
 *   <li>{@link #QUEUE_WITH_CORRUPT_BLOCKS} This is for blocks that are corrupt
 *   and for which there are no-non-corrupt copies (currently) available.
 *   The policy here is to keep those corrupt blocks replicated, but give
 *   blocks that are not corrupt higher priority.</li>
 * </ol>
 */
class LowRedundancyBlocks implements Iterable<BlockInfo> {
  /** The total number of queues : {@value} */
  static final int LEVEL = 5;
  /** The queue with the highest priority: {@value} */
  static final int QUEUE_HIGHEST_PRIORITY = 0;
  /** The queue for blocks that are way below their expected value : {@value} */
  static final int QUEUE_VERY_LOW_REDUNDANCY = 1;
  /**
   * The queue for "normally" without sufficient redundancy blocks : {@value}.
   */
  static final int QUEUE_LOW_REDUNDANCY = 2;
  /** The queue for blocks that have the right number of replicas,
   * but which the block manager felt were badly distributed: {@value}
   */
  static final int QUEUE_REPLICAS_BADLY_DISTRIBUTED = 3;
  /** The queue for corrupt blocks: {@value} */
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;
  /** the queues themselves */
  private final List<LightWeightLinkedSet<BlockInfo>> priorityQueues
      = new ArrayList<>(LEVEL);


  private final LongAdder lowRedundancyBlocks = new LongAdder();
  private final LongAdder corruptBlocks = new LongAdder();
  /** The number of corrupt blocks with replication factor 1 */
  private final LongAdder corruptReplicationOneBlocks = new LongAdder();
  private final LongAdder lowRedundancyECBlockGroups = new LongAdder();
  private final LongAdder corruptECBlockGroups = new LongAdder();
  private final LongAdder highestPriorityLowRedundancyReplicatedBlocks
      = new LongAdder();
  private final LongAdder highestPriorityLowRedundancyECBlocks
      = new LongAdder();

  /** Create an object. */
  LowRedundancyBlocks() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.add(new LightWeightLinkedSet<BlockInfo>());
    }
  }

  /**
   * Empty the queues.
   */
  synchronized void clear() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
    lowRedundancyBlocks.reset();
    corruptBlocks.reset();
    corruptReplicationOneBlocks.reset();
    lowRedundancyECBlockGroups.reset();
    corruptECBlockGroups.reset();
    highestPriorityLowRedundancyReplicatedBlocks.reset();
    highestPriorityLowRedundancyECBlocks.reset();
  }

  /** Return the total number of insufficient redundancy blocks. */
  synchronized int size() {
    int size = 0;
    for (int i = 0; i < LEVEL; i++) {
      size += priorityQueues.get(i).size();
    }
    return size;
  }

  /**
   * Return the number of insufficiently redundant blocks excluding corrupt
   * blocks.
   */
  synchronized int getLowRedundancyBlockCount() {
    int size = 0;
    for (int i = 0; i < LEVEL; i++) {
      if (i != QUEUE_WITH_CORRUPT_BLOCKS) {
        size += priorityQueues.get(i).size();
      }
    }
    return size;
  }

  /** Return the number of corrupt blocks */
  synchronized int getCorruptBlockSize() {
    return priorityQueues.get(QUEUE_WITH_CORRUPT_BLOCKS).size();
  }

  /** Return the number of corrupt blocks with replication factor 1 */
  long getCorruptReplicationOneBlockSize() {
    return getCorruptReplicationOneBlocks();
  }

  /**
   * Return under replicated block count excluding corrupt replicas.
   */
  long getLowRedundancyBlocks() {
    return lowRedundancyBlocks.longValue() - getCorruptBlocks();
  }

  long getCorruptBlocks() {
    return corruptBlocks.longValue();
  }

  long getCorruptReplicationOneBlocks() {
    return corruptReplicationOneBlocks.longValue();
  }

  /** Return the number of under replicated blocks
   * with the highest priority to recover */
  long getHighestPriorityReplicatedBlockCount() {
    return highestPriorityLowRedundancyReplicatedBlocks.longValue();
  }

  /** Return the number of under replicated EC blocks
   * with the highest priority to recover */
  long getHighestPriorityECBlockCount() {
    return highestPriorityLowRedundancyECBlocks.longValue();
  }

  /**
   *  Return low redundancy striped blocks excluding corrupt blocks.
   */
  long getLowRedundancyECBlockGroups() {
    return lowRedundancyECBlockGroups.longValue() -
        getCorruptECBlockGroups();
  }

  long getCorruptECBlockGroups() {
    return corruptECBlockGroups.longValue();
  }

  /** Check if a block is in the neededReconstruction queue. */
  synchronized boolean contains(BlockInfo block) {
    for(LightWeightLinkedSet<BlockInfo> set : priorityQueues) {
      if (set.contains(block)) {
        return true;
      }
    }
    return false;
  }

  /** Return the priority of a block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   * @return the priority for the blocks, between 0 and ({@link #LEVEL}-1)
   */
  private int getPriority(BlockInfo block,
                          int curReplicas,
                          int readOnlyReplicas,
                          int outOfServiceReplicas,
                          int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    if (curReplicas >= expectedReplicas) {
      // Block has enough copies, but not enough racks
      return QUEUE_REPLICAS_BADLY_DISTRIBUTED;
    }
    if (block.isStriped()) {
      BlockInfoStriped sblk = (BlockInfoStriped) block;
      return getPriorityStriped(curReplicas, outOfServiceReplicas,
          sblk.getRealDataBlockNum(), sblk.getParityBlockNum());
    } else {
      return getPriorityContiguous(curReplicas, readOnlyReplicas,
          outOfServiceReplicas, expectedReplicas);
    }
  }

  private int getPriorityContiguous(int curReplicas, int readOnlyReplicas,
      int outOfServiceReplicas, int expectedReplicas) {
    if (curReplicas == 0) {
      // If there are zero non-decommissioned replicas but there are
      // some out of service replicas, then assign them highest priority
      if (outOfServiceReplicas > 0) {
        return QUEUE_HIGHEST_PRIORITY;
      }
      if (readOnlyReplicas > 0) {
        // only has read-only replicas, highest risk
        // since the read-only replicas may go down all together.
        return QUEUE_HIGHEST_PRIORITY;
      }
      //all we have are corrupt blocks
      return QUEUE_WITH_CORRUPT_BLOCKS;
    } else if (curReplicas == 1) {
      // only one replica, highest risk of loss
      // highest priority
      return QUEUE_HIGHEST_PRIORITY;
    } else if ((curReplicas * 3) < expectedReplicas) {
      //there is less than a third as many blocks as requested;
      //this is considered very under-replicated.
      return QUEUE_VERY_LOW_REDUNDANCY;
    } else {
      //add to the normal queue for insufficiently redundant blocks
      return QUEUE_LOW_REDUNDANCY;
    }
  }

  private int getPriorityStriped(int curReplicas, int outOfServiceReplicas,
      short dataBlkNum, short parityBlkNum) {
    if (curReplicas < dataBlkNum) {
      // There are some replicas on decommissioned nodes so it's not corrupted
      if (curReplicas + outOfServiceReplicas >= dataBlkNum) {
        return QUEUE_HIGHEST_PRIORITY;
      }
      return QUEUE_WITH_CORRUPT_BLOCKS;
    } else if (curReplicas == dataBlkNum) {
      // highest risk of loss, highest priority
      return QUEUE_HIGHEST_PRIORITY;
    } else if ((curReplicas - dataBlkNum) * 3 < parityBlkNum + 1) {
      // can only afford one replica loss
      // this is considered very insufficiently redundant blocks.
      return QUEUE_VERY_LOW_REDUNDANCY;
    } else {
      // add to the normal queue for insufficiently redundant blocks.
      return QUEUE_LOW_REDUNDANCY;
    }
  }

  /**
   * Add a block to insufficiently redundant queue according to its priority.
   *
   * @param block a low redundancy block
   * @param curReplicas current number of replicas of the block
   * @param outOfServiceReplicas the number of out-of-service replicas
   * @param expectedReplicas expected number of replicas of the block
   * @return true if the block was added to a queue.
   */
  synchronized boolean add(BlockInfo block,
      int curReplicas, int readOnlyReplicas,
      int outOfServiceReplicas, int expectedReplicas) {
    final int priLevel = getPriority(block, curReplicas, readOnlyReplicas,
        outOfServiceReplicas, expectedReplicas);
    if(add(block, priLevel, expectedReplicas)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK* NameSystem.LowRedundancyBlock.add: {}"
              + " has only {} replicas and need {} replicas so is added to"
              + " neededReconstructions at priority level {}",
          block, curReplicas, expectedReplicas, priLevel);

      return true;
    }
    return false;
  }

  private boolean add(BlockInfo blockInfo, int priLevel, int expectedReplicas) {
    if (priorityQueues.get(priLevel).add(blockInfo)) {
      incrementBlockStat(blockInfo, priLevel, expectedReplicas);
      return true;
    }
    return false;
  }

  private void incrementBlockStat(BlockInfo blockInfo, int priLevel,
      int expectedReplicas) {
    if (blockInfo.isStriped()) {
      lowRedundancyECBlockGroups.increment();
      if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS) {
        corruptECBlockGroups.increment();
      }
      if (priLevel == QUEUE_HIGHEST_PRIORITY) {
        highestPriorityLowRedundancyECBlocks.increment();
      }
    } else {
      lowRedundancyBlocks.increment();
      if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS) {
        corruptBlocks.increment();
        if (expectedReplicas == 1) {
          corruptReplicationOneBlocks.increment();
        }
      }
      if (priLevel == QUEUE_HIGHEST_PRIORITY) {
        highestPriorityLowRedundancyReplicatedBlocks.increment();
      }
    }
  }

  /** Remove a block from a low redundancy queue. */
  synchronized boolean remove(BlockInfo block,
      int oldReplicas, int oldReadOnlyReplicas,
      int outOfServiceReplicas, int oldExpectedReplicas) {
    final int priLevel = getPriority(block, oldReplicas, oldReadOnlyReplicas,
        outOfServiceReplicas, oldExpectedReplicas);
    boolean removedBlock = remove(block, priLevel, oldExpectedReplicas);
    if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS &&
        oldExpectedReplicas == 1 &&
        removedBlock) {
      assert corruptReplicationOneBlocks.longValue() >= 0 :
          "Number of corrupt blocks with replication factor 1 " +
              "should be non-negative";
    }
    return removedBlock;
  }

  /**
   * Remove a block from the low redundancy queues.
   *
   * The priLevel parameter is a hint of which queue to query
   * first: if negative or &gt;= {@link #LEVEL} this shortcutting
   * is not attmpted.
   *
   * If the block is not found in the nominated queue, an attempt is made to
   * remove it from all queues.
   *
   * <i>Warning:</i> This is not a synchronized method.
   * @param block block to remove
   * @param priLevel expected privilege level
   * @return true if the block was found and removed from one of the priority
   *         queues
   */
  synchronized boolean remove(BlockInfo block, int priLevel) {
    return remove(block, priLevel, block.getReplication());
  }

  synchronized boolean remove(BlockInfo block, int priLevel, int oldExpectedReplicas) {
    if(priLevel >= 0 && priLevel < LEVEL
        && priorityQueues.get(priLevel).remove(block)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK* NameSystem.LowRedundancyBlock.remove: Removing block {}"
              + " from priority queue {}",
          block, priLevel);
      decrementBlockStat(block, priLevel, oldExpectedReplicas);
      return true;
    } else {
      // Try to remove the block from all queues if the block was
      // not found in the queue for the given priority level.
      boolean found = false;
      for (int i = 0; i < LEVEL; i++) {
        if (i != priLevel && priorityQueues.get(i).remove(block)) {
          NameNode.blockStateChangeLog.debug(
              "BLOCK* NameSystem.LowRedundancyBlock.remove: Removing block" +
                  " {} from priority queue {}", block, i);
          decrementBlockStat(block, i, oldExpectedReplicas);
          found = true;
        }
      }
      return found;
    }
  }

  private void decrementBlockStat(BlockInfo blockInfo, int priLevel,
      int oldExpectedReplicas) {
    if (blockInfo.isStriped()) {
      lowRedundancyECBlockGroups.decrement();
      if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS) {
        corruptECBlockGroups.decrement();
      }
      if (priLevel == QUEUE_HIGHEST_PRIORITY) {
        highestPriorityLowRedundancyECBlocks.decrement();
      }
    } else {
      lowRedundancyBlocks.decrement();
      if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS) {
        corruptBlocks.decrement();
        if (oldExpectedReplicas == 1) {
          corruptReplicationOneBlocks.decrement();
          assert corruptReplicationOneBlocks.longValue() >= 0 :
              "Number of corrupt blocks with replication factor 1 " +
                  "should be non-negative";
        }
      }
      if (priLevel == QUEUE_HIGHEST_PRIORITY) {
        highestPriorityLowRedundancyReplicatedBlocks.decrement();
      }
    }
  }

  /**
   * Recalculate and potentially update the priority level of a block.
   *
   * If the block priority has changed from before an attempt is made to
   * remove it from the block queue. Regardless of whether or not the block
   * is in the block queue of (recalculate) priority, an attempt is made
   * to add it to that queue. This ensures that the block will be
   * in its expected priority queue (and only that queue) by the end of the
   * method call.
   * @param block a low redundancy block
   * @param curReplicas current number of replicas of the block
   * @param outOfServiceReplicas  the number of out-of-service replicas
   * @param curExpectedReplicas expected number of replicas of the block
   * @param curReplicasDelta the change in the replicate count from before
   * @param expectedReplicasDelta the change in the expected replica count
   *        from before
   */
  synchronized void update(BlockInfo block, int curReplicas,
      int readOnlyReplicas, int outOfServiceReplicas,
      int curExpectedReplicas,
      int curReplicasDelta, int expectedReplicasDelta) {
    int oldReplicas = curReplicas-curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
    int curPri = getPriority(block, curReplicas, readOnlyReplicas,
        outOfServiceReplicas, curExpectedReplicas);
    int oldPri = getPriority(block, oldReplicas, readOnlyReplicas,
        outOfServiceReplicas, oldExpectedReplicas);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("LowRedundancyBlocks.update " +
        block +
        " curReplicas " + curReplicas +
        " curExpectedReplicas " + curExpectedReplicas +
        " oldReplicas " + oldReplicas +
        " oldExpectedReplicas  " + oldExpectedReplicas +
        " curPri  " + curPri +
        " oldPri  " + oldPri);
    }
    // oldPri is mostly correct, but not always. If not found with oldPri,
    // other levels will be searched until the block is found & removed.
    remove(block, oldPri, oldExpectedReplicas);
    if(add(block, curPri, curExpectedReplicas)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK* NameSystem.LowRedundancyBlock.update: {} has only {} "
              + "replicas and needs {} replicas so is added to "
              + "neededReconstructions at priority level {}",
          block, curReplicas, curExpectedReplicas, curPri);

    }
  }

  /**
   * Get a list of block lists without sufficient redundancy. The index of
   * block lists represents its replication priority. Iterates each block list
   * in priority order beginning with the highest priority list. Iterators use
   * a bookmark to resume where the previous iteration stopped. Returns when
   * the block count is met or iteration reaches the end of the lowest priority
   * list, in which case bookmarks for each block list are reset to the heads
   * of their respective lists.
   *
   * @param blocksToProcess - number of blocks to fetch from low redundancy
   *          blocks.
   * @return Return a list of block lists to be replicated. The block list
   *         index represents its redundancy priority.
   */
  synchronized List<List<BlockInfo>> chooseLowRedundancyBlocks(
      int blocksToProcess) {
    return chooseLowRedundancyBlocks(blocksToProcess, false);
  }

  /**
   * Get a list of block lists without sufficient redundancy. The index of
   * block lists represents its replication priority. Iterates each block list
   * in priority order beginning with the highest priority list. Iterators use
   * a bookmark to resume where the previous iteration stopped. Returns when
   * the block count is met or iteration reaches the end of the lowest priority
   * list, in which case bookmarks for each block list are reset to the heads
   * of their respective lists.
   * If a block is deleted (has invalid bcId), it will be removed from the low
   * redundancy queues.
   *
   * @param blocksToProcess - number of blocks to fetch from low redundancy
   *          blocks.
   * @param resetIterators - After gathering the list of blocks reset the
   *           position of all queue iterators to the head of the queue so
   *           subsequent calls will begin at the head of the queue
   * @return Return a list of block lists to be replicated. The block list
   *         index represents its redundancy priority.
   */
  synchronized List<List<BlockInfo>> chooseLowRedundancyBlocks(
      int blocksToProcess, boolean resetIterators) {
    final List<List<BlockInfo>> blocksToReconstruct = new ArrayList<>(LEVEL);
    chooseLowRedundancyBlocks(blocksToProcess, resetIterators, blocksToReconstruct);
    return blocksToReconstruct;
  }

  /**
   * @param blocksToProcess - number of blocks to fetch from low redundancy
   *          blocks.
   * @param resetIterators - After gathering the list of blocks reset the
   *           position of all queue iterators to the head of the queue so
   *           subsequent calls will begin at the head of the queue
   * @param blocksToReconstruct - The found candidates blocks need to be reconstructed
   * @return Return the number of found low-redundancy blocks(They are put in blocksToReconstruct)
   */
  synchronized int chooseLowRedundancyBlocks(
          int blocksToProcess, boolean resetIterators, List<List<BlockInfo>> blocksToReconstruct) {
    int foundBlocks = 0;
    int count = 0;
    int priority = 0;
    HashSet<BlockInfo> toRemove = new HashSet<>();
    for (; count < blocksToProcess && priority < LEVEL; priority++) {
      // Go through all blocks that need reconstructions with current priority.
      // Set the iterator to the first unprocessed block at this priority level
      // We do not want to skip QUEUE_WITH_CORRUPT_BLOCKS because we still need
      // to look for deleted blocks if any.
      final boolean inCorruptLevel = (QUEUE_WITH_CORRUPT_BLOCKS == priority);
      final Iterator<BlockInfo> i = priorityQueues.get(priority).getBookmark();
      final List<BlockInfo> blocks = new LinkedList<>();
      if (!inCorruptLevel) {
        blocksToReconstruct.add(blocks);
      }
      for(; count < blocksToProcess && i.hasNext(); count++) {
        BlockInfo block = i.next();
        if (block.isDeleted()) {
          toRemove.add(block);
          continue;
        }
        if (!inCorruptLevel) {
          foundBlocks++;
          blocks.add(block);
        }
      }
      for (BlockInfo bInfo : toRemove) {
        remove(bInfo, priority);
      }
      toRemove.clear();
    }

    if (priority == LEVEL || resetIterators) {
      // Reset all bookmarks because there were no recently added blocks.
      for (LightWeightLinkedSet<BlockInfo> q : priorityQueues) {
        q.resetBookmark();
      }
    }

    return foundBlocks;
  }

  /** Returns an iterator of all blocks in a given priority queue. */
  synchronized Iterator<BlockInfo> iterator(int level) {
    return priorityQueues.get(level).iterator();
  }

  /** Return an iterator of all the low redundancy blocks. */
  @Override
  public synchronized Iterator<BlockInfo> iterator() {
    final Iterator<LightWeightLinkedSet<BlockInfo>> q = priorityQueues.iterator();
    return new Iterator<BlockInfo>() {
      private Iterator<BlockInfo> b = q.next().iterator();

      @Override
      public BlockInfo next() {
        hasNext();
        return b.next();
      }

      @Override
      public boolean hasNext() {
        for(; !b.hasNext() && q.hasNext(); ) {
          b = q.next().iterator();
        }
        return b.hasNext();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}