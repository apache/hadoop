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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.PrintWriter;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

/***************************************************
 * PendingReconstructionBlocks does the bookkeeping of all
 * blocks that gains stronger redundancy.
 *
 * It does the following:
 * 1)  record blocks that gains stronger redundancy at this instant.
 * 2)  a coarse grain timer to track age of reconstruction request
 * 3)  a thread that periodically identifies reconstruction-requests
 *     that never made it.
 *
 ***************************************************/
class PendingReconstructionBlocks {
  private static final Logger LOG = BlockManager.LOG;

  private final static long DEFAULT_RECHECK_INTERVAL = 5 * 60 * 1000;

  private final Map<BlockInfo, PendingBlockInfo> pendingReconstructions;
  private final List<BlockInfo> timedOutItems;
  Daemon timerThread = null;
  private long timedOutCount = 0L;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private final long timeout;

  PendingReconstructionBlocks(long timeoutPeriod) {
    this.timeout = (timeoutPeriod > 0) ? timeoutPeriod
        : TimeUnit.SECONDS.toMillis(DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT);
    this.pendingReconstructions = new ConcurrentHashMap<>();
    this.timedOutItems = Collections.synchronizedList(new ArrayList<>());
  }

  void start() {
    timerThread = new Daemon(new PendingReconstructionMonitor());
    timerThread.start();
  }

  /**
   * Add a block to the list of pending reconstructions
   * @param block The corresponding block
   * @param targets The DataNodes where replicas of the block should be placed
   */
  void increment(BlockInfo block, DatanodeStorageInfo... targets) {
    this.pendingReconstructions.compute(block, (key, value) -> {
      if (value == null) {
        value = new PendingBlockInfo(targets);
      } else {
        value.incrementReplicas(targets);
        value.setTimeStamp();
      }
      return value;
    });
  }

  /**
   * One reconstruction request for this block has finished.
   * Decrement the number of pending reconstruction requests
   * for this block.
   *
   * @param dn The DataNode that finishes the reconstruction
   * @return true if the block is decremented to 0 and got removed.
   */
  boolean decrement(BlockInfo block, DatanodeStorageInfo dn) {
    final boolean[] found = { false };
    pendingReconstructions.computeIfPresent(block, (key, value) -> {
      LOG.debug("Removing pending reconstruction for {}", key);
      value.decrementReplicas(dn);
      if (value.getNumReplicas() <= 0) {
        found[0] = true;
        return null;
      }
      return value;
    });
    return found[0];
  }

  /**
   * Remove the record about the given block from pending reconstructions.
   *
   * @param block
   *          The given block whose pending reconstruction requests need to be
   *          removed
   */
  PendingBlockInfo remove(BlockInfo block) {
    return pendingReconstructions.remove(block);
  }

  public void clear() {
      pendingReconstructions.clear();
      timedOutItems.clear();
      timedOutCount = 0L;
  }

  /**
   * The total number of blocks that are undergoing reconstruction.
   */
  int size() {
    return pendingReconstructions.size();
  }

  /**
   * How many copies of this block is pending reconstruction?.
   */
  int getNumReplicas(BlockInfo block) {
    PendingBlockInfo found = pendingReconstructions.get(block);
    return (found == null) ? 0 : found.getNumReplicas();
  }

  /**
   * Used for metrics.
   * @return The number of timeouts
   */
  long getNumTimedOuts() {
    return timedOutCount + timedOutItems.size();
  }

  /**
   * Clears the list of blocks that have timed out their reconstruction
   * requests. Returns null if no blocks have timed out.
   *
   * @return an array of the blocks that were cleared from the list
   */
  BlockInfo[] clearTimedOutBlocks() {
    BlockInfo[] blockList = null;
    synchronized (timedOutItems) {
      if (!timedOutItems.isEmpty()) {
      blockList = timedOutItems.toArray(new BlockInfo[0]);
      timedOutCount += timedOutItems.size();
      timedOutItems.clear();
      }
    }
    return blockList;
  }

  /**
   * An object that contains information about a block that
   * is being reconstructed. It records the timestamp when the
   * system started reconstructing the most recent copy of this
   * block. It also records the list of Datanodes where the
   * reconstruction requests are in progress.
   */
  static class PendingBlockInfo {
    private long timeStamp;
    private final Set<DatanodeStorageInfo> targets;

    PendingBlockInfo(DatanodeStorageInfo[] targets) {
      this.timeStamp = monotonicNow();
      this.targets = new HashSet<>(Arrays.asList(targets));
    }

    long getTimeStamp() {
      return timeStamp;
    }

    void setTimeStamp() {
      timeStamp = monotonicNow();
    }

    void incrementReplicas(DatanodeStorageInfo... newTargets) {
      for (DatanodeStorageInfo newTarget : newTargets) {
        targets.add(newTarget);
      }
    }

    void decrementReplicas(DatanodeStorageInfo dn) {
      Iterator<DatanodeStorageInfo> iterator = targets.iterator();
      while (iterator.hasNext()) {
        DatanodeStorageInfo next = iterator.next();
        if (next.getDatanodeDescriptor().equals(dn.getDatanodeDescriptor())) {
          iterator.remove();
        }
      }
    }

    int getNumReplicas() {
      return targets.size();
    }

    Collection<DatanodeStorageInfo> getTargets() {
      return targets;
    }
  }

  /*
   * A periodic thread that scans for blocks that never finished
   * their reconstruction request.
   */
  class PendingReconstructionMonitor implements Runnable {
    @Override
    public void run() {
      while (true) {
        long period = Math.min(DEFAULT_RECHECK_INTERVAL, timeout);
        try {
          pendingReconstructionCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          LOG.debug("PendingReconstructionMonitor thread is interrupted", ie);
          return;
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    void pendingReconstructionCheck() {
        Iterator<Map.Entry<BlockInfo, PendingBlockInfo>> iter =
            pendingReconstructions.entrySet().iterator();
        final long now = monotonicNow();
        LOG.debug("PendingReconstructionMonitor checking Q");
        while (iter.hasNext()) {
          Map.Entry<BlockInfo, PendingBlockInfo> entry = iter.next();
          PendingBlockInfo pendingBlock = entry.getValue();
          if (now > pendingBlock.getTimeStamp() + timeout) {
            BlockInfo block = entry.getKey();
            LOG.warn("PendingReconstructionMonitor timed out " + block);
            timedOutItems.add(block);
            NameNode.getNameNodeMetrics().incTimeoutReReplications();
            iter.remove();
          }
        }
    }
  }

  /**
   * @return timer thread.
   */
  @VisibleForTesting
  public Daemon getTimerThread() {
    return timerThread;
  }

  /**
   * Shuts down the pending reconstruction monitor thread. Waits for the thread
   * to exit.
   */
  void stop() {
    if (timerThread == null)
      return;
    timerThread.interrupt();
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
      LOG.debug("PendingReconstructionMonitor stop is interrupted", ie);
    }
  }

  /**
   * Iterate through all items and print them.
   */
  void metaSave(PrintWriter out) {
    Set<Entry<BlockInfo, PendingBlockInfo>> entrySet =
        pendingReconstructions.entrySet();
    out.println("Metasave: Blocks being reconstructed: " + entrySet.size());
    for (Map.Entry<BlockInfo, PendingBlockInfo> entry : entrySet) {
      PendingBlockInfo pendingBlock = entry.getValue();
      Block block = entry.getKey();
      out.print(block);
      out.print(" StartTime: " + DateTimeFormatter.ISO_INSTANT
          .format(Instant.ofEpochMilli(pendingBlock.timeStamp)));
      out.print(" NumReconstructInProgress: " + pendingBlock.getNumReplicas());
      out.println();
    }
  }

  Collection<DatanodeStorageInfo> getTargets(BlockInfo block) {
    PendingBlockInfo found = pendingReconstructions.get(block);
    return (found == null) ? Collections.emptySet() : found.targets;
  }
}
