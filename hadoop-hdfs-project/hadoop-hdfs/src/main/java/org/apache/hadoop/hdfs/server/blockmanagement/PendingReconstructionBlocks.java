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
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

  private final Map<BlockInfo, PendingBlockInfo> pendingReconstructions;
  private final ArrayList<BlockInfo> timedOutItems;
  Daemon timerThread = null;
  private volatile boolean fsRunning = true;
  private long timedOutCount = 0L;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long timeout =
      DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT * 1000;
  private final static long DEFAULT_RECHECK_INTERVAL = 5 * 60 * 1000;

  PendingReconstructionBlocks(long timeoutPeriod) {
    if ( timeoutPeriod > 0 ) {
      this.timeout = timeoutPeriod;
    }
    pendingReconstructions = new HashMap<>();
    timedOutItems = new ArrayList<>();
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
  void increment(BlockInfo block, DatanodeDescriptor... targets) {
    synchronized (pendingReconstructions) {
      PendingBlockInfo found = pendingReconstructions.get(block);
      if (found == null) {
        pendingReconstructions.put(block, new PendingBlockInfo(targets));
      } else {
        found.incrementReplicas(targets);
        found.setTimeStamp();
      }
    }
  }

  /**
   * One reconstruction request for this block has finished.
   * Decrement the number of pending reconstruction requests
   * for this block.
   *
   * @param dn The DataNode that finishes the reconstruction
   * @return true if the block is decremented to 0 and got removed.
   */
  boolean decrement(BlockInfo block, DatanodeDescriptor dn) {
    boolean removed = false;
    synchronized (pendingReconstructions) {
      PendingBlockInfo found = pendingReconstructions.get(block);
      if (found != null) {
        LOG.debug("Removing pending reconstruction for {}", block);
        found.decrementReplicas(dn);
        if (found.getNumReplicas() <= 0) {
          pendingReconstructions.remove(block);
          removed = true;
        }
      }
    }
    return removed;
  }

  /**
   * Remove the record about the given block from pending reconstructions.
   *
   * @param block
   *          The given block whose pending reconstruction requests need to be
   *          removed
   */
  void remove(BlockInfo block) {
    synchronized (pendingReconstructions) {
      pendingReconstructions.remove(block);
    }
  }

  public void clear() {
    synchronized (pendingReconstructions) {
      pendingReconstructions.clear();
      timedOutItems.clear();
      timedOutCount = 0L;
    }
  }

  /**
   * The total number of blocks that are undergoing reconstruction.
   */
  int size() {
    synchronized (pendingReconstructions) {
      return pendingReconstructions.size();
    }
  }

  /**
   * How many copies of this block is pending reconstruction?.
   */
  int getNumReplicas(BlockInfo block) {
    synchronized (pendingReconstructions) {
      PendingBlockInfo found = pendingReconstructions.get(block);
      if (found != null) {
        return found.getNumReplicas();
      }
    }
    return 0;
  }

  /**
   * Used for metrics.
   * @return The number of timeouts
   */
  long getNumTimedOuts() {
    synchronized (timedOutItems) {
      return timedOutCount + timedOutItems.size();
    }
  }

  /**
   * Returns a list of blocks that have timed out their
   * reconstruction requests. Returns null if no blocks have
   * timed out.
   */
  BlockInfo[] getTimedOutBlocks() {
    synchronized (timedOutItems) {
      if (timedOutItems.size() <= 0) {
        return null;
      }
      int size = timedOutItems.size();
      BlockInfo[] blockList = timedOutItems.toArray(
          new BlockInfo[size]);
      timedOutItems.clear();
      timedOutCount += size;
      return blockList;
    }
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
    private final List<DatanodeDescriptor> targets;

    PendingBlockInfo(DatanodeDescriptor[] targets) {
      this.timeStamp = monotonicNow();
      this.targets = targets == null ? new ArrayList<DatanodeDescriptor>()
          : new ArrayList<>(Arrays.asList(targets));
    }

    long getTimeStamp() {
      return timeStamp;
    }

    void setTimeStamp() {
      timeStamp = monotonicNow();
    }

    void incrementReplicas(DatanodeDescriptor... newTargets) {
      if (newTargets != null) {
        for (DatanodeDescriptor newTarget : newTargets) {
          if (!targets.contains(newTarget)) {
            targets.add(newTarget);
          }
        }
      }
    }

    void decrementReplicas(DatanodeDescriptor dn) {
      targets.remove(dn);
    }

    int getNumReplicas() {
      return targets.size();
    }
  }

  /*
   * A periodic thread that scans for blocks that never finished
   * their reconstruction request.
   */
  class PendingReconstructionMonitor implements Runnable {
    @Override
    public void run() {
      while (fsRunning) {
        long period = Math.min(DEFAULT_RECHECK_INTERVAL, timeout);
        try {
          pendingReconstructionCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          LOG.debug("PendingReconstructionMonitor thread is interrupted.", ie);
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    void pendingReconstructionCheck() {
      synchronized (pendingReconstructions) {
        Iterator<Map.Entry<BlockInfo, PendingBlockInfo>> iter =
            pendingReconstructions.entrySet().iterator();
        long now = monotonicNow();
        LOG.debug("PendingReconstructionMonitor checking Q");
        while (iter.hasNext()) {
          Map.Entry<BlockInfo, PendingBlockInfo> entry = iter.next();
          PendingBlockInfo pendingBlock = entry.getValue();
          if (now > pendingBlock.getTimeStamp() + timeout) {
            BlockInfo block = entry.getKey();
            synchronized (timedOutItems) {
              timedOutItems.add(block);
            }
            LOG.warn("PendingReconstructionMonitor timed out " + block);
            NameNode.getNameNodeMetrics().incTimeoutReReplications();
            iter.remove();
          }
        }
      }
    }
  }

  /*
   * Shuts down the pending reconstruction monitor thread.
   * Waits for the thread to exit.
   */
  void stop() {
    fsRunning = false;
    if(timerThread == null) return;
    timerThread.interrupt();
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Iterate through all items and print them.
   */
  void metaSave(PrintWriter out) {
    synchronized (pendingReconstructions) {
      out.println("Metasave: Blocks being reconstructed: " +
                  pendingReconstructions.size());
      for (Map.Entry<BlockInfo, PendingBlockInfo> entry :
          pendingReconstructions.entrySet()) {
        PendingBlockInfo pendingBlock = entry.getValue();
        Block block = entry.getKey();
        out.println(block +
            " StartTime: " + new Time(pendingBlock.timeStamp) +
            " NumReconstructInProgress: " +
            pendingBlock.getNumReplicas());
      }
    }
  }
}
