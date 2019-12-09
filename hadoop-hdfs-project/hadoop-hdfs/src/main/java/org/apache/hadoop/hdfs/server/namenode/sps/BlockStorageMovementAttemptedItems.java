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
package org.apache.hadoop.hdfs.server.namenode.sps;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.AttemptedItemInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.StorageTypeNodePair;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A monitor class for checking whether block storage movements attempt
 * completed or not. If this receives block storage movement attempt
 * status(either success or failure) from DN then it will just remove the
 * entries from tracking. If there is no DN reports about movement attempt
 * finished for a longer time period, then such items will retries automatically
 * after timeout. The default timeout would be 5 minutes.
 */
public class BlockStorageMovementAttemptedItems {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementAttemptedItems.class);

  /**
   * A map holds the items which are already taken for blocks movements
   * processing and sent to DNs.
   */
  private final List<AttemptedItemInfo> storageMovementAttemptedItems;
  private Map<Block, Set<StorageTypeNodePair>> scheduledBlkLocs;
  // Maintains separate Queue to keep the movement finished blocks. This Q
  // is used to update the storageMovementAttemptedItems list asynchronously.
  private final BlockingQueue<Block> movementFinishedBlocks;
  private volatile boolean monitorRunning = true;
  private Daemon timerThread = null;
  private final Context context;
  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long selfRetryTimeout = 5 * 60 * 1000;

  //
  // It might take anywhere between 1 to 2 minutes before
  // a request is timed out.
  //
  private long minCheckTimeout = 1 * 60 * 1000; // minimum value
  private BlockStorageMovementNeeded blockStorageMovementNeeded;
  private final SPSService service;

  public BlockStorageMovementAttemptedItems(SPSService service,
      BlockStorageMovementNeeded unsatisfiedStorageMovementFiles,
      Context context) {
    this.service = service;
    long recheckTimeout = this.service.getConf().getLong(
        DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
        DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT);
    if (recheckTimeout > 0) {
      this.minCheckTimeout = Math.min(minCheckTimeout, recheckTimeout);
    }

    this.selfRetryTimeout = this.service.getConf().getLong(
        DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
        DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT);
    this.blockStorageMovementNeeded = unsatisfiedStorageMovementFiles;
    storageMovementAttemptedItems = new ArrayList<>();
    scheduledBlkLocs = new HashMap<>();
    movementFinishedBlocks = new LinkedBlockingQueue<>();
    this.context = context;
  }

  /**
   * Add item to block storage movement attempted items map which holds the
   * tracking/blockCollection id versus time stamp.
   *
   * @param startPathId
   *          - start satisfier path identifier
   * @param fileId
   *          - file identifier
   * @param monotonicNow
   *          - time now
   * @param assignedBlocks
   *          - assigned blocks for block movement
   * @param retryCount
   *          - retry count
   */
  public void add(long startPathId, long fileId, long monotonicNow,
      Map<Block, Set<StorageTypeNodePair>> assignedBlocks, int retryCount) {
    AttemptedItemInfo itemInfo = new AttemptedItemInfo(startPathId, fileId,
        monotonicNow, assignedBlocks.keySet(), retryCount);
    synchronized (storageMovementAttemptedItems) {
      storageMovementAttemptedItems.add(itemInfo);
    }
    synchronized (scheduledBlkLocs) {
      scheduledBlkLocs.putAll(assignedBlocks);
    }
  }

  /**
   * Notify the storage movement attempt finished block.
   *
   * @param reportedDn
   *          reported datanode
   * @param type
   *          storage type
   * @param reportedBlock
   *          reported block
   */
  public void notifyReportedBlock(DatanodeInfo reportedDn, StorageType type,
      Block reportedBlock) {
    synchronized (scheduledBlkLocs) {
      if (scheduledBlkLocs.size() <= 0) {
        return;
      }
      matchesReportedBlock(reportedDn, type, reportedBlock);
    }
  }

  private void matchesReportedBlock(DatanodeInfo reportedDn, StorageType type,
      Block reportedBlock) {
    Set<StorageTypeNodePair> blkLocs = scheduledBlkLocs.get(reportedBlock);
    if (blkLocs == null) {
      return; // unknown block, simply skip.
    }

    for (StorageTypeNodePair dn : blkLocs) {
      boolean foundDn = dn.getDatanodeInfo().compareTo(reportedDn) == 0 ? true
          : false;
      boolean foundType = dn.getStorageType().equals(type);
      if (foundDn && foundType) {
        blkLocs.remove(dn);
        Block[] mFinishedBlocks = new Block[1];
        mFinishedBlocks[0] = reportedBlock;
        context.notifyMovementTriedBlocks(mFinishedBlocks);
        // All the block locations has reported.
        if (blkLocs.size() <= 0) {
          movementFinishedBlocks.add(reportedBlock);
          scheduledBlkLocs.remove(reportedBlock); // clean-up reported block
        }
        return; // found
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reported block:{} not found in attempted blocks. Datanode:{}"
          + ", StorageType:{}", reportedBlock, reportedDn, type);
    }
  }

  /**
   * Starts the monitor thread.
   */
  public synchronized void start() {
    monitorRunning = true;
    timerThread = new Daemon(new BlocksStorageMovementAttemptMonitor());
    timerThread.setName("BlocksStorageMovementAttemptMonitor");
    timerThread.start();
  }

  /**
   * Sets running flag to false. Also, this will interrupt monitor thread and
   * clear all the queued up tasks.
   */
  public synchronized void stop() {
    monitorRunning = false;
    if (timerThread != null) {
      timerThread.interrupt();
    }
    this.clearQueues();
  }

  /**
   * Timed wait to stop monitor thread.
   */
  synchronized void stopGracefully() {
    if (timerThread == null) {
      return;
    }
    if (monitorRunning) {
      stop();
    }
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * A monitor class for checking block storage movement attempt status and long
   * waiting items periodically.
   */
  private class BlocksStorageMovementAttemptMonitor implements Runnable {
    @Override
    public void run() {
      while (monitorRunning) {
        try {
          blockStorageMovementReportedItemsCheck();
          blocksStorageMovementUnReportedItemsCheck();
          Thread.sleep(minCheckTimeout);
        } catch (InterruptedException ie) {
          LOG.info("BlocksStorageMovementAttemptMonitor thread "
              + "is interrupted.", ie);
        } catch (IOException ie) {
          LOG.warn("BlocksStorageMovementAttemptMonitor thread "
              + "received exception and exiting.", ie);
        }
      }
    }
  }

  @VisibleForTesting
  void blocksStorageMovementUnReportedItemsCheck() {
    synchronized (storageMovementAttemptedItems) {
      Iterator<AttemptedItemInfo> iter = storageMovementAttemptedItems
          .iterator();
      long now = monotonicNow();
      while (iter.hasNext()) {
        AttemptedItemInfo itemInfo = iter.next();
        if (now > itemInfo.getLastAttemptedOrReportedTime()
            + selfRetryTimeout) {
          long file = itemInfo.getFile();
          ItemInfo candidate = new ItemInfo(itemInfo.getStartPath(), file,
              itemInfo.getRetryCount() + 1);
          blockStorageMovementNeeded.add(candidate);
          iter.remove();
          LOG.info("TrackID: {} becomes timed out and moved to needed "
              + "retries queue for next iteration.", file);
        }
      }
    }
  }

  @VisibleForTesting
  void blockStorageMovementReportedItemsCheck() throws IOException {
    // Removes all available blocks from this queue and process it.
    Collection<Block> finishedBlks = new ArrayList<>();
    movementFinishedBlocks.drainTo(finishedBlks);

    // Update attempted items list
    for (Block blk : finishedBlks) {
      synchronized (storageMovementAttemptedItems) {
        Iterator<AttemptedItemInfo> iterator = storageMovementAttemptedItems
            .iterator();
        while (iterator.hasNext()) {
          AttemptedItemInfo attemptedItemInfo = iterator.next();
          attemptedItemInfo.getBlocks().remove(blk);
          if (attemptedItemInfo.getBlocks().isEmpty()) {
            blockStorageMovementNeeded.add(new ItemInfo(
                attemptedItemInfo.getStartPath(), attemptedItemInfo.getFile(),
                attemptedItemInfo.getRetryCount() + 1));
            iterator.remove();
          }
        }
      }
    }
  }

  @VisibleForTesting
  public int getMovementFinishedBlocksCount() {
    return movementFinishedBlocks.size();
  }

  @VisibleForTesting
  public int getAttemptedItemsCount() {
    synchronized (storageMovementAttemptedItems) {
      return storageMovementAttemptedItems.size();
    }
  }

  public void clearQueues() {
    movementFinishedBlocks.clear();
    synchronized (storageMovementAttemptedItems) {
      storageMovementAttemptedItems.clear();
    }
    synchronized (scheduledBlkLocs) {
      scheduledBlkLocs.clear();
    }
  }
}
