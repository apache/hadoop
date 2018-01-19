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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.AttemptedItemInfo;
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
public class BlockStorageMovementAttemptedItems
    implements BlockMovementListener {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementAttemptedItems.class);

  /**
   * A map holds the items which are already taken for blocks movements
   * processing and sent to DNs.
   */
  private final List<AttemptedItemInfo> storageMovementAttemptedItems;
  private final List<Block> movementFinishedBlocks;
  private volatile boolean monitorRunning = true;
  private Daemon timerThread = null;
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
      BlockStorageMovementNeeded unsatisfiedStorageMovementFiles) {
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
    movementFinishedBlocks = new ArrayList<>();
  }

  /**
   * Add item to block storage movement attempted items map which holds the
   * tracking/blockCollection id versus time stamp.
   *
   * @param itemInfo
   *          - tracking info
   */
  public void add(AttemptedItemInfo itemInfo) {
    synchronized (storageMovementAttemptedItems) {
      storageMovementAttemptedItems.add(itemInfo);
    }
  }

  /**
   * Add the storage movement attempt finished blocks to
   * storageMovementFinishedBlocks.
   *
   * @param moveAttemptFinishedBlks
   *          storage movement attempt finished blocks
   */
  public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
    if (moveAttemptFinishedBlks.length == 0) {
      return;
    }
    synchronized (movementFinishedBlocks) {
      movementFinishedBlocks.addAll(Arrays.asList(moveAttemptFinishedBlks));
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
          Long blockCollectionID = itemInfo.getFileId();
          synchronized (movementFinishedBlocks) {
            ItemInfo candidate = new ItemInfo(itemInfo.getStartId(),
                blockCollectionID, itemInfo.getRetryCount() + 1);
            blockStorageMovementNeeded.add(candidate);
            iter.remove();
            LOG.info("TrackID: {} becomes timed out and moved to needed "
                + "retries queue for next iteration.", blockCollectionID);
          }
        }
      }

    }
  }

  @VisibleForTesting
  void blockStorageMovementReportedItemsCheck() throws IOException {
    synchronized (movementFinishedBlocks) {
      Iterator<Block> finishedBlksIter = movementFinishedBlocks.iterator();
      while (finishedBlksIter.hasNext()) {
        Block blk = finishedBlksIter.next();
        synchronized (storageMovementAttemptedItems) {
          Iterator<AttemptedItemInfo> iterator = storageMovementAttemptedItems
              .iterator();
          while (iterator.hasNext()) {
            AttemptedItemInfo attemptedItemInfo = iterator.next();
            attemptedItemInfo.getBlocks().remove(blk);
            if (attemptedItemInfo.getBlocks().isEmpty()) {
              // TODO: try add this at front of the Queue, so that this element
              // gets the chance first and can be cleaned from queue quickly as
              // all movements already done.
              blockStorageMovementNeeded.add(new ItemInfo(attemptedItemInfo
                  .getStartId(), attemptedItemInfo.getFileId(),
                  attemptedItemInfo.getRetryCount() + 1));
              iterator.remove();
            }
          }
        }
        // Remove attempted blocks from movementFinishedBlocks list.
        finishedBlksIter.remove();
      }
    }
  }

  @VisibleForTesting
  public int getMovementFinishedBlocksCount() {
    return movementFinishedBlocks.size();
  }

  @VisibleForTesting
  public int getAttemptedItemsCount() {
    return storageMovementAttemptedItems.size();
  }

  public void clearQueues() {
    synchronized (movementFinishedBlocks) {
      movementFinishedBlocks.clear();
    }
    synchronized (storageMovementAttemptedItems) {
      storageMovementAttemptedItems.clear();
    }
  }
}
