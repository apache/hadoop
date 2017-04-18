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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult;
import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult.Status;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A monitor class for checking whether block storage movements finished or not.
 * If block storage movement results from datanode indicates about the movement
 * success, then it will just remove the entries from tracking. If it reports
 * failure, then it will add back to needed block storage movements list. If it
 * reports in_progress, that means the blocks movement is in progress and the
 * coordinator is still tracking the movement. If no DN reports about movement
 * for longer time, then such items will be retries automatically after timeout.
 * The default timeout would be 30mins.
 */
public class BlockStorageMovementAttemptedItems {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementAttemptedItems.class);

  /**
   * A map holds the items which are already taken for blocks movements
   * processing and sent to DNs.
   */
  private final Map<Long, ItemInfo> storageMovementAttemptedItems;
  private final List<BlocksStorageMovementResult> storageMovementAttemptedResults;
  private volatile boolean monitorRunning = true;
  private Daemon timerThread = null;
  private final StoragePolicySatisfier sps;
  //
  // It might take anywhere between 20 to 60 minutes before
  // a request is timed out.
  //
  private long selfRetryTimeout = 20 * 60 * 1000;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long minCheckTimeout = 5 * 60 * 1000; // minimum value
  private BlockStorageMovementNeeded blockStorageMovementNeeded;

  public BlockStorageMovementAttemptedItems(long recheckTimeout,
      long selfRetryTimeout,
      BlockStorageMovementNeeded unsatisfiedStorageMovementFiles,
      StoragePolicySatisfier sps) {
    if (recheckTimeout > 0) {
      this.minCheckTimeout = Math.min(minCheckTimeout, recheckTimeout);
    }

    this.selfRetryTimeout = selfRetryTimeout;
    this.blockStorageMovementNeeded = unsatisfiedStorageMovementFiles;
    storageMovementAttemptedItems = new HashMap<>();
    storageMovementAttemptedResults = new ArrayList<>();
    this.sps = sps;
  }

  /**
   * Add item to block storage movement attempted items map which holds the
   * tracking/blockCollection id versus time stamp.
   *
   * @param blockCollectionID
   *          - tracking id / block collection id
   * @param allBlockLocsAttemptedToSatisfy
   *          - failed to find matching target nodes to satisfy storage type for
   *          all the block locations of the given blockCollectionID
   */
  public void add(Long blockCollectionID,
      boolean allBlockLocsAttemptedToSatisfy) {
    synchronized (storageMovementAttemptedItems) {
      ItemInfo itemInfo = new ItemInfo(monotonicNow(),
          allBlockLocsAttemptedToSatisfy);
      storageMovementAttemptedItems.put(blockCollectionID, itemInfo);
    }
  }

  /**
   * Add the trackIDBlocksStorageMovementResults to
   * storageMovementAttemptedResults.
   *
   * @param blksMovementResults
   */
  public void addResults(BlocksStorageMovementResult[] blksMovementResults) {
    if (blksMovementResults.length == 0) {
      return;
    }
    synchronized (storageMovementAttemptedResults) {
      storageMovementAttemptedResults
          .addAll(Arrays.asList(blksMovementResults));
    }
  }

  /**
   * Starts the monitor thread.
   */
  public synchronized void start() {
    monitorRunning = true;
    timerThread = new Daemon(new BlocksStorageMovementAttemptResultMonitor());
    timerThread.setName("BlocksStorageMovementAttemptResultMonitor");
    timerThread.start();
  }

  /**
   * Sets running flag to false. Also, this will interrupt monitor thread and
   * clear all the queued up tasks.
   */
  public synchronized void deactivate() {
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
      deactivate();
    }
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * This class contains information of an attempted trackID. Information such
   * as, (a)last attempted or reported time stamp, (b)whether all the blocks in
   * the trackID were attempted and blocks movement has been scheduled to
   * satisfy storage policy. This is used by
   * {@link BlockStorageMovementAttemptedItems#storageMovementAttemptedItems}.
   */
  private final static class ItemInfo {
    private long lastAttemptedOrReportedTime;
    private final boolean allBlockLocsAttemptedToSatisfy;

    /**
     * ItemInfo constructor.
     *
     * @param lastAttemptedOrReportedTime
     *          last attempted or reported time
     * @param allBlockLocsAttemptedToSatisfy
     *          whether all the blocks in the trackID were attempted and blocks
     *          movement has been scheduled to satisfy storage policy
     */
    private ItemInfo(long lastAttemptedOrReportedTime,
        boolean allBlockLocsAttemptedToSatisfy) {
      this.lastAttemptedOrReportedTime = lastAttemptedOrReportedTime;
      this.allBlockLocsAttemptedToSatisfy = allBlockLocsAttemptedToSatisfy;
    }

    /**
     * @return last attempted or reported time stamp.
     */
    private long getLastAttemptedOrReportedTime() {
      return lastAttemptedOrReportedTime;
    }

    /**
     * @return true/false. True value represents that, all the block locations
     *         under the trackID has found matching target nodes to satisfy
     *         storage policy. False value represents that, trackID needed
     *         retries to satisfy the storage policy for some of the block
     *         locations.
     */
    private boolean isAllBlockLocsAttemptedToSatisfy() {
      return allBlockLocsAttemptedToSatisfy;
    }

    /**
     * Update lastAttemptedOrReportedTime, so that the expiration time will be
     * postponed to future.
     */
    private void touchLastReportedTimeStamp() {
      this.lastAttemptedOrReportedTime = monotonicNow();
    }
  }

  /**
   * A monitor class for checking block storage movement result and long waiting
   * items periodically.
   */
  private class BlocksStorageMovementAttemptResultMonitor implements Runnable {
    @Override
    public void run() {
      while (monitorRunning) {
        try {
          blockStorageMovementResultCheck();
          blocksStorageMovementUnReportedItemsCheck();
          Thread.sleep(minCheckTimeout);
        } catch (InterruptedException ie) {
          LOG.info("BlocksStorageMovementAttemptResultMonitor thread "
              + "is interrupted.", ie);
        } catch (IOException ie) {
          LOG.warn("BlocksStorageMovementAttemptResultMonitor thread "
              + "received exception and exiting.", ie);
        }
      }
    }
  }

  @VisibleForTesting
  void blocksStorageMovementUnReportedItemsCheck() {
    synchronized (storageMovementAttemptedItems) {
      Iterator<Entry<Long, ItemInfo>> iter = storageMovementAttemptedItems
          .entrySet().iterator();
      long now = monotonicNow();
      while (iter.hasNext()) {
        Entry<Long, ItemInfo> entry = iter.next();
        ItemInfo itemInfo = entry.getValue();
        if (now > itemInfo.getLastAttemptedOrReportedTime()
            + selfRetryTimeout) {
          Long blockCollectionID = entry.getKey();
          synchronized (storageMovementAttemptedResults) {
            if (!isExistInResult(blockCollectionID)) {
              blockStorageMovementNeeded.add(blockCollectionID);
              iter.remove();
              LOG.info("TrackID: {} becomes timed out and moved to needed "
                  + "retries queue for next iteration.", blockCollectionID);
            } else {
              LOG.info("Blocks storage movement results for the"
                  + " tracking id : " + blockCollectionID
                  + " is reported from one of the co-ordinating datanode."
                  + " So, the result will be processed soon.");
            }
          }
        }
      }

    }
  }

  private boolean isExistInResult(Long blockCollectionID) {
    Iterator<BlocksStorageMovementResult> iter = storageMovementAttemptedResults
        .iterator();
    while (iter.hasNext()) {
      BlocksStorageMovementResult storageMovementAttemptedResult = iter.next();
      if (storageMovementAttemptedResult.getTrackId() == blockCollectionID) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  void blockStorageMovementResultCheck() throws IOException {
    synchronized (storageMovementAttemptedResults) {
      Iterator<BlocksStorageMovementResult> resultsIter =
          storageMovementAttemptedResults.iterator();
      while (resultsIter.hasNext()) {
        boolean isInprogress = false;
        // TrackID need to be retried in the following cases:
        // 1) All or few scheduled block(s) movement has been failed.
        // 2) All the scheduled block(s) movement has been succeeded but there
        // are unscheduled block(s) movement in this trackID. Say, some of
        // the blocks in the trackID couldn't finding any matching target node
        // for scheduling block movement in previous SPS iteration.
        BlocksStorageMovementResult storageMovementAttemptedResult = resultsIter
            .next();
        synchronized (storageMovementAttemptedItems) {
          Status status = storageMovementAttemptedResult.getStatus();
          ItemInfo itemInfo;
          switch (status) {
          case FAILURE:
            blockStorageMovementNeeded
                .add(storageMovementAttemptedResult.getTrackId());
            LOG.warn("Blocks storage movement results for the tracking id: {}"
                + " is reported from co-ordinating datanode, but result"
                + " status is FAILURE. So, added for retry",
                storageMovementAttemptedResult.getTrackId());
            break;
          case SUCCESS:
            itemInfo = storageMovementAttemptedItems
                .get(storageMovementAttemptedResult.getTrackId());

            // ItemInfo could be null. One case is, before the blocks movements
            // result arrives the attempted trackID became timed out and then
            // removed the trackID from the storageMovementAttemptedItems list.
            // TODO: Need to ensure that trackID is added to the
            // 'blockStorageMovementNeeded' queue for retries to handle the
            // following condition. If all the block locations under the trackID
            // are attempted and failed to find matching target nodes to satisfy
            // storage policy in previous SPS iteration.
            if (itemInfo != null
                && !itemInfo.isAllBlockLocsAttemptedToSatisfy()) {
              blockStorageMovementNeeded
                  .add(storageMovementAttemptedResult.getTrackId());
              LOG.warn("Blocks storage movement is SUCCESS for the track id: {}"
                  + " reported from co-ordinating datanode. But adding trackID"
                  + " back to retry queue as some of the blocks couldn't find"
                  + " matching target nodes in previous SPS iteration.",
                  storageMovementAttemptedResult.getTrackId());
            } else {
              LOG.info("Blocks storage movement is SUCCESS for the track id: {}"
                  + " reported from co-ordinating datanode. But the trackID "
                  + "doesn't exists in storageMovementAttemptedItems list",
                  storageMovementAttemptedResult.getTrackId());
              // Remove xattr for the track id.
              this.sps.notifyBlkStorageMovementFinished(
                  storageMovementAttemptedResult.getTrackId());
            }
            break;
          case IN_PROGRESS:
            isInprogress = true;
            itemInfo = storageMovementAttemptedItems
                .get(storageMovementAttemptedResult.getTrackId());
            if(itemInfo != null){
              // update the attempted expiration time to next cycle.
              itemInfo.touchLastReportedTimeStamp();
            }
            break;
          default:
            LOG.error("Unknown status: {}", status);
            break;
          }
          // Remove trackID from the attempted list if the attempt has been
          // completed(success or failure), if any.
          if (!isInprogress) {
            storageMovementAttemptedItems
                .remove(storageMovementAttemptedResult.getTrackId());
          }
        }
        // Remove trackID from results as processed above.
        resultsIter.remove();
      }
    }
  }

  @VisibleForTesting
  public int resultsCount() {
    return storageMovementAttemptedResults.size();
  }

  @VisibleForTesting
  public int getAttemptedItemsCount() {
    return storageMovementAttemptedItems.size();
  }

  public void clearQueues() {
    storageMovementAttemptedResults.clear();
    storageMovementAttemptedItems.clear();
  }
}
