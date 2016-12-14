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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A monitor class for checking whether block storage movements finished or not.
 * If block storage movement results from datanode indicates about the movement
 * success, then it will just remove the entries from tracking. If it reports
 * failure, then it will add back to needed block storage movements list. If no
 * DN reports about movement for longer time, then such items will be retries
 * automatically after timeout. The default timeout would be 30mins.
 */
public class BlockStorageMovementAttemptedItems {
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementAttemptedItems.class);
  // A map holds the items which are already taken for blocks movements
  // processing and sent to DNs.
  private final Map<Long, Long> storageMovementAttemptedItems;
  private final List<BlocksStorageMovementResult> storageMovementAttemptedResults;
  private volatile boolean monitorRunning = true;
  private Daemon timerThread = null;
  //
  // It might take anywhere between 30 to 60 minutes before
  // a request is timed out.
  //
  private long selfRetryTimeout = 30 * 60 * 1000;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long checkTimeout = 5 * 60 * 1000; // minimum value
  private BlockStorageMovementNeeded blockStorageMovementNeeded;

  public BlockStorageMovementAttemptedItems(long timeoutPeriod,
      long selfRetryTimeout,
      BlockStorageMovementNeeded unsatisfiedStorageMovementFiles) {
    if (timeoutPeriod > 0) {
      this.checkTimeout = Math.min(checkTimeout, timeoutPeriod);
    }

    this.selfRetryTimeout = selfRetryTimeout;
    this.blockStorageMovementNeeded = unsatisfiedStorageMovementFiles;
    storageMovementAttemptedItems = new HashMap<>();
    storageMovementAttemptedResults = new ArrayList<>();
  }

  /**
   * Add item to block storage movement attempted items map which holds the
   * tracking/blockCollection id versus time stamp.
   *
   * @param blockCollectionID
   *          - tracking id / block collection id
   */
  public void add(Long blockCollectionID) {
    synchronized (storageMovementAttemptedItems) {
      storageMovementAttemptedItems.put(blockCollectionID, monotonicNow());
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
   * Stops the monitor thread.
   */
  public synchronized void stop() {
    monitorRunning = false;
    timerThread.interrupt();
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
    this.clearQueues();
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
          Thread.sleep(checkTimeout);
        } catch (InterruptedException ie) {
          LOG.info("BlocksStorageMovementAttemptResultMonitor thread "
              + "is interrupted.", ie);
        }
      }
    }

    private void blocksStorageMovementUnReportedItemsCheck() {
      synchronized (storageMovementAttemptedItems) {
        Iterator<Entry<Long, Long>> iter =
            storageMovementAttemptedItems.entrySet().iterator();
        long now = monotonicNow();
        while (iter.hasNext()) {
          Entry<Long, Long> entry = iter.next();
          if (now > entry.getValue() + selfRetryTimeout) {
            Long blockCollectionID = entry.getKey();
            synchronized (storageMovementAttemptedResults) {
              boolean exist = isExistInResult(blockCollectionID);
              if (!exist) {
                blockStorageMovementNeeded.add(blockCollectionID);
              } else {
                LOG.info("Blocks storage movement results for the"
                    + " tracking id : " + blockCollectionID
                    + " is reported from one of the co-ordinating datanode."
                    + " So, the result will be processed soon.");
              }
              iter.remove();
            }
          }
        }

      }
    }

    private boolean isExistInResult(Long blockCollectionID) {
      Iterator<BlocksStorageMovementResult> iter =
          storageMovementAttemptedResults.iterator();
      while (iter.hasNext()) {
        BlocksStorageMovementResult storageMovementAttemptedResult =
            iter.next();
        if (storageMovementAttemptedResult.getTrackId() == blockCollectionID) {
          return true;
        }
      }
      return false;
    }

    private void blockStorageMovementResultCheck() {
      synchronized (storageMovementAttemptedResults) {
        Iterator<BlocksStorageMovementResult> iter =
            storageMovementAttemptedResults.iterator();
        while (iter.hasNext()) {
          BlocksStorageMovementResult storageMovementAttemptedResult =
              iter.next();
          if (storageMovementAttemptedResult
              .getStatus() == BlocksStorageMovementResult.Status.FAILURE) {
            blockStorageMovementNeeded
                .add(storageMovementAttemptedResult.getTrackId());
            LOG.warn("Blocks storage movement results for the tracking id : "
                + storageMovementAttemptedResult.getTrackId()
                + " is reported from co-ordinating datanode, but result"
                + " status is FAILURE. So, added for retry");
          } else {
            synchronized (storageMovementAttemptedItems) {
              storageMovementAttemptedItems
                  .remove(storageMovementAttemptedResult.getTrackId());
            }
            LOG.info("Blocks storage movement results for the tracking id : "
                + storageMovementAttemptedResult.getTrackId()
                + " is reported from co-ordinating datanode. "
                + "The result status is SUCCESS.");
          }
          iter.remove(); // remove from results as processed above
        }
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
