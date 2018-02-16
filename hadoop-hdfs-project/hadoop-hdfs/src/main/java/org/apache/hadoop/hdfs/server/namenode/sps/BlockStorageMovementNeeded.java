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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfyPathStatus;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A Class to track the block collection IDs (Inode's ID) for which physical
 * storage movement needed as per the Namespace and StorageReports from DN.
 * It scan the pending directories for which storage movement is required and
 * schedule the block collection IDs for movement. It track the info of
 * scheduled items and remove the SPS xAttr from the file/Directory once
 * movement is success.
 *
 * @param <T>
 *          is identifier of inode or full path name of inode. Internal sps will
 *          use the file inodeId for the block movement. External sps will use
 *          file string path representation for the block movement.
 */
@InterfaceAudience.Private
public class BlockStorageMovementNeeded<T> {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementNeeded.class);

  private final Queue<ItemInfo<T>> storageMovementNeeded =
      new LinkedList<ItemInfo<T>>();

  /**
   * Map of startPath and number of child's. Number of child's indicate the
   * number of files pending to satisfy the policy.
   */
  private final Map<T, DirPendingWorkInfo> pendingWorkForDirectory =
      new HashMap<>();

  private final Map<T, StoragePolicySatisfyPathStatusInfo> spsStatus =
      new ConcurrentHashMap<>();

  private final Context<T> ctxt;

  private Daemon pathIdCollector;

  private FileCollector<T> fileCollector;

  private SPSPathIdProcessor pathIDProcessor;

  // Amount of time to cache the SUCCESS status of path before turning it to
  // NOT_AVAILABLE.
  private static long statusClearanceElapsedTimeMs = 300000;

  public BlockStorageMovementNeeded(Context<T> context,
      FileCollector<T> fileCollector) {
    this.ctxt = context;
    this.fileCollector = fileCollector;
    pathIDProcessor = new SPSPathIdProcessor();
  }

  /**
   * Add the candidate to tracking list for which storage movement
   * expected if necessary.
   *
   * @param trackInfo
   *          - track info for satisfy the policy
   */
  public synchronized void add(ItemInfo<T> trackInfo) {
    spsStatus.put(trackInfo.getFile(),
        new StoragePolicySatisfyPathStatusInfo(
            StoragePolicySatisfyPathStatus.IN_PROGRESS));
    storageMovementNeeded.add(trackInfo);
  }

  /**
   * Add the itemInfo list to tracking list for which storage movement expected
   * if necessary.
   *
   * @param startPath
   *          - start path
   * @param itemInfoList
   *          - List of child in the directory
   * @param scanCompleted
   *          -Indicates whether the start id directory has no more elements to
   *          scan.
   */
  @VisibleForTesting
  public synchronized void addAll(T startPath, List<ItemInfo<T>> itemInfoList,
      boolean scanCompleted) {
    storageMovementNeeded.addAll(itemInfoList);
    updatePendingDirScanStats(startPath, itemInfoList.size(), scanCompleted);
  }

  /**
   * Add the itemInfo to tracking list for which storage movement expected if
   * necessary.
   *
   * @param itemInfoList
   *          - List of child in the directory
   * @param scanCompleted
   *          -Indicates whether the ItemInfo start id directory has no more
   *          elements to scan.
   */
  @VisibleForTesting
  public synchronized void add(ItemInfo<T> itemInfo, boolean scanCompleted) {
    storageMovementNeeded.add(itemInfo);
    // This represents sps start id is file, so no need to update pending dir
    // stats.
    if (itemInfo.getStartPath() == itemInfo.getFile()) {
      return;
    }
    updatePendingDirScanStats(itemInfo.getFile(), 1, scanCompleted);
  }

  private void updatePendingDirScanStats(T startPath, int numScannedFiles,
      boolean scanCompleted) {
    DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(startPath);
    if (pendingWork == null) {
      pendingWork = new DirPendingWorkInfo();
      pendingWorkForDirectory.put(startPath, pendingWork);
    }
    pendingWork.addPendingWorkCount(numScannedFiles);
    if (scanCompleted) {
      pendingWork.markScanCompleted();
    }
  }

  /**
   * Gets the satisfier files for which block storage movements check necessary
   * and make the movement if required.
   *
   * @return satisfier files
   */
  public synchronized ItemInfo<T> get() {
    return storageMovementNeeded.poll();
  }

  /**
   * Returns queue size.
   */
  public synchronized int size() {
    return storageMovementNeeded.size();
  }

  public synchronized void clearAll() {
    ctxt.removeAllSPSPathIds();
    storageMovementNeeded.clear();
    pendingWorkForDirectory.clear();
  }

  /**
   * Decrease the pending child count for directory once one file blocks moved
   * successfully. Remove the SPS xAttr if pending child count is zero.
   */
  public synchronized void removeItemTrackInfo(ItemInfo<T> trackInfo,
      boolean isSuccess) throws IOException {
    if (trackInfo.isDir()) {
      // If track is part of some start inode then reduce the pending
      // directory work count.
      T startId = trackInfo.getStartPath();
      if (!ctxt.isFileExist(startId)) {
        // directory deleted just remove it.
        this.pendingWorkForDirectory.remove(startId);
        updateStatus(startId, isSuccess);
      } else {
        DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(startId);
        if (pendingWork != null) {
          pendingWork.decrementPendingWorkCount();
          if (pendingWork.isDirWorkDone()) {
            ctxt.removeSPSHint(startId);
            pendingWorkForDirectory.remove(startId);
            pendingWork.setFailure(!isSuccess);
            updateStatus(startId, pendingWork.isPolicySatisfied());
          }
          pendingWork.setFailure(isSuccess);
        }
      }
    } else {
      // Remove xAttr if trackID doesn't exist in
      // storageMovementAttemptedItems or file policy satisfied.
      ctxt.removeSPSHint(trackInfo.getFile());
      updateStatus(trackInfo.getFile(), isSuccess);
    }
  }

  public synchronized void clearQueue(T trackId) {
    ctxt.removeSPSPathId(trackId);
    Iterator<ItemInfo<T>> iterator = storageMovementNeeded.iterator();
    while (iterator.hasNext()) {
      ItemInfo<T> next = iterator.next();
      if (next.getFile() == trackId) {
        iterator.remove();
      }
    }
    pendingWorkForDirectory.remove(trackId);
  }

  /**
   * Mark inode status as SUCCESS in map.
   */
  private void updateStatus(T startId, boolean isSuccess){
    StoragePolicySatisfyPathStatusInfo spsStatusInfo =
        spsStatus.get(startId);
    if (spsStatusInfo == null) {
      spsStatusInfo = new StoragePolicySatisfyPathStatusInfo();
      spsStatus.put(startId, spsStatusInfo);
    }

    if (isSuccess) {
      spsStatusInfo.setSuccess();
    } else {
      spsStatusInfo.setFailure();
    }
  }

  /**
   * Clean all the movements in spsDirsToBeTraveresed/storageMovementNeeded
   * and notify to clean up required resources.
   * @throws IOException
   */
  public synchronized void clearQueuesWithNotification() {
    // Remove xAttr from directories
    T trackId;
    while ((trackId = ctxt.getNextSPSPath()) != null) {
      try {
        // Remove xAttr for file
        ctxt.removeSPSHint(trackId);
      } catch (IOException ie) {
        LOG.warn("Failed to remove SPS xattr for track id " + trackId, ie);
      }
    }

    // File's directly added to storageMovementNeeded, So try to remove
    // xAttr for file
    ItemInfo<T> itemInfo;
    while ((itemInfo = get()) != null) {
      try {
        // Remove xAttr for file
        if (!itemInfo.isDir()) {
          ctxt.removeSPSHint(itemInfo.getFile());
        }
      } catch (IOException ie) {
        LOG.warn(
            "Failed to remove SPS xattr for track id "
                + itemInfo.getFile(), ie);
      }
    }
    this.clearAll();
  }

  /**
   * Take dir tack ID from the spsDirsToBeTraveresed queue and collect child
   * ID's to process for satisfy the policy.
   */
  private class SPSPathIdProcessor implements Runnable {

    @Override
    public void run() {
      LOG.info("Starting SPSPathIdProcessor!.");
      long lastStatusCleanTime = 0;
      T startINode = null;
      while (ctxt.isRunning()) {
        try {
          if (!ctxt.isInSafeMode()) {
            if (startINode == null) {
              startINode = ctxt.getNextSPSPath();
            } // else same id will be retried
            if (startINode == null) {
              // Waiting for SPS path
              Thread.sleep(3000);
            } else {
              spsStatus.put(startINode,
                  new StoragePolicySatisfyPathStatusInfo(
                      StoragePolicySatisfyPathStatus.IN_PROGRESS));
              fileCollector.scanAndCollectFiles(startINode);
              // check if directory was empty and no child added to queue
              DirPendingWorkInfo dirPendingWorkInfo =
                  pendingWorkForDirectory.get(startINode);
              if (dirPendingWorkInfo != null
                  && dirPendingWorkInfo.isDirWorkDone()) {
                ctxt.removeSPSHint(startINode);
                pendingWorkForDirectory.remove(startINode);
                updateStatus(startINode, true);
              }
            }
            //Clear the SPS status if status is in SUCCESS more than 5 min.
            if (Time.monotonicNow()
                - lastStatusCleanTime > statusClearanceElapsedTimeMs) {
              lastStatusCleanTime = Time.monotonicNow();
              cleanSPSStatus();
            }
            startINode = null; // Current inode successfully scanned.
          }
        } catch (Throwable t) {
          String reClass = t.getClass().getName();
          if (InterruptedException.class.getName().equals(reClass)) {
            LOG.info("SPSPathIdProcessor thread is interrupted. Stopping..");
            break;
          }
          LOG.warn("Exception while scanning file inodes to satisfy the policy",
              t);
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            LOG.info("Interrupted while waiting in SPSPathIdProcessor", t);
            break;
          }
        }
      }
    }

    private synchronized void cleanSPSStatus() {
      for (Iterator<Entry<T, StoragePolicySatisfyPathStatusInfo>> it = spsStatus
          .entrySet().iterator(); it.hasNext();) {
        Entry<T, StoragePolicySatisfyPathStatusInfo> entry = it.next();
        if (entry.getValue().canRemove()) {
          it.remove();
        }
      }
    }
  }

  /**
   * Info for directory recursive scan.
   */
  public static class DirPendingWorkInfo {

    private int pendingWorkCount = 0;
    private boolean fullyScanned = false;
    private boolean success = true;

    /**
     * Increment the pending work count for directory.
     */
    public synchronized void addPendingWorkCount(int count) {
      this.pendingWorkCount = this.pendingWorkCount + count;
    }

    /**
     * Decrement the pending work count for directory one track info is
     * completed.
     */
    public synchronized void decrementPendingWorkCount() {
      this.pendingWorkCount--;
    }

    /**
     * Return true if all the pending work is done and directory fully
     * scanned, otherwise false.
     */
    public synchronized boolean isDirWorkDone() {
      return (pendingWorkCount <= 0 && fullyScanned);
    }

    /**
     * Mark directory scan is completed.
     */
    public synchronized void markScanCompleted() {
      this.fullyScanned = true;
    }

    /**
     * Return true if all the files block movement is success, otherwise false.
     */
    public boolean isPolicySatisfied() {
      return success;
    }

    /**
     * Set directory SPS status failed.
     */
    public void setFailure(boolean failure) {
      this.success = this.success || failure;
    }
  }

  public void activate() {
    pathIdCollector = new Daemon(pathIDProcessor);
    pathIdCollector.setName("SPSPathIdProcessor");
    pathIdCollector.start();
  }

  public void close() {
    if (pathIdCollector != null) {
      pathIdCollector.interrupt();
    }
  }

  /**
   * Represent the file/directory block movement status.
   */
  static class StoragePolicySatisfyPathStatusInfo {
    private StoragePolicySatisfyPathStatus status =
        StoragePolicySatisfyPathStatus.NOT_AVAILABLE;
    private long lastStatusUpdateTime;

    StoragePolicySatisfyPathStatusInfo() {
      this.lastStatusUpdateTime = 0;
    }

    StoragePolicySatisfyPathStatusInfo(StoragePolicySatisfyPathStatus status) {
      this.status = status;
      this.lastStatusUpdateTime = 0;
    }

    private void setSuccess() {
      this.status = StoragePolicySatisfyPathStatus.SUCCESS;
      this.lastStatusUpdateTime = Time.monotonicNow();
    }

    private void setFailure() {
      this.status = StoragePolicySatisfyPathStatus.FAILURE;
      this.lastStatusUpdateTime = Time.monotonicNow();
    }

    private StoragePolicySatisfyPathStatus getStatus() {
      return status;
    }

    /**
     * Return true if SUCCESS status cached more then 5 min.
     */
    private boolean canRemove() {
      return (StoragePolicySatisfyPathStatus.SUCCESS == status
          || StoragePolicySatisfyPathStatus.FAILURE == status)
          && (Time.monotonicNow()
              - lastStatusUpdateTime) > statusClearanceElapsedTimeMs;
    }
  }

  public StoragePolicySatisfyPathStatus getStatus(long id) {
    StoragePolicySatisfyPathStatusInfo spsStatusInfo = spsStatus.get(id);
    if(spsStatusInfo == null){
      return StoragePolicySatisfyPathStatus.NOT_AVAILABLE;
    }
    return spsStatusInfo.getStatus();
  }

  @VisibleForTesting
  public static void setStatusClearanceElapsedTimeMs(
      long statusClearanceElapsedTimeMs) {
    BlockStorageMovementNeeded.statusClearanceElapsedTimeMs =
        statusClearanceElapsedTimeMs;
  }

  @VisibleForTesting
  public static long getStatusClearanceElapsedTimeMs() {
    return statusClearanceElapsedTimeMs;
  }

  public void markScanCompletedForDir(T inode) {
    DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(inode);
    if (pendingWork != null) {
      pendingWork.markScanCompleted();
    }
  }
}
