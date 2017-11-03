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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSTreeTraverser.TraverseInfo;
import org.apache.hadoop.hdfs.server.namenode.StoragePolicySatisfier.ItemInfo;
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
 */
@InterfaceAudience.Private
public class BlockStorageMovementNeeded {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementNeeded.class);

  private final Queue<ItemInfo> storageMovementNeeded =
      new LinkedList<ItemInfo>();

  /**
   * Map of startId and number of child's. Number of child's indicate the
   * number of files pending to satisfy the policy.
   */
  private final Map<Long, DirPendingWorkInfo> pendingWorkForDirectory =
      new HashMap<Long, DirPendingWorkInfo>();

  private final Map<Long, StoragePolicySatisfyPathStatusInfo> spsStatus =
      new ConcurrentHashMap<>();

  private final Namesystem namesystem;

  // List of pending dir to satisfy the policy
  private final Queue<Long> spsDirsToBeTraveresed = new LinkedList<Long>();

  private final StoragePolicySatisfier sps;

  private Daemon inodeIdCollector;

  private final int maxQueuedItem;

  // Amount of time to cache the SUCCESS status of path before turning it to
  // NOT_AVAILABLE.
  private static long statusClearanceElapsedTimeMs = 300000;

  public BlockStorageMovementNeeded(Namesystem namesystem,
      StoragePolicySatisfier sps, int queueLimit) {
    this.namesystem = namesystem;
    this.sps = sps;
    this.maxQueuedItem = queueLimit;
  }

  /**
   * Add the candidate to tracking list for which storage movement
   * expected if necessary.
   *
   * @param trackInfo
   *          - track info for satisfy the policy
   */
  public synchronized void add(ItemInfo trackInfo) {
    spsStatus.put(trackInfo.getStartId(),
        new StoragePolicySatisfyPathStatusInfo(
            StoragePolicySatisfyPathStatus.IN_PROGRESS));
    storageMovementNeeded.add(trackInfo);
  }

  /**
   * Add the itemInfo to tracking list for which storage movement
   * expected if necessary.
   * @param startId
   *            - start id
   * @param itemInfoList
   *            - List of child in the directory
   */
  @VisibleForTesting
  public synchronized void addAll(long startId,
      List<ItemInfo> itemInfoList, boolean scanCompleted) {
    storageMovementNeeded.addAll(itemInfoList);
    DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(startId);
    if (pendingWork == null) {
      pendingWork = new DirPendingWorkInfo();
      pendingWorkForDirectory.put(startId, pendingWork);
    }
    pendingWork.addPendingWorkCount(itemInfoList.size());
    if (scanCompleted) {
      pendingWork.markScanCompleted();
    }
  }

  /**
   * Gets the block collection id for which storage movements check necessary
   * and make the movement if required.
   *
   * @return block collection ID
   */
  public synchronized ItemInfo get() {
    return storageMovementNeeded.poll();
  }

  public synchronized void addToPendingDirQueue(long id) {
    spsStatus.put(id, new StoragePolicySatisfyPathStatusInfo(
        StoragePolicySatisfyPathStatus.PENDING));
    spsDirsToBeTraveresed.add(id);
    // Notify waiting FileInodeIdCollector thread about the newly
    // added SPS path.
    synchronized (spsDirsToBeTraveresed) {
      spsDirsToBeTraveresed.notify();
    }
  }

  /**
   * Returns queue remaining capacity.
   */
  public synchronized int remainingCapacity() {
    int size = storageMovementNeeded.size();
    if (size >= maxQueuedItem) {
      return 0;
    } else {
      return (maxQueuedItem - size);
    }
  }

  /**
   * Returns queue size.
   */
  public synchronized int size() {
    return storageMovementNeeded.size();
  }

  public synchronized void clearAll() {
    spsDirsToBeTraveresed.clear();
    storageMovementNeeded.clear();
    pendingWorkForDirectory.clear();
  }

  /**
   * Decrease the pending child count for directory once one file blocks moved
   * successfully. Remove the SPS xAttr if pending child count is zero.
   */
  public synchronized void removeItemTrackInfo(ItemInfo trackInfo)
      throws IOException {
    if (trackInfo.isDir()) {
      // If track is part of some start inode then reduce the pending
      // directory work count.
      long startId = trackInfo.getStartId();
      INode inode = namesystem.getFSDirectory().getInode(startId);
      if (inode == null) {
        // directory deleted just remove it.
        this.pendingWorkForDirectory.remove(startId);
        markSuccess(startId);
      } else {
        DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(startId);
        if (pendingWork != null) {
          pendingWork.decrementPendingWorkCount();
          if (pendingWork.isDirWorkDone()) {
            namesystem.removeXattr(startId, XATTR_SATISFY_STORAGE_POLICY);
            pendingWorkForDirectory.remove(startId);
            markSuccess(startId);
          }
        }
      }
    } else {
      // Remove xAttr if trackID doesn't exist in
      // storageMovementAttemptedItems or file policy satisfied.
      namesystem.removeXattr(trackInfo.getTrackId(),
          XATTR_SATISFY_STORAGE_POLICY);
      markSuccess(trackInfo.getStartId());
    }
  }

  public synchronized void clearQueue(long trackId) {
    spsDirsToBeTraveresed.remove(trackId);
    Iterator<ItemInfo> iterator = storageMovementNeeded.iterator();
    while (iterator.hasNext()) {
      ItemInfo next = iterator.next();
      if (next.getStartId() == trackId) {
        iterator.remove();
      }
    }
    pendingWorkForDirectory.remove(trackId);
  }

  /**
   * Mark inode status as SUCCESS in map.
   */
  private void markSuccess(long startId){
    StoragePolicySatisfyPathStatusInfo spsStatusInfo =
        spsStatus.get(startId);
    if (spsStatusInfo == null) {
      spsStatusInfo = new StoragePolicySatisfyPathStatusInfo();
      spsStatus.put(startId, spsStatusInfo);
    }
    spsStatusInfo.setSuccess();
  }

  /**
   * Clean all the movements in spsDirsToBeTraveresed/storageMovementNeeded
   * and notify to clean up required resources.
   * @throws IOException
   */
  public synchronized void clearQueuesWithNotification() {
    // Remove xAttr from directories
    Long trackId;
    while ((trackId = spsDirsToBeTraveresed.poll()) != null) {
      try {
        // Remove xAttr for file
        namesystem.removeXattr(trackId, XATTR_SATISFY_STORAGE_POLICY);
      } catch (IOException ie) {
        LOG.warn("Failed to remove SPS xattr for track id " + trackId, ie);
      }
    }

    // File's directly added to storageMovementNeeded, So try to remove
    // xAttr for file
    ItemInfo itemInfo;
    while ((itemInfo = storageMovementNeeded.poll()) != null) {
      try {
        // Remove xAttr for file
        if (!itemInfo.isDir()) {
          namesystem.removeXattr(itemInfo.getTrackId(),
              XATTR_SATISFY_STORAGE_POLICY);
        }
      } catch (IOException ie) {
        LOG.warn(
            "Failed to remove SPS xattr for track id "
                + itemInfo.getTrackId(), ie);
      }
    }
    this.clearAll();
  }

  /**
   * Take dir tack ID from the spsDirsToBeTraveresed queue and collect child
   * ID's to process for satisfy the policy.
   */
  private class StorageMovementPendingInodeIdCollector extends FSTreeTraverser
      implements Runnable {

    private int remainingCapacity = 0;

    private List<ItemInfo> currentBatch = new ArrayList<>(maxQueuedItem);

    StorageMovementPendingInodeIdCollector(FSDirectory dir) {
      super(dir);
    }

    @Override
    public void run() {
      LOG.info("Starting FileInodeIdCollector!.");
      long lastStatusCleanTime = 0;
      while (namesystem.isRunning() && sps.isRunning()) {
        try {
          if (!namesystem.isInSafeMode()) {
            FSDirectory fsd = namesystem.getFSDirectory();
            Long startINodeId = spsDirsToBeTraveresed.poll();
            if (startINodeId == null) {
              // Waiting for SPS path
              synchronized (spsDirsToBeTraveresed) {
                spsDirsToBeTraveresed.wait(5000);
              }
            } else {
              INode startInode = fsd.getInode(startINodeId);
              if (startInode != null) {
                try {
                  remainingCapacity = remainingCapacity();
                  spsStatus.put(startINodeId,
                      new StoragePolicySatisfyPathStatusInfo(
                          StoragePolicySatisfyPathStatus.IN_PROGRESS));
                  readLock();
                  traverseDir(startInode.asDirectory(), startINodeId,
                      HdfsFileStatus.EMPTY_NAME,
                      new SPSTraverseInfo(startINodeId));
                } finally {
                  readUnlock();
                }
                // Mark startInode traverse is done
                addAll(startInode.getId(), currentBatch, true);
                currentBatch.clear();

                // check if directory was empty and no child added to queue
                DirPendingWorkInfo dirPendingWorkInfo =
                    pendingWorkForDirectory.get(startInode.getId());
                if (dirPendingWorkInfo.isDirWorkDone()) {
                  namesystem.removeXattr(startInode.getId(),
                      XATTR_SATISFY_STORAGE_POLICY);
                  pendingWorkForDirectory.remove(startInode.getId());
                  markSuccess(startInode.getId());
                }
              }
            }
            //Clear the SPS status if status is in SUCCESS more than 5 min.
            if (Time.monotonicNow()
                - lastStatusCleanTime > statusClearanceElapsedTimeMs) {
              lastStatusCleanTime = Time.monotonicNow();
              cleanSpsStatus();
            }
          }
        } catch (Throwable t) {
          LOG.warn("Exception while loading inodes to satisfy the policy", t);
        }
      }
    }

    private synchronized void cleanSpsStatus() {
      for (Iterator<Entry<Long, StoragePolicySatisfyPathStatusInfo>> it =
          spsStatus.entrySet().iterator(); it.hasNext();) {
        Entry<Long, StoragePolicySatisfyPathStatusInfo> entry = it.next();
        if (entry.getValue().canRemove()) {
          it.remove();
        }
      }
    }

    @Override
    protected void checkPauseForTesting() throws InterruptedException {
      // TODO implement if needed
    }

    @Override
    protected boolean processFileInode(INode inode, TraverseInfo traverseInfo)
        throws IOException, InterruptedException {
      assert getFSDirectory().hasReadLock();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Processing {} for statisy the policy",
            inode.getFullPathName());
      }
      if (!inode.isFile()) {
        return false;
      }
      if (inode.isFile() && inode.asFile().numBlocks() != 0) {
        currentBatch.add(new ItemInfo(
            ((SPSTraverseInfo) traverseInfo).getStartId(), inode.getId()));
        remainingCapacity--;
      }
      return true;
    }

    @Override
    protected boolean canSubmitCurrentBatch() {
      return remainingCapacity <= 0;
    }

    @Override
    protected void checkINodeReady(long startId) throws IOException {
      FSNamesystem fsn = ((FSNamesystem) namesystem);
      fsn.checkNameNodeSafeMode("NN is in safe mode,"
          + "cannot satisfy the policy.");
      // SPS work should be cancelled when NN goes to standby. Just
      // double checking for sanity.
      fsn.checkOperation(NameNode.OperationCategory.WRITE);
    }

    @Override
    protected void submitCurrentBatch(long startId)
        throws IOException, InterruptedException {
      // Add current child's to queue
      addAll(startId, currentBatch, false);
      currentBatch.clear();
    }

    @Override
    protected void throttle() throws InterruptedException {
      assert !getFSDirectory().hasReadLock();
      assert !namesystem.hasReadLock();
      if (LOG.isDebugEnabled()) {
        LOG.debug("StorageMovementNeeded queue remaining capacity is zero,"
            + " waiting for some free slots.");
      }
      remainingCapacity = remainingCapacity();
      // wait for queue to be free
      while (remainingCapacity <= 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for storageMovementNeeded queue to be free!");
        }
        Thread.sleep(5000);
        remainingCapacity = remainingCapacity();
      }
    }

    @Override
    protected boolean canTraverseDir(INode inode) throws IOException {
      return true;
    }
  }

  /**
   * Info for directory recursive scan.
   */
  public static class DirPendingWorkInfo {

    private int pendingWorkCount = 0;
    private boolean fullyScanned = false;

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
  }

  public void init() {
    inodeIdCollector = new Daemon(new StorageMovementPendingInodeIdCollector(
        namesystem.getFSDirectory()));
    inodeIdCollector.setName("FileInodeIdCollector");
    inodeIdCollector.start();
  }

  public void close() {
    if (inodeIdCollector != null) {
      inodeIdCollector.interrupt();
    }
  }

  class SPSTraverseInfo extends TraverseInfo {
    private long startId;

    SPSTraverseInfo(long startId) {
      this.startId = startId;
    }

    public long getStartId() {
      return startId;
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

    private StoragePolicySatisfyPathStatus getStatus() {
      return status;
    }

    /**
     * Return true if SUCCESS status cached more then 5 min.
     */
    private boolean canRemove() {
      return StoragePolicySatisfyPathStatus.SUCCESS == status
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
}
