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
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.StoragePolicySatisfier.ItemInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Map of rootId and number of child's. Number of child's indicate the number
   * of files pending to satisfy the policy.
   */
  private final Map<Long, Integer> pendingWorkForDirectory =
      new HashMap<Long, Integer>();

  private final Namesystem namesystem;

  // List of pending dir to satisfy the policy
  private final Queue<Long> spsDirsToBeTraveresed = new LinkedList<Long>();

  private final StoragePolicySatisfier sps;

  private Daemon fileInodeIdCollector;

  public BlockStorageMovementNeeded(Namesystem namesystem,
      StoragePolicySatisfier sps) {
    this.namesystem = namesystem;
    this.sps = sps;
  }

  /**
   * Add the candidate to tracking list for which storage movement
   * expected if necessary.
   *
   * @param trackInfo
   *          - track info for satisfy the policy
   */
  public synchronized void add(ItemInfo trackInfo) {
    storageMovementNeeded.add(trackInfo);
  }

  /**
   * Add the itemInfo to tracking list for which storage movement
   * expected if necessary.
   * @param rootId
   *            - root inode id
   * @param itemInfoList
   *            - List of child in the directory
   */
  private synchronized void addAll(Long rootId,
      List<ItemInfo> itemInfoList) {
    storageMovementNeeded.addAll(itemInfoList);
    pendingWorkForDirectory.put(rootId, itemInfoList.size());
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
    spsDirsToBeTraveresed.add(id);
    // Notify waiting FileInodeIdCollector thread about the newly
    // added SPS path.
    synchronized (spsDirsToBeTraveresed) {
      spsDirsToBeTraveresed.notify();
    }
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
      // If track is part of some root then reduce the pending directory work
      // count.
      long rootId = trackInfo.getRootId();
      INode inode = namesystem.getFSDirectory().getInode(rootId);
      if (inode == null) {
        // directory deleted just remove it.
        this.pendingWorkForDirectory.remove(rootId);
      } else {
        if (pendingWorkForDirectory.get(rootId) != null) {
          Integer pendingWork = pendingWorkForDirectory.get(rootId) - 1;
          pendingWorkForDirectory.put(rootId, pendingWork);
          if (pendingWork <= 0) {
            namesystem.removeXattr(rootId, XATTR_SATISFY_STORAGE_POLICY);
            pendingWorkForDirectory.remove(rootId);
          }
        }
      }
    } else {
      // Remove xAttr if trackID doesn't exist in
      // storageMovementAttemptedItems or file policy satisfied.
      namesystem.removeXattr(trackInfo.getTrackId(),
          XATTR_SATISFY_STORAGE_POLICY);
    }
  }

  public synchronized void clearQueue(long trackId) {
    spsDirsToBeTraveresed.remove(trackId);
    Iterator<ItemInfo> iterator = storageMovementNeeded.iterator();
    while (iterator.hasNext()) {
      ItemInfo next = iterator.next();
      if (next.getRootId() == trackId) {
        iterator.remove();
      }
    }
    pendingWorkForDirectory.remove(trackId);
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
  private class FileInodeIdCollector implements Runnable {
    @Override
    public void run() {
      LOG.info("Starting FileInodeIdCollector!.");
      while (namesystem.isRunning() && sps.isRunning()) {
        try {
          if (!namesystem.isInSafeMode()) {
            FSDirectory fsd = namesystem.getFSDirectory();
            Long rootINodeId = spsDirsToBeTraveresed.poll();
            if (rootINodeId == null) {
              // Waiting for SPS path
              synchronized (spsDirsToBeTraveresed) {
                spsDirsToBeTraveresed.wait(5000);
              }
            } else {
              INode rootInode = fsd.getInode(rootINodeId);
              if (rootInode != null) {
                // TODO : HDFS-12291
                // 1. Implement an efficient recursive directory iteration
                // mechanism and satisfies storage policy for all the files
                // under the given directory.
                // 2. Process files in batches,so datanodes workload can be
                // handled.
                List<ItemInfo> itemInfoList =
                    new ArrayList<>();
                for (INode childInode : rootInode.asDirectory()
                    .getChildrenList(Snapshot.CURRENT_STATE_ID)) {
                  if (childInode.isFile()
                      && childInode.asFile().numBlocks() != 0) {
                    itemInfoList.add(
                        new ItemInfo(rootINodeId, childInode.getId()));
                  }
                }
                if (itemInfoList.isEmpty()) {
                  // satisfy track info is empty, so remove the xAttr from the
                  // directory
                  namesystem.removeXattr(rootINodeId,
                      XATTR_SATISFY_STORAGE_POLICY);
                }
                addAll(rootINodeId, itemInfoList);
              }
            }
          }
        } catch (Throwable t) {
          LOG.warn("Exception while loading inodes to satisfy the policy", t);
        }
      }
    }
  }

  public void start() {
    fileInodeIdCollector = new Daemon(new FileInodeIdCollector());
    fileInodeIdCollector.setName("FileInodeIdCollector");
    fileInodeIdCollector.start();
  }

  public void stop() {
    if (fileInodeIdCollector != null) {
      fileInodeIdCollector.interrupt();
    }
  }
}
