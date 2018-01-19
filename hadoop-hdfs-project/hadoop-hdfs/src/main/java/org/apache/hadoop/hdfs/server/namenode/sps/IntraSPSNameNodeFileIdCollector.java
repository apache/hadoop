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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSTreeTraverser;
import org.apache.hadoop.hdfs.server.namenode.INode;

/**
 * A specific implementation for scanning the directory with Namenode internal
 * Inode structure and collects the file ids under the given directory ID.
 */
public class IntraSPSNameNodeFileIdCollector extends FSTreeTraverser
    implements FileIdCollector {
  private int maxQueueLimitToScan;
  private final SPSService service;

  private int remainingCapacity = 0;

  private List<ItemInfo> currentBatch;

  public IntraSPSNameNodeFileIdCollector(FSDirectory dir, SPSService service) {
    super(dir);
    this.service = service;
    this.maxQueueLimitToScan = service.getConf().getInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_DEFAULT);
    currentBatch = new ArrayList<>(maxQueueLimitToScan);
  }

  @Override
  protected boolean processFileInode(INode inode, TraverseInfo traverseInfo)
      throws IOException, InterruptedException {
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
    // SPS work won't be scheduled if NN is in standby. So, skipping NN
    // standby check.
    return;
  }

  @Override
  protected void submitCurrentBatch(long startId)
      throws IOException, InterruptedException {
    // Add current child's to queue
    service.addAllFileIdsToProcess(startId,
        currentBatch, false);
    currentBatch.clear();
  }

  @Override
  protected void throttle() throws InterruptedException {
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

  @Override
  protected void checkPauseForTesting() throws InterruptedException {
    // Nothing to do
  }

  @Override
  public void scanAndCollectFileIds(final Long startINodeId)
      throws IOException, InterruptedException {
    FSDirectory fsd = getFSDirectory();
    INode startInode = fsd.getInode(startINodeId);
    if (startInode != null) {
      remainingCapacity = remainingCapacity();
      if (remainingCapacity == 0) {
        throttle();
      }
      if (startInode.isFile()) {
        currentBatch.add(new ItemInfo(startInode.getId(), startInode.getId()));
      } else {

        readLock();
        // NOTE: this lock will not be held until full directory scanning. It is
        // basically a sliced locking. Once it collects a batch size( at max the
        // size of maxQueueLimitToScan (default 1000)) file ids, then it will
        // unlock and submits the current batch to SPSService. Once
        // service.processingQueueSize() shows empty slots, then lock will be
        // resumed and scan also will be resumed. This logic was re-used from
        // EDEK feature.
        try {
          traverseDir(startInode.asDirectory(), startINodeId,
              HdfsFileStatus.EMPTY_NAME, new SPSTraverseInfo(startINodeId));
        } finally {
          readUnlock();
        }
      }
      // Mark startInode traverse is done, this is last-batch
      service.addAllFileIdsToProcess(startInode.getId(), currentBatch, true);
      currentBatch.clear();
    }
  }

  /**
   * Returns queue remaining capacity.
   */
  public synchronized int remainingCapacity() {
    int size = service.processingQueueSize();
    if (size >= maxQueueLimitToScan) {
      return 0;
    } else {
      return (maxQueueLimitToScan - size);
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

}