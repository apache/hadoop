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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundFSImage;
import org.apache.hadoop.hdfs.util.MD5FileUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The NNStorageArchivalManager is responsible for inspecting the storage
 * directories of the NN and enforcing a retention policy on checkpoints
 * and edit logs.
 * 
 * It delegates the actual removal of files to a {@link #StorageArchiver}
 * implementation, which might delete the files or instead copy them to
 * a filer or HDFS for later analysis.
 */
public class NNStorageArchivalManager {
  
  private final int numCheckpointsToRetain;
  private static final Log LOG = LogFactory.getLog(NNStorageArchivalManager.class);
  private final NNStorage storage;
  private final StorageArchiver archiver;
  
  public NNStorageArchivalManager(
      Configuration conf,
      NNStorage storage,
      StorageArchiver archiver) {
    this.numCheckpointsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT);
    this.storage = storage;
    this.archiver = archiver;
  }
  
  public NNStorageArchivalManager(Configuration conf, NNStorage storage) {
    this(conf, storage, new DeletionStorageArchiver());
  }

  public void archiveOldStorage() throws IOException {
    FSImageTransactionalStorageInspector inspector =
      new FSImageTransactionalStorageInspector();
    storage.inspectStorageDirs(inspector);

    long minImageTxId = getImageTxIdToRetain(inspector);
    archiveCheckpointsOlderThan(inspector, minImageTxId);
    archiveLogsOlderThan(inspector, minImageTxId);
  }
  
  private void archiveLogsOlderThan(
      FSImageTransactionalStorageInspector inspector,
      long minImageTxId) {
    for (FoundEditLog log : inspector.getFoundEditLogs()) {
      if (log.getStartTxId() < minImageTxId) {
        LOG.info("Purging old edit log " + log);
        archiver.archiveLog(log);
      }
    }
  }

  private void archiveCheckpointsOlderThan(
      FSImageTransactionalStorageInspector inspector,
      long minTxId) {
    for (FoundFSImage image : inspector.getFoundImages()) {
      if (image.getTxId() < minTxId) {
        LOG.info("Purging old image " + image);
        archiver.archiveImage(image);
      }
    }
  }

  /**
   * @param inspector inspector that has already inspected all storage dirs
   * @return the transaction ID corresponding to the oldest checkpoint
   * that should be retained. 
   */
  private long getImageTxIdToRetain(FSImageTransactionalStorageInspector inspector) {
      
    List<FoundFSImage> images = inspector.getFoundImages();
    TreeSet<Long> imageTxIds = Sets.newTreeSet();
    for (FoundFSImage image : images) {
      imageTxIds.add(image.getTxId());
    }
    
    List<Long> imageTxIdsList = Lists.newArrayList(imageTxIds);
    if (imageTxIdsList.isEmpty()) {
      return 0;
    }
    
    Collections.reverse(imageTxIdsList);
    int toRetain = Math.min(numCheckpointsToRetain, imageTxIdsList.size());    
    long minTxId = imageTxIdsList.get(toRetain - 1);
    LOG.info("Going to retain " + toRetain + " images with txid >= " +
        minTxId);
    return minTxId;
  }
  
  /**
   * Interface responsible for archiving old checkpoints and edit logs.
   */
  static interface StorageArchiver {
    void archiveLog(FoundEditLog log);
    void archiveImage(FoundFSImage image);
  }
  
  static class DeletionStorageArchiver implements StorageArchiver {
    @Override
    public void archiveLog(FoundEditLog log) {
      log.getFile().delete();
    }

    @Override
    public void archiveImage(FoundFSImage image) {
      image.getFile().delete();
      MD5FileUtils.getDigestFileForFile(image.getFile()).delete();
    }
  }
}
