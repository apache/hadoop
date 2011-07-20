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

import java.io.File;
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
 * It delegates the actual removal of files to a StorageArchiver
 * implementation, which might delete the files or instead copy them to
 * a filer or HDFS for later analysis.
 */
public class NNStorageArchivalManager {
  
  private final int numCheckpointsToRetain;
  private static final Log LOG = LogFactory.getLog(NNStorageArchivalManager.class);
  private final NNStorage storage;
  private final StorageArchiver archiver;
  private final FSEditLog editLog;
  
  public NNStorageArchivalManager(
      Configuration conf,
      NNStorage storage,
      FSEditLog editLog,
      StorageArchiver archiver) {
    this.numCheckpointsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT);
    this.storage = storage;
    this.editLog = editLog;
    this.archiver = archiver;
  }
  
  public NNStorageArchivalManager(Configuration conf, NNStorage storage,
      FSEditLog editLog) {
    this(conf, storage, editLog, new DeletionStorageArchiver());
  }

  public void archiveOldStorage() throws IOException {
    FSImageTransactionalStorageInspector inspector =
      new FSImageTransactionalStorageInspector();
    storage.inspectStorageDirs(inspector);

    long minImageTxId = getImageTxIdToRetain(inspector);
    archiveCheckpointsOlderThan(inspector, minImageTxId);
    // If fsimage_N is the image we want to keep, then we need to keep
    // all txns > N. We can remove anything < N+1, since fsimage_N
    // reflects the state up to and including N.
    editLog.archiveLogsOlderThan(minImageTxId + 1, archiver);
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
      deleteOrWarn(log.getFile());
    }

    @Override
    public void archiveImage(FoundFSImage image) {
      deleteOrWarn(image.getFile());
      deleteOrWarn(MD5FileUtils.getDigestFileForFile(image.getFile()));
    }

    private static void deleteOrWarn(File file) {
      if (!file.delete()) {
        // It's OK if we fail to delete something -- we'll catch it
        // next time we swing through this directory.
        LOG.warn("Could not delete " + file);
      }      
    }
  }
}
