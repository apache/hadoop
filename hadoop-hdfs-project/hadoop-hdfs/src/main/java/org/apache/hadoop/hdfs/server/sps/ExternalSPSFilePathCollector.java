package org.apache.hadoop.hdfs.server.sps;
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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.sps.FileCollector;
import org.apache.hadoop.hdfs.server.namenode.sps.ItemInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to scan the paths recursively. If file is directory, then it
 * will scan for files recursively. If the file is non directory, then it will
 * just submit the same file to process. This will use file string path
 * representation.
 */
@InterfaceAudience.Private
public class ExternalSPSFilePathCollector implements FileCollector {
  public static final Logger LOG =
      LoggerFactory.getLogger(ExternalSPSFilePathCollector.class);
  private DistributedFileSystem dfs;
  private SPSService service;
  private int maxQueueLimitToScan;

  public ExternalSPSFilePathCollector(SPSService service) {
    this.service = service;
    this.maxQueueLimitToScan = service.getConf().getInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_DEFAULT);
    try {
      // TODO: probably we could get this dfs from external context? but this is
      // too specific to external.
      dfs = getFS(service.getConf());
    } catch (IOException e) {
      LOG.error("Unable to get the filesystem. Make sure Namenode running and "
          + "configured namenode address is correct.", e);
    }
  }

  private DistributedFileSystem getFS(Configuration conf) throws IOException {
    return (DistributedFileSystem) FileSystem
        .get(FileSystem.getDefaultUri(conf), conf);
  }

  /**
   * Recursively scan the given path and add the file info to SPS service for
   * processing.
   */
  private long processPath(Long startID, String childPath) {
    long pendingWorkCount = 0; // to be satisfied file counter
    for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
      final DirectoryListing children;
      try {
        children = dfs.getClient().listPaths(childPath,
            lastReturnedName, false);
      } catch (IOException e) {
        LOG.warn("Failed to list directory " + childPath
            + ". Ignore the directory and continue.", e);
        return pendingWorkCount;
      }
      if (children == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The scanning start dir/sub dir " + childPath
              + " does not have childrens.");
        }
        return pendingWorkCount;
      }

      for (HdfsFileStatus child : children.getPartialListing()) {
        if (child.isFile()) {
          service.addFileToProcess(new ItemInfo(startID, child.getFileId()),
              false);
          checkProcessingQueuesFree();
          pendingWorkCount++; // increment to be satisfied file count
        } else {
          String childFullPathName = child.getFullName(childPath);
          if (child.isDirectory()) {
            if (!childFullPathName.endsWith(Path.SEPARATOR)) {
              childFullPathName = childFullPathName + Path.SEPARATOR;
            }
            pendingWorkCount += processPath(startID, childFullPathName);
          }
        }
      }

      if (children.hasMore()) {
        lastReturnedName = children.getLastName();
      } else {
        return pendingWorkCount;
      }
    }
  }

  private void checkProcessingQueuesFree() {
    int remainingCapacity = remainingCapacity();
    // wait for queue to be free
    while (remainingCapacity <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting for storageMovementNeeded queue to be free!");
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      remainingCapacity = remainingCapacity();
    }
  }

  /**
   * Returns queue remaining capacity.
   */
  public int remainingCapacity() {
    int size = service.processingQueueSize();
    int remainingSize = 0;
    if (size < maxQueueLimitToScan) {
      remainingSize = maxQueueLimitToScan - size;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("SPS processing Q -> maximum capacity:{}, current size:{},"
          + " remaining size:{}", maxQueueLimitToScan, size, remainingSize);
    }
    return remainingSize;
  }

  @Override
  public void scanAndCollectFiles(long pathId) throws IOException {
    if (dfs == null) {
      dfs = getFS(service.getConf());
    }
    Path filePath = DFSUtilClient.makePathFromFileId(pathId);
    long pendingSatisfyItemsCount = processPath(pathId, filePath.toString());
    // Check whether the given path contains any item to be tracked
    // or the no to be satisfied paths. In case of empty list, add the given
    // inodeId to the 'pendingWorkForDirectory' with empty list so that later
    // SPSPathIdProcessor#run function will remove the SPS hint considering that
    // this path is already satisfied the storage policy.
    if (pendingSatisfyItemsCount <= 0) {
      LOG.debug("There is no pending items to satisfy the given path "
          + "inodeId:{}", pathId);
      service.addAllFilesToProcess(pathId, new ArrayList<>(), true);
    } else {
      service.markScanCompletedForPath(pathId);
    }
  }

}
