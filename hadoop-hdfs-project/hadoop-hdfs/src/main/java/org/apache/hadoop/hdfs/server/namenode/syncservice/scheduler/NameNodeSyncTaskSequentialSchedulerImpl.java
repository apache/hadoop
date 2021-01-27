/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceSatisfier;
import org.apache.hadoop.hdfs.server.namenode.syncservice.executor.MetadataSyncOperationExecutor;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * NameNode sync task scheduler implementation. It will schedule metadata sync
 * task to NameNode in sequential order.
 */
public class NameNodeSyncTaskSequentialSchedulerImpl
    implements NameNodeSyncTaskScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(SyncServiceSatisfier.class);

  private final MetadataSyncOperationExecutor syncOperationExecutor;
  private final SyncServiceSatisfier syncServiceSatisfier;
  private BlockManager blockManager;

  public NameNodeSyncTaskSequentialSchedulerImpl(
      SyncServiceSatisfier syncServiceSatisfier,
      BlockManager blockManager,
      Configuration conf) {
    this.syncServiceSatisfier = syncServiceSatisfier;
    this.blockManager = blockManager;
    this.syncOperationExecutor =
        MetadataSyncOperationExecutor.createOnNameNode(conf);
  }

  @Override
  public void scheduleOnNameNode(MetadataSyncTask metadataSyncTask) {
    try {
      SyncTaskExecutionResult syncTaskExecutionResult =
          syncOperationExecutor.execute(metadataSyncTask);
      MetadataSyncTaskExecutionFeedback success =
          MetadataSyncTaskExecutionFeedback.finishedSuccessfully(
              metadataSyncTask.getSyncTaskId(),
              metadataSyncTask.getSyncMountId(),
              syncTaskExecutionResult,
              metadataSyncTask.getOperation());
      handleBlockReportUpdate(metadataSyncTask);
      syncServiceSatisfier.handleExecutionFeedback(success);
    } catch (Exception e) {
      LOG.error("Error running task: {} on Namenode: ", metadataSyncTask, e);
      MetadataSyncTaskExecutionFeedback failure =
          MetadataSyncTaskExecutionFeedback.failedWithException(
              metadataSyncTask.getSyncTaskId(),
              metadataSyncTask.getSyncMountId(),
              e,
              metadataSyncTask.getOperation());
      syncServiceSatisfier.handleExecutionFeedback(failure);
    }
  }

  private void handleBlockReportUpdate(MetadataSyncTask metadataSyncTask)
      throws IOException {
    if (metadataSyncTask.getOperation() ==
        MetadataSyncTaskOperation.MULTIPART_COMPLETE) {
      MetadataSyncTask.MultipartCompleteMetadataSyncTask
          completeMetadataSyncTask =
          (MetadataSyncTask.MultipartCompleteMetadataSyncTask) metadataSyncTask;
      for (ExtendedBlock extendedBlock : completeMetadataSyncTask.getBlocks()) {
        Block localBlock = extendedBlock.getLocalBlock();
        this.blockManager.processProvidedBlockReport(localBlock);
      }
    }
  }
}
