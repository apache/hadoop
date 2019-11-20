/*
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceSatisfier;
import org.apache.hadoop.hdfs.server.namenode.syncservice.executor.MetadataSyncOperationExecutor;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Consumer;

public class NameNodeSyncTaskSequentialSchedulerImpl
    implements NameNodeSyncTaskScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(SyncServiceSatisfier.class);

  private final MetadataSyncOperationExecutor syncOperationExecutor;
  private final SyncServiceSatisfier syncServiceSatisfier;
  private final Consumer<MetadataSyncTaskExecutionFeedback> updateStats;
  private BlockManager blockManager;

  public NameNodeSyncTaskSequentialSchedulerImpl(
      SyncServiceSatisfier syncServiceSatisfier,
      BlockManager blockManager,
      Consumer<MetadataSyncTaskExecutionFeedback> updateStats,
      Configuration conf) {
    this.syncServiceSatisfier = syncServiceSatisfier;
    this.blockManager = blockManager;
    this.updateStats = updateStats;
    this.syncOperationExecutor =
        MetadataSyncOperationExecutor.createOnNameNode(conf);
  }

  @Override
  public void scheduleOnNameNode(MetadataSyncTask metadataSyncTask) {
    try {
      SyncTaskExecutionResult syncTaskExecutionResult =
          syncOperationExecutor.execute(metadataSyncTask);
      MetadataSyncTaskExecutionFeedback success = MetadataSyncTaskExecutionFeedback.finishedSuccessfully(
          metadataSyncTask.getSyncTaskId(),
          metadataSyncTask.getSyncMountId(),
          syncTaskExecutionResult,
          metadataSyncTask.getOperation());
      handleBlockReportUpdate(metadataSyncTask, syncTaskExecutionResult);
      syncServiceSatisfier.handleExecutionFeedback(success);
      updateStats.accept(success);
    } catch (Exception e) {
      LOG.error("Error running task on Namenode: ", e);
      MetadataSyncTaskExecutionFeedback failure =
          MetadataSyncTaskExecutionFeedback.failedWithException(
              metadataSyncTask.getSyncTaskId(),
              metadataSyncTask.getSyncMountId(),
              e,
              metadataSyncTask.getOperation());
      syncServiceSatisfier.handleExecutionFeedback(failure);
      updateStats.accept(failure);
    }
  }

  private void handleBlockReportUpdate(MetadataSyncTask metadataSyncTask,
      SyncTaskExecutionResult syncTaskExecutionResult) throws IOException {
    if (metadataSyncTask.getOperation() == MetadataSyncTaskOperation.MULTIPART_COMPLETE) {
      MetadataSyncTask.MultipartCompleteMetadataSyncTask completeMetadataSyncTask =
          (MetadataSyncTask.MultipartCompleteMetadataSyncTask) metadataSyncTask;
      for (ExtendedBlock extendedBlock : completeMetadataSyncTask.blocks) {
        Block localBlock = extendedBlock.getLocalBlock();

        BlockInfo blockInfo = new BlockInfoContiguous(localBlock,
            (short) extendedBlock.getNumBytes());
        blockInfo.setBlockCollectionId(completeMetadataSyncTask.getBlockCollectionId());
        this.blockManager.processProvidedBlockReport(blockInfo, localBlock);
      }
    }

  }
}
