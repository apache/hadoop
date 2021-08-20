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
package org.apache.hadoop.hdfs.server.datanode.syncservice.executor;

import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * BlockSyncTaskRunner glues together the sync task and the feedback reporting.
 */
public class BlockSyncTaskRunner implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockSyncTaskRunner.class);

  private BlockSyncTask blockSyncTask;
  private BlockSyncOperationExecutor syncOperationExecutor;

  private Consumer<BlockSyncTaskExecutionFeedback> publishOutcomeCallback;

  public BlockSyncTaskRunner(BlockSyncTask blockSyncTask,
      BlockSyncOperationExecutor syncOperationExecutor,
      Consumer<BlockSyncTaskExecutionFeedback> publishOutcomeCallback) {
    this.blockSyncTask = blockSyncTask;
    this.syncOperationExecutor = syncOperationExecutor;
    this.publishOutcomeCallback = publishOutcomeCallback;
  }

  @Override
  public void run() {
    LOG.info("Executing BlockSyncTask {} to {}",
        blockSyncTask.getSyncTaskId(), blockSyncTask.getRemoteURI());
    try {
      SyncTaskExecutionResult result =
          syncOperationExecutor.execute(blockSyncTask);
      publishOutcomeCallback.accept(BlockSyncTaskExecutionFeedback
          .finishedSuccessfully(blockSyncTask.getSyncTaskId(),
              blockSyncTask.getSyncMountId(),
              result));
    } catch (Exception e) {
      LOG.error(
          String.format("Exception executing BlockSyncTask %s (on %s)",
              blockSyncTask.getSyncTaskId(), blockSyncTask.getRemoteURI()), e);
      publishOutcomeCallback.accept(BlockSyncTaskExecutionFeedback
          .failedWithException(blockSyncTask.getSyncTaskId(),
              blockSyncTask.getSyncMountId(), e));
    }
  }

}
