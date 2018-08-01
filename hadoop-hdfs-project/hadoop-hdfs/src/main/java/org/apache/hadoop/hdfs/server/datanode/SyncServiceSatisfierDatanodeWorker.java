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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.syncservice.SyncTaskExecutionFeedbackCollector;
import org.apache.hadoop.hdfs.server.datanode.syncservice.executor.BlockSyncOperationExecutor;
import org.apache.hadoop.hdfs.server.datanode.syncservice.executor.BlockSyncReaderFactory;
import org.apache.hadoop.hdfs.server.datanode.syncservice.executor.BlockSyncTaskRunner;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class SyncServiceSatisfierDatanodeWorker {
  private static final Logger LOG = LoggerFactory
      .getLogger(SyncServiceSatisfierDatanodeWorker.class);

  private ExecutorService executorService;
  private BlockSyncOperationExecutor syncOperationExecutor;
  private SyncTaskExecutionFeedbackCollector syncTaskExecutionFeedbackCollector;

  public SyncServiceSatisfierDatanodeWorker(Configuration conf, DataNode dataNode) throws IOException {
    this.executorService = HadoopExecutors.newFixedThreadPool(4);
    this.syncOperationExecutor =
        BlockSyncOperationExecutor.createOnDataNode(conf,
          (locatedBlock, config) -> {
              try {
                return BlockSyncReaderFactory.createBlockReader(dataNode, locatedBlock, config);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          );
    this.syncTaskExecutionFeedbackCollector = new SyncTaskExecutionFeedbackCollector();
  }


  public void start() {
    this.executorService = HadoopExecutors.newFixedThreadPool(4);
  }

  public void stop() {
    this.executorService.shutdown();
  }

  public void waitToFinishWorkerThread() {
    try {
      this.executorService.awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.warn("SyncServiceSatisfierDatanodeWorker interrupted during waiting for finalization.");
      Thread.currentThread().interrupt();
    }
  }

  public void processSyncTasks(Collection<BlockSyncTask> blockSyncTasks) {

    LOG.debug("Received SyncTasks: {}", blockSyncTasks);
    for (BlockSyncTask blockSyncTask : blockSyncTasks) {
      try {
        executorService.submit(new BlockSyncTaskRunner(blockSyncTask,
            syncOperationExecutor,
            syncTaskExecutionFeedback -> syncTaskExecutionFeedbackCollector
                .addFeedback(syncTaskExecutionFeedback)));
      } catch (RejectedExecutionException e) {
        LOG.warn("BlockSyncTask {} for {} was rejected: {}",
            blockSyncTask.getSyncTaskId(), blockSyncTask.getRemoteURI(),
            e.getCause());
      }
    }
  }

  public SyncTaskExecutionFeedbackCollector getSyncTaskExecutionFeedbackCollector() {
    return syncTaskExecutionFeedbackCollector;
  }

}
