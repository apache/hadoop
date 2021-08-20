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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceSatisfier;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SchedulableSyncPhase;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sync task scheduler implementation, metadata sync task and block sync task
 * will be scheduled respectively by NameNode sync task scheduler and DataNode
 * sync task scheduler.
 */
public class SyncTaskSchedulerImpl implements SyncTaskScheduler {

  public static final Logger LOG =
      LoggerFactory.getLogger(SyncTaskSchedulerImpl.class);

  private NameNodeSyncTaskScheduler nameNodeSyncTaskScheduler;
  private DataNodeSyncTaskScheduler dataNodeSyncTaskScheduler;
  private SyncServiceSatisfier syncServiceSatisfier;
  private ScheduledExecutorService taskTimeoutTracker;
  private int taskTimeOut;

  public SyncTaskSchedulerImpl(SyncServiceSatisfier syncServiceSatisfier,
      BlockManager blockManager,
      Configuration conf) {
    this.syncServiceSatisfier = syncServiceSatisfier;
    this.nameNodeSyncTaskScheduler =
        new NameNodeSyncTaskSequentialSchedulerImpl(syncServiceSatisfier,
            blockManager, conf);
    this.dataNodeSyncTaskScheduler =
        new DataNodeSyncTaskSchedulerImpl(blockManager);
    this.taskTimeoutTracker = Executors.newSingleThreadScheduledExecutor();
    this.taskTimeOut =
        conf.getInt(DFSConfigKeys.DFS_PROVIDED_SYNC_TASK_TIMEOUT_SEC_KEY,
            DFSConfigKeys.DFS_PROVIDED_SYNC_TASK_TIMEOUT_SEC_DEFAULT);
    int taskTimeoutTrackerInterval = taskTimeOut / 2;
    taskTimeoutTracker
        .scheduleAtFixedRate(this::generateTimeoutFeedback, 10,
            taskTimeoutTrackerInterval,
            TimeUnit.SECONDS);
  }

  @Override
  public void schedule(SchedulableSyncPhase schedulableSyncPhase) {
    for (MetadataSyncTask metadataSyncTask :
        schedulableSyncPhase.getMetadataSyncTaskList()) {
      nameNodeSyncTaskScheduler.scheduleOnNameNode(metadataSyncTask);
    }
    for (BlockSyncTask blockSyncTask :
        schedulableSyncPhase.getBlockSyncTaskList()) {
      dataNodeSyncTaskScheduler.scheduleOnDataNode(blockSyncTask);
    }
  }

  @Override
  public void dropTrackingTasks(UUID syncTaskId) {
    Map<UUID, Pair<BlockSyncTask, Long>> trackingBlkSyncTasks =
        dataNodeSyncTaskScheduler.getTrackingBlkSyncTasks();
    if (trackingBlkSyncTasks.containsKey(syncTaskId)) {
      trackingBlkSyncTasks.remove(syncTaskId);
    }
  }

  private void generateTimeoutFeedback() {
    Map<UUID, Pair<BlockSyncTask, Long>> tasks = dataNodeSyncTaskScheduler
        .getTrackingBlkSyncTasks();
    Iterator<Map.Entry<UUID, Pair<BlockSyncTask, Long>>> it = tasks
        .entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<UUID, Pair<BlockSyncTask, Long>> next = it.next();
      if (Time.monotonicNow()
          > next.getValue().getRight() + taskTimeOut * 1000) {
        BlockSyncTask task = next.getValue().getLeft();
        LOG.warn("Block sync task time out: {} : {}", task.getRemoteURI(),
            task.getSyncTaskId());
        BlockSyncTaskExecutionFeedback feedback = BlockSyncTaskExecutionFeedback
            .failedWithException(task.getSyncTaskId(), task.getSyncMountId(),
                new TimeoutException());
        syncServiceSatisfier.handleExecutionFeedback(feedback);
      }
    }
  }

}
