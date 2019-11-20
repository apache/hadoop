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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceSatisfier;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SchedulableSyncPhase;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;

import java.util.function.Consumer;

public class SyncTaskSchedulerImpl implements SyncTaskScheduler {

  private NameNodeSyncTaskScheduler nameNodeSyncTaskScheduler;
  private DataNodeSyncTaskScheduler dataNodeSyncTaskScheduler;

  public SyncTaskSchedulerImpl(SyncServiceSatisfier syncServiceSatisfier,
      BlockManager blockManager,
      Consumer<MetadataSyncTaskExecutionFeedback> updateStats,
      Configuration conf) {
    this.nameNodeSyncTaskScheduler =
        new NameNodeSyncTaskSequentialSchedulerImpl(syncServiceSatisfier,
            blockManager, updateStats, conf);
    this.dataNodeSyncTaskScheduler =
        new DataNodeSyncTaskSchedulerImpl(blockManager);
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

}
