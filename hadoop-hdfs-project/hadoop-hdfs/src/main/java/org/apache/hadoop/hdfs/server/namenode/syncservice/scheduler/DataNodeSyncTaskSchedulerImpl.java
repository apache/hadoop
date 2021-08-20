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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataNode sync task scheduler implementation. It will schedule block sync
 * task to a DataNode where one replica is stored.
 */
public class DataNodeSyncTaskSchedulerImpl implements
    DataNodeSyncTaskScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(DataNodeSyncTaskSchedulerImpl.class);

  private BlockManager blockManager;
  private Map<UUID, Pair<BlockSyncTask, Long>> trackingBlkSyncTasks;

  public DataNodeSyncTaskSchedulerImpl(BlockManager blockManager) {
    this.blockManager = blockManager;
    trackingBlkSyncTasks = new ConcurrentHashMap<>();
  }

  @Override
  public void scheduleOnDataNode(BlockSyncTask blockSyncTask) {
    trackingBlkSyncTasks.put(blockSyncTask.getSyncTaskId(),
        Pair.of(blockSyncTask, Time.monotonicNow()));
    if (blockSyncTask.getLocatedBlocks() != null
        && !blockSyncTask.getLocatedBlocks().isEmpty()) {
      scheduleOnFirstBlockDatanode(blockSyncTask);
    } else {
      scheduleOnRandomDatanode(blockSyncTask);
    }
  }

  /**
   * Schedule task to the first valid DN.
   * TODO: try other nodes for failed task.
   */
  private void scheduleOnFirstBlockDatanode(BlockSyncTask blockSyncTask) {
    Iterable<DatanodeStorageInfo> storages = blockManager
        .getStorages(blockSyncTask.getLocatedBlocks().get(0)
            .getBlock()
            .getLocalBlock());
    DatanodeStorageInfo firstStorage = null;
    for (DatanodeStorageInfo storage : storages) {
      // Exclude storage who is transient or whose type is provided.
      if (storage.getStorageType() != StorageType.PROVIDED &&
          !storage.getStorageType().isTransient()) {
        firstStorage = storage;
        break;
      }
    }
    if (firstStorage != null) {
      DatanodeDescriptor datanodeDescriptor =
          firstStorage.getDatanodeDescriptor();
      LOG.info("Scheduling BlockSyncTask {} on data node {}",
          blockSyncTask.getSyncTaskId(), datanodeDescriptor.getName());
      datanodeDescriptor.scheduleSyncTaskOnHeartbeat(blockSyncTask);
    } else {
      scheduleOnRandomDatanode(blockSyncTask);
    }
  }

  private void scheduleOnRandomDatanode(BlockSyncTask blockSyncTask) {
    Optional<DatanodeDescriptor> randomDatanodeDescriptor =
        blockManager.getDatanodeManager().getDatanodes().stream()
            .findFirst();
    if (randomDatanodeDescriptor.isPresent()) {
      DatanodeDescriptor datanodeDescriptor = randomDatanodeDescriptor.get();
      LOG.info("Scheduling BlockSyncTask {} on data node {}",
          blockSyncTask.getSyncTaskId(), datanodeDescriptor.getName());
      datanodeDescriptor.scheduleSyncTaskOnHeartbeat(blockSyncTask);
    } else {
      throw new RuntimeException("Could not schedule BlockSyncTask: " +
          blockSyncTask.getSyncTaskId());
    }
  }

  @Override
  public Map<UUID, Pair<BlockSyncTask, Long>> getTrackingBlkSyncTasks() {
    return trackingBlkSyncTasks;
  }
}
