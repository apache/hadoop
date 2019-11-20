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

import com.google.common.collect.Iterables;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class DataNodeSyncTaskSchedulerImpl implements DataNodeSyncTaskScheduler {


  public static final Logger LOG =
      LoggerFactory.getLogger(DataNodeSyncTaskSchedulerImpl.class);

  private BlockManager blockManager;

  public DataNodeSyncTaskSchedulerImpl(BlockManager blockManager) {
    this.blockManager = blockManager;
  }

  @Override
  public void scheduleOnDataNode(BlockSyncTask blockSyncTask) {
    if (blockSyncTask.getLocatedBlocks() != null
        && !blockSyncTask.getLocatedBlocks().isEmpty()) {
      scheduleOnFirstBlockDatanode(blockSyncTask);
    } else {
      scheduleOnRandomDatanode(blockSyncTask);
    }
  }

  private void scheduleOnFirstBlockDatanode(BlockSyncTask blockSyncTask) {
    Iterable<DatanodeStorageInfo> storages = blockManager
        .getStorages(blockSyncTask.getLocatedBlocks().get(0)
            .getBlock()
            .getLocalBlock());

    DatanodeStorageInfo firstStorage = Iterables.getFirst(storages, null);

    if (firstStorage != null) {
      DatanodeDescriptor datanodeDescriptor = firstStorage.getDatanodeDescriptor();
      LOG.info("Scheduling MetadataSyncTask {} on data node {}",
          blockSyncTask.getSyncTaskId(), datanodeDescriptor.getName());
      datanodeDescriptor.scheduleSyncTaskOnHeartbeat(blockSyncTask);
    } else {
      throw new RuntimeException("TODO Could not schedule task");
    }
  }

  private void scheduleOnRandomDatanode(BlockSyncTask blockSyncTask) {
    Optional<DatanodeDescriptor> randomDatanodeDescriptor =
        blockManager.getDatanodeManager().getDatanodes().stream()
            .findFirst();

    if (randomDatanodeDescriptor.isPresent()) {
      DatanodeDescriptor datanodeDescriptor = randomDatanodeDescriptor.get();
      LOG.info("Scheduling MetadataSyncTask {} on data node {}",
          blockSyncTask.getSyncTaskId(), datanodeDescriptor.getName());
      datanodeDescriptor.scheduleSyncTaskOnHeartbeat(blockSyncTask);
    } else {
      throw new RuntimeException("Could not schedule task");
    }
  }

}
