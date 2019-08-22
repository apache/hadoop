
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.statemachine.background;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.TopNOrderedContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ratis.thirdparty.com.google.protobuf
    .InvalidProtocolBufferException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BackgroundTask;
import org.apache.hadoop.utils.BackgroundTaskQueue;
import org.apache.hadoop.utils.BackgroundTaskResult;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER_DEFAULT;

/**
 * A per-datanode container block deleting service takes in charge
 * of deleting staled ozone blocks.
 */
// TODO: Fix BlockDeletingService to work with new StorageLayer
public class BlockDeletingService extends BackgroundService{

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockDeletingService.class);

  private ContainerSet containerSet;
  private ContainerDeletionChoosingPolicy containerDeletionPolicy;
  private final Configuration conf;

  // Throttle number of blocks to delete per task,
  // set to 1 for testing
  private final int blockLimitPerTask;

  // Throttle the number of containers to process concurrently at a time,
  private final int containerLimitPerInterval;

  // Task priority is useful when a to-delete block has weight.
  private final static int TASK_PRIORITY_DEFAULT = 1;
  // Core pool size for container tasks
  private final static int BLOCK_DELETING_SERVICE_CORE_POOL_SIZE = 10;

  public BlockDeletingService(ContainerSet containerSet, long serviceInterval,
      long serviceTimeout, TimeUnit timeUnit, Configuration conf) {
    super("BlockDeletingService", serviceInterval, timeUnit,
        BLOCK_DELETING_SERVICE_CORE_POOL_SIZE, serviceTimeout);
    this.containerSet = containerSet;
    containerDeletionPolicy = ReflectionUtils.newInstance(conf.getClass(
        ScmConfigKeys.OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY,
        TopNOrderedContainerDeletionChoosingPolicy.class,
        ContainerDeletionChoosingPolicy.class), conf);
    this.conf = conf;
    this.blockLimitPerTask = conf.getInt(
        OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER,
        OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER_DEFAULT);
    this.containerLimitPerInterval = conf.getInt(
        OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL,
        OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL_DEFAULT);
  }


  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    List<ContainerData> containers = Lists.newArrayList();
    try {
      // We at most list a number of containers a time,
      // in case there are too many containers and start too many workers.
      // We must ensure there is no empty container in this result.
      // The chosen result depends on what container deletion policy is
      // configured.
      containers = containerSet.chooseContainerForBlockDeletion(
          containerLimitPerInterval, containerDeletionPolicy);
      if (containers.size() > 0) {
        LOG.info("Plan to choose {} containers for block deletion, "
                + "actually returns {} valid containers.",
            containerLimitPerInterval, containers.size());
      }

      for(ContainerData container : containers) {
        BlockDeletingTask containerTask =
            new BlockDeletingTask(container, TASK_PRIORITY_DEFAULT);
        queue.add(containerTask);
      }
    } catch (StorageContainerException e) {
      LOG.warn("Failed to initiate block deleting tasks, "
          + "caused by unable to get containers info. "
          + "Retry in next interval. ", e);
    } catch (Exception e) {
      // In case listContainer call throws any uncaught RuntimeException.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unexpected error occurs during deleting blocks.", e);
      }
    }
    return queue;
  }

  private static class ContainerBackgroundTaskResult
      implements BackgroundTaskResult {
    private List<String> deletedBlockIds;

    ContainerBackgroundTaskResult() {
      deletedBlockIds = new LinkedList<>();
    }

    public void addBlockId(String blockId) {
      deletedBlockIds.add(blockId);
    }

    public void addAll(List<String> blockIds) {
      deletedBlockIds.addAll(blockIds);
    }

    public List<String> getDeletedBlocks() {
      return deletedBlockIds;
    }

    @Override
    public int getSize() {
      return deletedBlockIds.size();
    }
  }

  private class BlockDeletingTask
      implements BackgroundTask<BackgroundTaskResult> {

    private final int priority;
    private final KeyValueContainerData containerData;

    BlockDeletingTask(ContainerData containerName, int priority) {
      this.priority = priority;
      this.containerData = (KeyValueContainerData) containerName;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      ContainerBackgroundTaskResult crr = new ContainerBackgroundTaskResult();
      long startTime = Time.monotonicNow();
      // Scan container's db and get list of under deletion blocks
      try (ReferenceCountedDB meta = BlockUtils.getDB(containerData, conf)) {
        // # of blocks to delete is throttled
        KeyPrefixFilter filter =
            new KeyPrefixFilter().addFilter(OzoneConsts.DELETING_KEY_PREFIX);
        List<Map.Entry<byte[], byte[]>> toDeleteBlocks =
            meta.getStore().getSequentialRangeKVs(null, blockLimitPerTask,
                filter);
        if (toDeleteBlocks.isEmpty()) {
          LOG.debug("No under deletion block found in container : {}",
              containerData.getContainerID());
        }

        List<String> succeedBlocks = new LinkedList<>();
        LOG.debug("Container : {}, To-Delete blocks : {}",
            containerData.getContainerID(), toDeleteBlocks.size());
        File dataDir = new File(containerData.getChunksPath());
        if (!dataDir.exists() || !dataDir.isDirectory()) {
          LOG.error("Invalid container data dir {} : "
              + "does not exist or not a directory", dataDir.getAbsolutePath());
          return crr;
        }

        toDeleteBlocks.forEach(entry -> {
          String blockName = DFSUtil.bytes2String(entry.getKey());
          LOG.debug("Deleting block {}", blockName);
          try {
            ContainerProtos.BlockData data =
                ContainerProtos.BlockData.parseFrom(entry.getValue());
            for (ContainerProtos.ChunkInfo chunkInfo : data.getChunksList()) {
              File chunkFile = dataDir.toPath()
                  .resolve(chunkInfo.getChunkName()).toFile();
              if (FileUtils.deleteQuietly(chunkFile)) {
                LOG.debug("block {} chunk {} deleted", blockName,
                    chunkFile.getAbsolutePath());
              }
            }
            succeedBlocks.add(blockName);
          } catch (InvalidProtocolBufferException e) {
            LOG.error("Failed to parse block info for block {}", blockName, e);
          }
        });

        // Once files are deleted... replace deleting entries with deleted
        // entries
        BatchOperation batch = new BatchOperation();
        succeedBlocks.forEach(entry -> {
          String blockId =
              entry.substring(OzoneConsts.DELETING_KEY_PREFIX.length());
          String deletedEntry = OzoneConsts.DELETED_KEY_PREFIX + blockId;
          batch.put(DFSUtil.string2Bytes(deletedEntry),
              DFSUtil.string2Bytes(blockId));
          batch.delete(DFSUtil.string2Bytes(entry));
        });
        meta.getStore().writeBatch(batch);
        // update count of pending deletion blocks in in-memory container status
        containerData.decrPendingDeletionBlocks(succeedBlocks.size());

        if (!succeedBlocks.isEmpty()) {
          LOG.info("Container: {}, deleted blocks: {}, task elapsed time: {}ms",
              containerData.getContainerID(), succeedBlocks.size(),
              Time.monotonicNow() - startTime);
        }
        crr.addAll(succeedBlocks);
        return crr;
      }
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }
}
