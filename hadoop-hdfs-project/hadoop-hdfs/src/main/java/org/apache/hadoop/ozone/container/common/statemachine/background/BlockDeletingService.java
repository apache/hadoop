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

package org.apache.hadoop.ozone.container.common.statemachine.background;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BackgroundTaskResult;
import org.apache.hadoop.utils.BackgroundTaskQueue;
import org.apache.hadoop.utils.BackgroundTask;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL_DEFAULT;

/**
 * A per-datanode container block deleting service takes in charge
 * of deleting staled ozone blocks.
 */
public class BlockDeletingService extends BackgroundService{

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockDeletingService.class);

  private final ContainerManager containerManager;
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

  public BlockDeletingService(ContainerManager containerManager,
      int serviceInterval, long serviceTimeout, Configuration conf) {
    super("BlockDeletingService", serviceInterval,
        TimeUnit.MILLISECONDS, BLOCK_DELETING_SERVICE_CORE_POOL_SIZE,
        serviceTimeout);
    this.containerManager = containerManager;
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
      containerManager.listContainer(null, containerLimitPerInterval,
          null, containers);

      // TODO
      // in case we always fetch a few same containers,
      // should we list some more containers a time and shuffle them?
      for(ContainerData container : containers) {
        BlockDeletingTask containerTask =
            new BlockDeletingTask(container, TASK_PRIORITY_DEFAULT);
        queue.add(containerTask);
      }
    } catch (StorageContainerException e) {
      LOG.warn("Failed to initiate block deleting tasks, "
          + "caused by unable to get containers info. "
          + "Retry in next interval. ", e);
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
    private final ContainerData containerData;

    BlockDeletingTask(ContainerData containerName, int priority) {
      this.priority = priority;
      this.containerData = containerName;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      long startTime = Time.monotonicNow();
      // Scan container's db and get list of under deletion blocks
      MetadataStore meta = KeyUtils.getDB(containerData, conf);
      // # of blocks to delete is throttled
      KeyPrefixFilter filter = new KeyPrefixFilter(
          OzoneConsts.DELETING_KEY_PREFIX);
      List<Map.Entry<byte[], byte[]>> toDeleteBlocks =
          meta.getRangeKVs(null, blockLimitPerTask, filter);
      if (toDeleteBlocks.isEmpty()) {
        LOG.debug("No under deletion block found in container : {}",
            containerData.getContainerName());
      }

      List<String> succeedBlocks = new LinkedList<>();
      LOG.debug("Container : {}, To-Delete blocks : {}",
          containerData.getContainerName(), toDeleteBlocks.size());
      toDeleteBlocks.forEach(entry -> {
        String blockName = DFSUtil.bytes2String(entry.getKey());
        LOG.debug("Deleting block {}", blockName);
        try {
          ContainerProtos.KeyData data =
              ContainerProtos.KeyData.parseFrom(entry.getValue());

          for (ContainerProtos.ChunkInfo chunkInfo : data.getChunksList()) {
            File chunkFile = new File(chunkInfo.getChunkName());
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

      // Once files are deleted ... clean up DB
      BatchOperation batch = new BatchOperation();
      succeedBlocks.forEach(entry ->
          batch.delete(DFSUtil.string2Bytes(entry)));
      meta.writeBatch(batch);

      LOG.info("The elapsed time of task@{} for"
          + " deleting blocks: {}ms.",
          Integer.toHexString(this.hashCode()),
          Time.monotonicNow() - startTime);
      ContainerBackgroundTaskResult crr = new ContainerBackgroundTaskResult();
      crr.addAll(succeedBlocks);
      return crr;
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }
}
