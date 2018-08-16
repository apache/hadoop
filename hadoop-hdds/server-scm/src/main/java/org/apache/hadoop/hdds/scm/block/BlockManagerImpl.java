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
package org.apache.hadoop.hdds.scm.block;

import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.Mapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .CHILL_MODE_EXCEPTION;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .INVALID_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;

/** Block Manager manages the block access for SCM. */
public class BlockManagerImpl implements BlockManager, BlockmanagerMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockManagerImpl.class);
  // TODO : FIX ME : Hard coding the owner.
  // Currently only user of the block service is Ozone, CBlock manages blocks
  // by itself and does not rely on the Block service offered by SCM.

  private final NodeManager nodeManager;
  private final Mapping containerManager;

  private final Lock lock;
  private final long containerSize;

  private final DeletedBlockLog deletedBlockLog;
  private final SCMBlockDeletingService blockDeletingService;

  private final int containerProvisionBatchSize;
  private final Random rand;
  private ObjectName mxBean;

  /**
   * Constructor.
   *
   * @param conf - configuration.
   * @param nodeManager - node manager.
   * @param containerManager - container manager.
   * @param eventPublisher - event publisher.
   * @throws IOException
   */
  public BlockManagerImpl(final Configuration conf,
      final NodeManager nodeManager, final Mapping containerManager,
      EventPublisher eventPublisher)
      throws IOException {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;

    this.containerSize = OzoneConsts.GB * conf.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);

    this.containerProvisionBatchSize =
        conf.getInt(
            ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE_DEFAULT);
    rand = new Random();
    this.lock = new ReentrantLock();

    mxBean = MBeans.register("BlockManager", "BlockManagerImpl", this);

    // SCM block deleting transaction log and deleting service.
    deletedBlockLog = new DeletedBlockLogImpl(conf, containerManager);
    long svcInterval =
        conf.getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
            OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    long serviceTimeout =
        conf.getTimeDuration(
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    blockDeletingService =
        new SCMBlockDeletingService(deletedBlockLog, containerManager,
            nodeManager, eventPublisher, svcInterval, serviceTimeout, conf);
  }

  /**
   * Start block manager services.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    this.blockDeletingService.start();
  }

  /**
   * Shutdown block manager services.
   *
   * @throws IOException
   */
  public void stop() throws IOException {
    this.blockDeletingService.shutdown();
    this.close();
  }

  /**
   * Pre allocate specified count of containers for block creation.
   *
   * @param count - Number of containers to allocate.
   * @param type - Type of containers
   * @param factor - how many copies needed for this container.
   * @throws IOException
   */
  private void preAllocateContainers(int count, ReplicationType type,
      ReplicationFactor factor, String owner)
      throws IOException {
    lock.lock();
    try {
      for (int i = 0; i < count; i++) {
        ContainerWithPipeline containerWithPipeline = null;
        try {
          // TODO: Fix this later when Ratis is made the Default.
          containerWithPipeline = containerManager.allocateContainer(type, factor,
              owner);

          if (containerWithPipeline == null) {
            LOG.warn("Unable to allocate container.");
            continue;
          }
        } catch (IOException ex) {
          LOG.warn("Unable to allocate container: {}", ex);
          continue;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Allocates a block in a container and returns that info.
   *
   * @param size - Block Size
   * @param type Replication Type
   * @param factor - Replication Factor
   * @return Allocated block
   * @throws IOException on failure.
   */
  @Override
  public AllocatedBlock allocateBlock(final long size,
      ReplicationType type, ReplicationFactor factor, String owner)
      throws IOException {
    LOG.trace("Size;{} , type : {}, factor : {} ", size, type, factor);

    if (size < 0 || size > containerSize) {
      LOG.warn("Invalid block size requested : {}", size);
      throw new SCMException("Unsupported block size: " + size,
          INVALID_BLOCK_SIZE);
    }

    if (!nodeManager.isOutOfChillMode()) {
      LOG.warn("Not out of Chill mode.");
      throw new SCMException("Unable to create block while in chill mode",
          CHILL_MODE_EXCEPTION);
    }

    lock.lock();
    try {
      /*
               Here is the high level logic.

               1. First we check if there are containers in ALLOCATED state,
               that is
                SCM has allocated them in the SCM namespace but the
                corresponding
                container has not been created in the Datanode yet. If we
                have any
                in that state, we will return that to the client, which allows
                client to finish creating those containers. This is a sort of
                 greedy
                 algorithm, our primary purpose is to get as many containers as
                 possible.

                2. If there are no allocated containers -- Then we find a Open
                container that matches that pattern.

                3. If both of them fail, the we will pre-allocate a bunch of
                conatainers in SCM and try again.

               TODO : Support random picking of two containers from the list.
                So we
               can use different kind of policies.
      */

      ContainerWithPipeline containerWithPipeline;

      // Look for ALLOCATED container that matches all other parameters.
      containerWithPipeline = containerManager
          .getMatchingContainerWithPipeline(size, owner, type, factor,
              HddsProtos.LifeCycleState.ALLOCATED);
      if (containerWithPipeline != null) {
        containerManager.updateContainerState(
            containerWithPipeline.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
        return newBlock(containerWithPipeline,
            HddsProtos.LifeCycleState.ALLOCATED);
      }

      // Since we found no allocated containers that match our criteria, let us
      // look for OPEN containers that match the criteria.
      containerWithPipeline = containerManager
          .getMatchingContainerWithPipeline(size, owner, type, factor,
              HddsProtos.LifeCycleState.OPEN);
      if (containerWithPipeline != null) {
        return newBlock(containerWithPipeline, HddsProtos.LifeCycleState.OPEN);
      }

      // We found neither ALLOCATED or OPEN Containers. This generally means
      // that most of our containers are full or we have not allocated
      // containers of the type and replication factor. So let us go and
      // allocate some.
      preAllocateContainers(containerProvisionBatchSize, type, factor, owner);

      // Since we just allocated a set of containers this should work
      containerWithPipeline = containerManager
          .getMatchingContainerWithPipeline(size, owner, type, factor,
              HddsProtos.LifeCycleState.ALLOCATED);
      if (containerWithPipeline != null) {
        containerManager.updateContainerState(
            containerWithPipeline.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
        return newBlock(containerWithPipeline,
            HddsProtos.LifeCycleState.ALLOCATED);
      }

      // we have tried all strategies we know and but somehow we are not able
      // to get a container for this block. Log that info and return a null.
      LOG.error(
          "Unable to allocate a block for the size: {}, type: {}, " +
              "factor: {}",
          size,
          type,
          factor);
      return null;
    } finally {
      lock.unlock();
    }
  }

  private String getChannelName(ReplicationType type) {
    switch (type) {
      case RATIS:
        return "RA" + UUID.randomUUID().toString().substring(3);
      case STAND_ALONE:
        return "SA" + UUID.randomUUID().toString().substring(3);
      default:
        return "RA" + UUID.randomUUID().toString().substring(3);
    }
  }

  /**
   * newBlock - returns a new block assigned to a container.
   *
   * @param containerWithPipeline - Container Info.
   * @param state - Current state of the container.
   * @return AllocatedBlock
   */
  private AllocatedBlock newBlock(ContainerWithPipeline containerWithPipeline,
      HddsProtos.LifeCycleState state) throws IOException {
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();
    if (containerWithPipeline.getPipeline().getDatanodes().size() == 0) {
      LOG.error("Pipeline Machine count is zero.");
      return null;
    }

    // TODO : Revisit this local ID allocation when HA is added.
    // TODO: this does not work well if multiple allocation kicks in a tight
    // loop.
    long localID = Time.getUtcTime();
    long containerID = containerInfo.getContainerID();

    boolean createContainer = (state == HddsProtos.LifeCycleState.ALLOCATED);

    AllocatedBlock.Builder abb =
        new AllocatedBlock.Builder()
            .setBlockID(new BlockID(containerID, localID))
            .setPipeline(containerWithPipeline.getPipeline())
            .setShouldCreateContainer(createContainer);
    LOG.trace("New block allocated : {} Container ID: {}", localID,
        containerID);
    return abb.build();
  }

  /**
   * Deletes a list of blocks in an atomic operation. Internally, SCM writes
   * these blocks into a
   * {@link DeletedBlockLog} and deletes them from SCM DB. If this is
   * successful, given blocks are
   * entering pending deletion state and becomes invisible from SCM namespace.
   *
   * @param blockIDs block IDs. This is often the list of blocks of a
   * particular object key.
   * @throws IOException if exception happens, non of the blocks is deleted.
   */
  @Override
  public void deleteBlocks(List<BlockID> blockIDs) throws IOException {
    if (!nodeManager.isOutOfChillMode()) {
      throw new SCMException("Unable to delete block while in chill mode",
          CHILL_MODE_EXCEPTION);
    }

    lock.lock();
    LOG.info("Deleting blocks {}", StringUtils.join(",", blockIDs));
    Map<Long, List<Long>> containerBlocks = new HashMap<>();
    // TODO: track the block size info so that we can reclaim the container
    // TODO: used space when the block is deleted.
    try {
      for (BlockID block : blockIDs) {
        // Merge blocks to a container to blocks mapping,
        // prepare to persist this info to the deletedBlocksLog.
        long containerID = block.getContainerID();
        if (containerBlocks.containsKey(containerID)) {
          containerBlocks.get(containerID).add(block.getLocalID());
        } else {
          List<Long> item = new ArrayList<>();
          item.add(block.getLocalID());
          containerBlocks.put(containerID, item);
        }
      }

      try {
        deletedBlockLog.addTransactions(containerBlocks);
      } catch (IOException e) {
        throw new IOException(
            "Skip writing the deleted blocks info to"
                + " the delLog because addTransaction fails. Batch skipped: "
                + StringUtils.join(",", blockIDs),
            e);
      }
      // TODO: Container report handling of the deleted blocks:
      // Remove tombstone and update open container usage.
      // We will revisit this when the closed container replication is done.
    } finally {
      lock.unlock();
    }
  }

  @Override
  public DeletedBlockLog getDeletedBlockLog() {
    return this.deletedBlockLog;
  }

  /**
   * Close the resources for BlockManager.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (deletedBlockLog != null) {
      deletedBlockLog.close();
    }
    blockDeletingService.shutdown();
    if (mxBean != null) {
      MBeans.unregister(mxBean);
      mxBean = null;
    }
  }

  @Override
  public int getOpenContainersNo() {
    return 0;
    // TODO : FIX ME : The open container being a single number does not make
    // sense.
    // We have to get open containers by Replication Type and Replication
    // factor. Hence returning 0 for now.
    // containers.get(HddsProtos.LifeCycleState.OPEN).size();
  }

  @Override
  public SCMBlockDeletingService getSCMBlockDeletingService() {
    return this.blockDeletingService;
  }
}
