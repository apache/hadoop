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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.chillmode.ChillModePrecheck;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.util.StringUtils;
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
public class BlockManagerImpl implements EventHandler<Boolean>,
    BlockManager, BlockmanagerMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockManagerImpl.class);
  // TODO : FIX ME : Hard coding the owner.
  // Currently only user of the block service is Ozone, CBlock manages blocks
  // by itself and does not rely on the Block service offered by SCM.

  private final PipelineManager pipelineManager;
  private final ContainerManager containerManager;

  private final long containerSize;

  private final DeletedBlockLog deletedBlockLog;
  private final SCMBlockDeletingService blockDeletingService;

  private final int containerProvisionBatchSize;
  private final Random rand;
  private ObjectName mxBean;
  private ChillModePrecheck chillModePrecheck;

  /**
   * Constructor.
   *
   * @param conf - configuration.
   * @param nodeManager - node manager.
   * @param pipelineManager - pipeline manager.
   * @param containerManager - container manager.
   * @param eventPublisher - event publisher.
   * @throws IOException
   */
  public BlockManagerImpl(final Configuration conf,
      final NodeManager nodeManager, final PipelineManager pipelineManager,
      final ContainerManager containerManager, EventPublisher eventPublisher)
      throws IOException {
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;

    this.containerSize = (long)conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);

    this.containerProvisionBatchSize =
        conf.getInt(
            ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE_DEFAULT);
    rand = new Random();

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
    chillModePrecheck = new ChillModePrecheck(conf);
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
  private synchronized void preAllocateContainers(int count,
      ReplicationType type, ReplicationFactor factor, String owner) {
    for (int i = 0; i < count; i++) {
      ContainerInfo containerInfo;
      try {
        // TODO: Fix this later when Ratis is made the Default.
        containerInfo = containerManager.allocateContainer(
            type, factor, owner);

        if (containerInfo == null) {
          LOG.warn("Unable to allocate container.");
        }
      } catch (IOException ex) {
        LOG.warn("Unable to allocate container.", ex);
      }
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
    ScmUtils.preCheck(ScmOps.allocateBlock, chillModePrecheck);
    if (size < 0 || size > containerSize) {
      LOG.warn("Invalid block size requested : {}", size);
      throw new SCMException("Unsupported block size: " + size,
          INVALID_BLOCK_SIZE);
    }

    /*
      Here is the high level logic.

      1. We try to find containers in open state.

      2. If there are no containers in open state, then we will pre-allocate a
      bunch of containers in SCM and try again.

      TODO : Support random picking of two containers from the list. So we can
             use different kind of policies.
    */

    ContainerInfo containerInfo;

    // look for OPEN containers that match the criteria.
    containerInfo = containerManager
        .getMatchingContainer(size, owner, type, factor,
            HddsProtos.LifeCycleState.OPEN);

    // We did not find OPEN Containers. This generally means
    // that most of our containers are full or we have not allocated
    // containers of the type and replication factor. So let us go and
    // allocate some.

    // Even though we have already checked the containers in OPEN
    // state, we have to check again as we only hold a read lock.
    // Some other thread might have pre-allocated container in meantime.
    if (containerInfo == null) {
      synchronized (this) {
        if (!containerManager.getContainers(HddsProtos.LifeCycleState.OPEN)
            .isEmpty()) {
          containerInfo = containerManager
              .getMatchingContainer(size, owner, type, factor,
                  HddsProtos.LifeCycleState.OPEN);
        }

        if (containerInfo == null) {
          preAllocateContainers(containerProvisionBatchSize, type, factor,
              owner);
          containerInfo = containerManager
              .getMatchingContainer(size, owner, type, factor,
                  HddsProtos.LifeCycleState.OPEN);
        }
      }
    }

    if (containerInfo != null) {
      return newBlock(containerInfo);
    }

    // we have tried all strategies we know and but somehow we are not able
    // to get a container for this block. Log that info and return a null.
    LOG.error(
        "Unable to allocate a block for the size: {}, type: {}, factor: {}",
        size, type, factor);
    return null;
  }

  /**
   * newBlock - returns a new block assigned to a container.
   *
   * @param containerInfo - Container Info.
   * @return AllocatedBlock
   */
  private AllocatedBlock newBlock(ContainerInfo containerInfo) {
    try {
      final Pipeline pipeline = pipelineManager
          .getPipeline(containerInfo.getPipelineID());
      // TODO : Revisit this local ID allocation when HA is added.
      long localID = UniqueId.next();
      long containerID = containerInfo.getContainerID();
      AllocatedBlock.Builder abb =  new AllocatedBlock.Builder()
          .setContainerBlockID(new ContainerBlockID(containerID, localID))
          .setPipeline(pipeline);
      LOG.trace("New block allocated : {} Container ID: {}", localID,
          containerID);
      return abb.build();
    } catch (PipelineNotFoundException ex) {
      LOG.error("Pipeline Machine count is zero.", ex);
      return null;
    }
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
    ScmUtils.preCheck(ScmOps.deleteBlock, chillModePrecheck);

    LOG.info("Deleting blocks {}", StringUtils.join(",", blockIDs));
    Map<Long, List<Long>> containerBlocks = new HashMap<>();
    // TODO: track the block size info so that we can reclaim the container
    // TODO: used space when the block is deleted.
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
              + StringUtils.join(",", blockIDs), e);
    }
    // TODO: Container report handling of the deleted blocks:
    // Remove tombstone and update open container usage.
    // We will revisit this when the closed container replication is done.
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

  @Override
  public void onMessage(Boolean inChillMode, EventPublisher publisher) {
    this.chillModePrecheck.setInChillMode(inChillMode);
  }

  /**
   * Returns status of scm chill mode determined by CHILL_MODE_STATUS event.
   * */
  public boolean isScmInChillMode() {
    return this.chillModePrecheck.isInChillMode();
  }

  /**
   * Get class logger.
   * */
  public static Logger getLogger() {
    return LOG;
  }

  /**
   * This class uses system current time milliseconds to generate unique id.
   */
  public static final class UniqueId {
    /*
     * When we represent time in milliseconds using 'long' data type,
     * the LSB bits are used. Currently we are only using 44 bits (LSB),
     * 20 bits (MSB) are not used.
     * We will exhaust this 44 bits only when we are in year 2525,
     * until then we can safely use this 20 bits (MSB) for offset to generate
     * unique id within millisecond.
     *
     * Year        : Mon Dec 31 18:49:04 IST 2525
     * TimeInMillis: 17545641544247
     * Binary Representation:
     *   MSB (20 bits): 0000 0000 0000 0000 0000
     *   LSB (44 bits): 1111 1111 0101 0010 1001 1011 1011 0100 1010 0011 0111
     *
     * We have 20 bits to run counter, we should exclude the first bit (MSB)
     * as we don't want to deal with negative values.
     * To be on safer side we will use 'short' data type which is of length
     * 16 bits and will give us 65,536 values for offset.
     *
     */

    private static volatile short offset = 0;

    /**
     * Private constructor so that no one can instantiate this class.
     */
    private UniqueId() {}

    /**
     * Calculate and returns next unique id based on System#currentTimeMillis.
     *
     * @return unique long value
     */
    public static synchronized long next() {
      long utcTime = HddsUtils.getUtcTime();
      if ((utcTime & 0xFFFF000000000000L) == 0) {
        return utcTime << Short.SIZE | (offset++ & 0x0000FFFF);
      }
      throw new RuntimeException("Got invalid UTC time," +
          " cannot generate unique Id. UTC Time: " + utcTime);
    }
  }
}
