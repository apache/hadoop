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
package org.apache.hadoop.ozone.scm.block;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationFactor;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationType;
import org.apache.hadoop.ozone.scm.container.Mapping;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;

import static org.apache.hadoop.ozone.web.util.ServerUtils.getOzoneMetaDirPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_DB;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes
    .CHILL_MODE_EXCEPTION;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_FIND_BLOCK;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes
    .INVALID_BLOCK_SIZE;

/** Block Manager manages the block access for SCM. */
public class BlockManagerImpl implements BlockManager, BlockmanagerMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockManagerImpl.class);
  // TODO : FIX ME : Hard coding the owner.
  // Currently only user of the block service is Ozone, CBlock manages blocks
  // by itself and does not rely on the Block service offered by SCM.

  private final NodeManager nodeManager;
  private final Mapping containerManager;
  private final MetadataStore blockStore;

  private final Lock lock;
  private final long containerSize;
  private final long cacheSize;

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
   * @param cacheSizeMB - cache size for level db store.
   * @throws IOException
   */
  public BlockManagerImpl(final Configuration conf,
      final NodeManager nodeManager, final Mapping containerManager,
      final int cacheSizeMB) throws IOException {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.cacheSize = cacheSizeMB;

    this.containerSize = OzoneConsts.GB * conf.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);
    File metaDir = getOzoneMetaDirPath(conf);
    String scmMetaDataDir = metaDir.getPath();

    // Write the block key to container name mapping.
    File blockContainerDbPath = new File(scmMetaDataDir, BLOCK_DB);
    blockStore =
        MetadataStoreBuilder.newBuilder()
            .setConf(conf)
            .setDbFile(blockContainerDbPath)
            .setCacheSize(this.cacheSize * OzoneConsts.MB)
            .build();

    this.containerProvisionBatchSize =
        conf.getInt(
            ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE_DEFAULT);
    rand = new Random();
    this.lock = new ReentrantLock();

    mxBean = MBeans.register("BlockManager", "BlockManagerImpl", this);

    // SCM block deleting transaction log and deleting service.
    deletedBlockLog = new DeletedBlockLogImpl(conf);
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
        new SCMBlockDeletingService(
            deletedBlockLog, containerManager, nodeManager, svcInterval,
            serviceTimeout, conf);
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
        String containerName = UUID.randomUUID().toString();
        ContainerInfo containerInfo = null;
        try {
          // TODO: Fix this later when Ratis is made the Default.
          containerInfo = containerManager.allocateContainer(type, factor,
              containerName, owner);

          if (containerInfo == null) {
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

      ContainerInfo containerInfo;

      // Look for ALLOCATED container that matches all other parameters.
      containerInfo =
          containerManager
              .getStateManager()
              .getMatchingContainer(
                  size, owner, type, factor, HdslProtos.LifeCycleState
                      .ALLOCATED);
      if (containerInfo != null) {
        containerManager.updateContainerState(containerInfo.getContainerName(),
            HdslProtos.LifeCycleEvent.CREATE);
        return newBlock(containerInfo, HdslProtos.LifeCycleState.ALLOCATED);
      }

      // Since we found no allocated containers that match our criteria, let us
      // look for OPEN containers that match the criteria.
      containerInfo =
          containerManager
              .getStateManager()
              .getMatchingContainer(size, owner, type, factor, HdslProtos
                  .LifeCycleState.OPEN);
      if (containerInfo != null) {
        return newBlock(containerInfo, HdslProtos.LifeCycleState.OPEN);
      }

      // We found neither ALLOCATED or OPEN Containers. This generally means
      // that most of our containers are full or we have not allocated
      // containers of the type and replication factor. So let us go and
      // allocate some.
      preAllocateContainers(containerProvisionBatchSize, type, factor, owner);

      // Since we just allocated a set of containers this should work
      containerInfo =
          containerManager
              .getStateManager()
              .getMatchingContainer(
                  size, owner, type, factor, HdslProtos.LifeCycleState
                      .ALLOCATED);
      if (containerInfo != null) {
        containerManager.updateContainerState(containerInfo.getContainerName(),
            HdslProtos.LifeCycleEvent.CREATE);
        return newBlock(containerInfo, HdslProtos.LifeCycleState.ALLOCATED);
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

  /**
   * newBlock - returns a new block assigned to a container.
   *
   * @param containerInfo - Container Info.
   * @param state - Current state of the container.
   * @return AllocatedBlock
   */
  private AllocatedBlock newBlock(
      ContainerInfo containerInfo, HdslProtos.LifeCycleState state)
      throws IOException {

    // TODO : Replace this with Block ID.
    String blockKey = UUID.randomUUID().toString();
    boolean createContainer = (state == HdslProtos.LifeCycleState.ALLOCATED);

    AllocatedBlock.Builder abb =
        new AllocatedBlock.Builder()
            .setKey(blockKey)
            // TODO : Use containerinfo instead of pipeline.
            .setPipeline(containerInfo.getPipeline())
            .setShouldCreateContainer(createContainer);
    LOG.trace("New block allocated : {} Container ID: {}", blockKey,
        containerInfo.toString());

    if (containerInfo.getPipeline().getMachines().size() == 0) {
      LOG.error("Pipeline Machine count is zero.");
      return null;
    }

    // Persist this block info to the blockStore DB, so getBlock(key) can
    // find which container the block lives.
    // TODO : Remove this DB in future
    // and make this a KSM operation. Category: SCALABILITY.
    if (containerInfo.getPipeline().getMachines().size() > 0) {
      blockStore.put(
          DFSUtil.string2Bytes(blockKey),
          DFSUtil.string2Bytes(containerInfo.getPipeline().getContainerName()));
    }
    return abb.build();
  }

  /**
   * Given a block key, return the Pipeline information.
   *
   * @param key - block key assigned by SCM.
   * @return Pipeline (list of DNs and leader) to access the block.
   * @throws IOException
   */
  @Override
  public Pipeline getBlock(final String key) throws IOException {
    lock.lock();
    try {
      byte[] containerBytes = blockStore.get(DFSUtil.string2Bytes(key));
      if (containerBytes == null) {
        throw new SCMException(
            "Specified block key does not exist. key : " + key,
            FAILED_TO_FIND_BLOCK);
      }

      String containerName = DFSUtil.bytes2String(containerBytes);
      ContainerInfo containerInfo = containerManager.getContainer(
          containerName);
      if (containerInfo == null) {
        LOG.debug("Container {} allocated by block service"
            + "can't be found in SCM", containerName);
        throw new SCMException(
            "Unable to find container for the block",
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }
      return containerInfo.getPipeline();
    } finally {
      lock.unlock();
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
  public void deleteBlocks(List<String> blockIDs) throws IOException {
    if (!nodeManager.isOutOfChillMode()) {
      throw new SCMException("Unable to delete block while in chill mode",
          CHILL_MODE_EXCEPTION);
    }

    lock.lock();
    LOG.info("Deleting blocks {}", String.join(",", blockIDs));
    Map<String, List<String>> containerBlocks = new HashMap<>();
    BatchOperation batch = new BatchOperation();
    BatchOperation rollbackBatch = new BatchOperation();
    // TODO: track the block size info so that we can reclaim the container
    // TODO: used space when the block is deleted.
    try {
      for (String blockKey : blockIDs) {
        byte[] blockKeyBytes = DFSUtil.string2Bytes(blockKey);
        byte[] containerBytes = blockStore.get(blockKeyBytes);
        if (containerBytes == null) {
          throw new SCMException(
              "Specified block key does not exist. key : " + blockKey,
              FAILED_TO_FIND_BLOCK);
        }
        batch.delete(blockKeyBytes);
        rollbackBatch.put(blockKeyBytes, containerBytes);

        // Merge blocks to a container to blocks mapping,
        // prepare to persist this info to the deletedBlocksLog.
        String containerName = DFSUtil.bytes2String(containerBytes);
        if (containerBlocks.containsKey(containerName)) {
          containerBlocks.get(containerName).add(blockKey);
        } else {
          List<String> item = new ArrayList<>();
          item.add(blockKey);
          containerBlocks.put(containerName, item);
        }
      }

      // We update SCM DB first, so if this step fails, we end up here,
      // nothing gets into the delLog so no blocks will be accidentally
      // removed. If we write the log first, once log is written, the
      // async deleting service will start to scan and might be picking
      // up some blocks to do real deletions, that might cause data loss.
      blockStore.writeBatch(batch);
      try {
        deletedBlockLog.addTransactions(containerBlocks);
      } catch (IOException e) {
        try {
          // If delLog update is failed, we need to rollback the changes.
          blockStore.writeBatch(rollbackBatch);
        } catch (IOException rollbackException) {
          // This is a corner case. AddTX fails and rollback also fails,
          // this will leave these blocks in inconsistent state. They were
          // moved to pending deletion state in SCM DB but were not written
          // into delLog so real deletions would not be done. Blocks become
          // to be invisible from namespace but actual data are not removed.
          // We log an error here so admin can manually check and fix such
          // errors.
          LOG.error(
              "Blocks might be in inconsistent state because"
                  + " they were moved to pending deletion state in SCM DB but"
                  + " not written into delLog. Admin can manually add them"
                  + " into delLog for deletions. Inconsistent block list: {}",
              String.join(",", blockIDs),
              e);
          throw rollbackException;
        }
        throw new IOException(
            "Skip writing the deleted blocks info to"
                + " the delLog because addTransaction fails. Batch skipped: "
                + String.join(",", blockIDs),
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

  @VisibleForTesting
  public String getDeletedKeyName(String key) {
    return StringUtils.format(".Deleted/%s", key);
  }

  /**
   * Close the resources for BlockManager.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (blockStore != null) {
      blockStore.close();
    }
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
    // containers.get(HdslProtos.LifeCycleState.OPEN).size();
  }

  @Override
  public SCMBlockDeletingService getSCMBlockDeletingService() {
    return this.blockDeletingService;
  }
}
