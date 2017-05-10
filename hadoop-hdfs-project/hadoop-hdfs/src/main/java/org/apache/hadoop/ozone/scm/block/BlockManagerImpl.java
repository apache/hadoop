/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.scm.block;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.scm.container.Mapping;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_DB;
import static org.apache.hadoop.ozone.OzoneConsts.OPEN_CONTAINERS_DB;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException
    .ResultCodes.CHILL_MODE_EXCEPTION;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException
    .ResultCodes.FAILED_TO_ALLOCATE_CONTAINER;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException
    .ResultCodes.FAILED_TO_FIND_CONTAINER;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException
    .ResultCodes.FAILED_TO_FIND_CONTAINER_WITH_SAPCE;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException
    .ResultCodes.FAILED_TO_LOAD_OPEN_CONTAINER;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException
    .ResultCodes.INVALID_BLOCK_SIZE;

/**
 * Block Manager manages the block access for SCM.
 */
public class BlockManagerImpl implements BlockManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockManagerImpl.class);

  private final NodeManager nodeManager;
  private final Mapping containerManager;
  private final LevelDBStore blockStore;

  private final Lock lock;
  private final long containerSize;
  private final long cacheSize;

  private final LevelDBStore openContainerStore;
  private Map<String, Long> openContainers;
  private final int containerProvisionBatchSize;
  private final Random rand;

  /**
   * Constructor.
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
    String scmMetaDataDir = conf.get(OzoneConfigKeys
        .OZONE_CONTAINER_METADATA_DIRS);
    if ((scmMetaDataDir == null) || (scmMetaDataDir.isEmpty())) {
      throw
          new IllegalArgumentException("SCM metadata directory is not valid.");
    }
    Options options = new Options();
    options.cacheSize(this.cacheSize * OzoneConsts.MB);

    // Write the block key to container name mapping.
    File blockContainerDbPath = new File(scmMetaDataDir, BLOCK_DB);
    blockStore = new LevelDBStore(blockContainerDbPath, options);

    this.containerSize = OzoneConsts.GB * conf.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);

    // Load store of all open contains for block allocation
    File openContainsDbPath = new File(scmMetaDataDir, OPEN_CONTAINERS_DB);
    openContainerStore = new LevelDBStore(openContainsDbPath, options);
    openContainers = new HashMap<>();
    loadOpenContainers();

    this.containerProvisionBatchSize = conf.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE_DEFAULT);
    rand = new Random();
    this.lock = new ReentrantLock();
  }

  // TODO: close full (or almost full) containers with a separate thread.
  /**
   * Load open containers from persistent store.
   * @throws IOException
   */
  private void loadOpenContainers() throws IOException {
    try (DBIterator iter = openContainerStore.getIterator()) {
      for (iter.seekToFirst(); iter.hasNext(); iter.next()) {
        try {
          byte[] key = iter.peekNext().getKey();
          String containerName = DFSUtil.bytes2String(key);
          byte[] value = iter.peekNext().getValue();
          Long containerUsed = Long.parseLong(DFSUtil.bytes2String(value));
          openContainers.put(containerName, containerUsed);
          LOG.debug("Loading open container: {} used : {}", containerName,
              containerUsed);
        } catch (Exception ex) {
          LOG.warn("Failed loading open container, continue next...");
        }
      }
    } catch (IOException e) {
      LOG.error("Loading open container store failed." + e);
      throw new SCMException("Failed to load open container store",
          FAILED_TO_LOAD_OPEN_CONTAINER);
    }
  }

  /**
   * Pre-provision specified count of containers for block creation.
   * @param count - number of containers to create.
   * @return list of container names created.
   * @throws IOException
   */
  private List<String> provisionContainers(int count) throws IOException {
    List<String> results = new ArrayList();
    lock.lock();
    try {
      for (int i = 0; i < count; i++) {
        String containerName = UUID.randomUUID().toString();
        try {
          Pipeline pipeline = containerManager.allocateContainer(containerName);
          if (pipeline == null) {
            LOG.warn("Unable to allocate container.");
            continue;
          }
        } catch (IOException ex) {
          LOG.warn("Unable to allocate container: " + ex);
          continue;
        }
        openContainers.put(containerName, 0L);
        openContainerStore.put(DFSUtil.string2Bytes(containerName),
            DFSUtil.string2Bytes(Long.toString(0L)));
        results.add(containerName);
      }
    } finally {
      lock.unlock();
    }
    return results;
  }

  /**
   * Allocates a new block for a given size.
   *
   * SCM choose one of the open containers and returns that as the location for
   * the new block. An open container is a container that is actively written to
   * via replicated log.
   * @param size - size of the block to be allocated
   * @return - the allocated pipeline and key for the block
   * @throws IOException
   */
  @Override
  public AllocatedBlock allocateBlock(final long size) throws IOException {
    boolean createContainer = false;
    Pipeline pipeline;
    if (size < 0 || size > containerSize) {
      throw new SCMException("Unsupported block size",
          INVALID_BLOCK_SIZE);
    }
    if (!nodeManager.isOutOfNodeChillMode()) {
      throw new SCMException("Unable to create block while in chill mode",
          CHILL_MODE_EXCEPTION);
    }

    lock.lock();
    try {
      List<String> candidates;
      if (openContainers.size() == 0) {
        try {
          candidates = provisionContainers(containerProvisionBatchSize);
        } catch (IOException ex) {
          throw new SCMException("Unable to allocate container for the block",
              FAILED_TO_ALLOCATE_CONTAINER);
        }
      } else {
        candidates = openContainers.entrySet().parallelStream()
            .filter(e -> (e.getValue() + size < containerSize))
            .map(e -> e.getKey())
            .collect(Collectors.toList());
      }

      if (candidates.size() == 0) {
        try {
          candidates = provisionContainers(containerProvisionBatchSize);
        } catch (IOException ex) {
          throw new SCMException("Unable to allocate container for the block",
              FAILED_TO_ALLOCATE_CONTAINER);
        }
      }

      if (candidates.size() == 0) {
        throw new SCMException("Fail to find any container to allocate block " +
            "of size " + size + ".", FAILED_TO_FIND_CONTAINER_WITH_SAPCE);
      }

      int randomIdx = rand.nextInt(candidates.size());
      String containerName = candidates.get(randomIdx);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Find {} candidates: {}, picking: {}", candidates.size(),
            candidates.toString(), containerName);
      }

      pipeline = containerManager.getContainer(containerName);
      if (pipeline == null) {
        LOG.debug("Unable to find container for the block");
        throw new SCMException("Unable to find container to allocate block",
            FAILED_TO_FIND_CONTAINER);
      }

      // TODO: make block key easier to debug (e.g., seq no)
      // Allocate key for the block
      String blockKey = UUID.randomUUID().toString();
      AllocatedBlock.Builder abb = new AllocatedBlock.Builder()
          .setKey(blockKey).setPipeline(pipeline)
          .setShouldCreateContainer(createContainer);
      if (pipeline.getMachines().size() > 0) {
        blockStore.put(DFSUtil.string2Bytes(blockKey),
            DFSUtil.string2Bytes(containerName));

        // update the container usage information
        Long newUsed = openContainers.get(containerName) + size;
        openContainers.put(containerName, newUsed);
        openContainerStore.put(DFSUtil.string2Bytes(containerName),
            DFSUtil.string2Bytes(Long.toString(newUsed)));
        return abb.build();
      }
    } finally {
      lock.unlock();
    }
    return null;
  }

  /**
   *
   * Given a block key, return the Pipeline information.
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
        throw new IOException("Specified block key does not exist. key : " +
            key);
      }
      return containerManager.getContainer(
          DFSUtil.bytes2String(containerBytes));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Given a block key, delete a block.
   * @param key - block key assigned by SCM.
   * @throws IOException
   */
  @Override
  public void deleteBlock(final String key) throws IOException {
    throw new NotImplementedException("deleteBlock is not supported");
  }

  /**
   * Close the resources for BlockManager.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (blockStore != null) {
      blockStore.close();
    }
    if (openContainerStore != null) {
      openContainerStore.close();
    }
  }
}