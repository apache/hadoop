/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;

/**
 * Manages caching for an FsDatasetImpl by using the mmap(2) and mlock(2)
 * system calls to lock blocks into memory. Block checksums are verified upon
 * entry into the cache.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsDatasetCache {

  private static final Log LOG = LogFactory.getLog(FsDatasetCache.class);

  /**
   * Map of cached blocks
   */
  private final ConcurrentMap<Long, MappableBlock> cachedBlocks;

  private final FsDatasetImpl dataset;
  /**
   * Number of cached bytes
   */
  private AtomicLong usedBytes;
  /**
   * Total cache capacity in bytes
   */
  private final long maxBytes;

  public FsDatasetCache(FsDatasetImpl dataset) {
    this.dataset = dataset;
    this.cachedBlocks = new ConcurrentHashMap<Long, MappableBlock>();
    this.usedBytes = new AtomicLong(0);
    this.maxBytes = dataset.datanode.getDnConf().getMaxLockedMemory();
  }

  /**
   * @return if the block is cached
   */
  boolean isCached(String bpid, long blockId) {
    MappableBlock mapBlock = cachedBlocks.get(blockId);
    if (mapBlock != null) {
      return mapBlock.getBlockPoolId().equals(bpid);
    }
    return false;
  }

  /**
   * @return List of cached blocks suitable for translation into a
   * {@link BlockListAsLongs} for a cache report.
   */
  List<Long> getCachedBlocks(String bpid) {
    List<Long> blocks = new ArrayList<Long>();
    // ConcurrentHashMap iteration doesn't see latest updates, which is okay
    Iterator<MappableBlock> it = cachedBlocks.values().iterator();
    while (it.hasNext()) {
      MappableBlock mapBlock = it.next();
      if (mapBlock.getBlockPoolId().equals(bpid)) {
        blocks.add(mapBlock.getBlock().getBlockId());
      }
    }
    return blocks;
  }

  /**
   * Asynchronously attempts to cache a block. This is subject to the
   * configured maximum locked memory limit.
   * 
   * @param block block to cache
   * @param volume volume of the block
   * @param blockIn stream of the block's data file
   * @param metaIn stream of the block's meta file
   */
  void cacheBlock(String bpid, Block block, FsVolumeImpl volume,
      FileInputStream blockIn, FileInputStream metaIn) {
    if (isCached(bpid, block.getBlockId())) {
      return;
    }
    MappableBlock mapBlock = null;
    try {
      mapBlock = new MappableBlock(bpid, block, volume, blockIn, metaIn);
    } catch (IOException e) {
      LOG.warn("Failed to cache replica " + block + ": Could not instantiate"
          + " MappableBlock", e);
      IOUtils.closeQuietly(blockIn);
      IOUtils.closeQuietly(metaIn);
      return;
    }
    // Check if there's sufficient cache capacity
    boolean success = false;
    long bytes = mapBlock.getNumBytes();
    long used = usedBytes.get();
    while (used+bytes < maxBytes) {
      if (usedBytes.compareAndSet(used, used+bytes)) {
        success = true;
        break;
      }
      used = usedBytes.get();
    }
    if (!success) {
      LOG.warn(String.format(
          "Failed to cache replica %s: %s exceeded (%d + %d > %d)",
          mapBlock.getBlock().toString(),
          DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
          used, bytes, maxBytes));
      mapBlock.close();
      return;
    }
    // Submit it to the worker pool to be cached
    volume.getExecutor().execute(new WorkerTask(mapBlock));
  }

  /**
   * Uncaches a block if it is cached.
   * @param blockId id to uncache
   */
  void uncacheBlock(String bpid, long blockId) {
    MappableBlock mapBlock = cachedBlocks.get(blockId);
    if (mapBlock != null &&
        mapBlock.getBlockPoolId().equals(bpid) &&
        mapBlock.getBlock().getBlockId() == blockId) {
      mapBlock.close();
      cachedBlocks.remove(blockId);
      long bytes = mapBlock.getNumBytes();
      long used = usedBytes.get();
      while (!usedBytes.compareAndSet(used, used - bytes)) {
        used = usedBytes.get();
      }
      LOG.info("Successfully uncached block " + blockId);
    } else {
      LOG.info("Could not uncache block " + blockId + ": unknown block.");
    }
  }

  /**
   * Background worker that mmaps, mlocks, and checksums a block
   */
  private class WorkerTask implements Runnable {

    private MappableBlock block;
    WorkerTask(MappableBlock block) {
      this.block = block;
    }

    @Override
    public void run() {
      boolean success = false;
      try {
        block.map();
        block.lock();
        block.verifyChecksum();
        success = true;
      } catch (ChecksumException e) {
        // Exception message is bogus since this wasn't caused by a file read
        LOG.warn("Failed to cache block " + block.getBlock() + ": Checksum "
            + "verification failed.");
      } catch (IOException e) {
        LOG.warn("Failed to cache block " + block.getBlock() + ": IOException",
            e);
      }
      // If we failed or the block became uncacheable in the meantime,
      // clean up and return the reserved cache allocation 
      if (!success || 
          !dataset.validToCache(block.getBlockPoolId(),
              block.getBlock().getBlockId())) {
        block.close();
        long used = usedBytes.get();
        while (!usedBytes.compareAndSet(used, used-block.getNumBytes())) {
          used = usedBytes.get();
        }
      } else {
        LOG.info("Successfully cached block " + block.getBlock());
        cachedBlocks.put(block.getBlock().getBlockId(), block);
      }
    }
  }

  // Stats related methods for FsDatasetMBean

  public long getDnCacheUsed() {
    return usedBytes.get();
  }

  public long getDnCacheCapacity() {
    return maxBytes;
  }
}
