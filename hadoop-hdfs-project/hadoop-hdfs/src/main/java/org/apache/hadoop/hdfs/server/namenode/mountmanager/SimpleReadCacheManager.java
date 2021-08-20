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
package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CacheReplicationMonitor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_SCAN_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_SCAN_INTERVAL_MS_DEFAULT;

/**
 * Default implementation of the cache manager for remote stores. which
 * works at the block-level.
 */
public class SimpleReadCacheManager implements ProvidedReadCacheManagerSpi {

  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleReadCacheManager.class);

  /**
   * The FSN associated with this CacheManager.
   */
  private final FSNamesystem namesystem;

  /**
   * The BlockManager associated with the FSN which owns this cache manager.
   */
  private final BlockManager blockManager;

  private Map<BlockInfo, Set<DatanodeStorageInfo>> cachedBlocks;

  private long cacheUsed;

  private double maxCacheFraction;

  private long maxCacheSpaceBytes;

  private double defaultCapacityFraction;

  private long defaultCapacityAbs;

  private double cacheEvictionThreshold;

  private long scanIntervalMs;

  /**
   * Lock which protects the CacheReplicationMonitor. Other class members
   * should be access in synchronized blocks.
   */
  private final ReentrantLock monitorLock = new ReentrantLock();

  private MountCacheMonitor cacheMonitor;

  private long blocksEvicted;

  /**
   * The cache monitor used to schedule cache evictions.
   */
  private class MountCacheMonitor extends CacheReplicationMonitor {

    private final ProvidedReadCacheManagerSpi cacheManager;

    MountCacheMonitor(FSNamesystem namesystem, long intervalMs,
        ReentrantLock lock, ProvidedReadCacheManagerSpi cacheManager) {
      super(namesystem, intervalMs, lock);
      this.cacheManager = cacheManager;
    }

    @Override
    protected void rescan() throws InterruptedException {
      try {
        namesystem.writeLock();
        try {
          monitorLock.lock();
          if (doShutdown()) {
            throw new InterruptedException(
                "CacheReplicationMonitor was shut down.");
          }
          incrCurrentScanCount();
        } finally {
          monitorLock.unlock();
        }
        cacheManager.evictBlocks();
      } finally {
        namesystem.writeUnlock();
      }
    }
  }

  public SimpleReadCacheManager(Configuration conf, FSNamesystem namesystem,
      BlockManager blockManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.cachedBlocks = new HashMap<>();

    this.cacheEvictionThreshold =
        conf.getDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD,
            DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD_DEFAULT);
    this.scanIntervalMs =
        conf.getTimeDuration(DFS_PROVIDED_READ_CACHE_SCAN_INTERVAL_MS,
            DFS_PROVIDED_READ_CACHE_SCAN_INTERVAL_MS_DEFAULT,
            TimeUnit.MILLISECONDS);
    // configure cache capacities.
    double cacheCapacityFraction =
        conf.getDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION,
            DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION_DEFAULT);
    long cacheCapacityAbs  =
        conf.getLong(DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES,
            DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES_DEFAULT);

    setCacheCapacity(cacheCapacityFraction, cacheCapacityAbs);
  }

  /**
   * Set cache capacity.
   *
   * @param cacheCapacityFraction
   * @param cacheCapacityAbs
   */
  private synchronized void setCacheCapacity(double cacheCapacityFraction,
      long cacheCapacityAbs) {
    if (cacheCapacityFraction < 0 && cacheCapacityAbs < 0) {
      // if both are negative do not cache anything.
      this.maxCacheFraction = 0;
      this.maxCacheSpaceBytes = 0;
      LOG.warn(
          "Cache space set to 0 as negative values configured for {} and {}",
          DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION,
          DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES);
    } else {
      // ignore the negative value by setting it to the max.
      this.maxCacheFraction =
          cacheCapacityFraction >= 0 ? cacheCapacityFraction : 1;
      this.maxCacheSpaceBytes =
          cacheCapacityAbs >= 0 ? cacheCapacityAbs : Long.MAX_VALUE;
      LOG.info("Setting cache capacity: fraction {}, max bytes {}",
          maxCacheFraction, maxCacheSpaceBytes);
    }
  }

  @Override
  public void startService() {
    resetCounters();
    monitorLock.lock();
    try {
      if (this.cacheMonitor == null) {
        this.cacheMonitor = new MountCacheMonitor(namesystem, scanIntervalMs,
            monitorLock, this);
        this.cacheMonitor.start();
      }
    } finally {
      monitorLock.unlock();
    }
  }

  @Override
  public void stopService() {
    // clean up state.
    resetCounters();
    monitorLock.lock();
    try {
      // stop the monitor thread.
      if (this.cacheMonitor != null) {
        MountCacheMonitor prevMonitor = this.cacheMonitor;
        this.cacheMonitor = null;
        IOUtils.closeQuietly(prevMonitor);
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    } finally {
      monitorLock.unlock();
    }
  }

  private synchronized void resetCounters() {
    cacheUsed = 0;
    cachedBlocks.clear();
    blocksEvicted = 0;
  }

  @Override
  public void addCachedBlock(BlockInfo block,
      DatanodeStorageInfo storageInfo) {
    if (storageInfo.getStorageType() == StorageType.PROVIDED) {
      return;
    }
    if (block == null) {
      return;
    }
    checkPreConditions();
    synchronized (this) {
      Set<DatanodeStorageInfo> storageInfos = cachedBlocks.get(block);
      if (storageInfos == null) {
        // first cached copy of the block
        storageInfos = new HashSet<>();
        cachedBlocks.put(block, storageInfos);
      }

      // add to the set.
      if (storageInfos.add(storageInfo)) {
        cacheUsed += block.getNumBytes();
      }
    }

    monitorLock.lock();
    try {
      // rescan is needed as the cache occupied has increased.
      cacheMonitor.setNeedsRescan();
    } finally {
      monitorLock.unlock();
    }
  }

  @Override
  public synchronized void removeCachedBlocks(List<BlockInfo> blocks) {
    if (blocks == null || blocks.isEmpty()) {
      return;
    }
    checkPreConditions();
    for (BlockInfo block : blocks) {
      if (cachedBlocks.remove(block) != null) {
        cacheUsed -= block.getNumBytes();
      }
    }
  }

  @Override
  public synchronized void removeCachedBlock(BlockInfo blockInfo,
      DatanodeDescriptor dnDesc) {
    checkPreConditions();

    Set<DatanodeStorageInfo> storageInfos = cachedBlocks.get(blockInfo);
    if (storageInfos != null) {
      Iterator<DatanodeStorageInfo> iterator = storageInfos.iterator();
      while (iterator.hasNext()) {
        if (iterator.next().getDatanodeDescriptor().equals(dnDesc)) {
          iterator.remove();
          cacheUsed -= blockInfo.getNumBytes();
          break;
        }
      }
      // remove the entry if there are no more nodes for it.
      if (storageInfos.isEmpty()) {
        cachedBlocks.remove(blockInfo);
      }
    }
  }

  @Override
  public synchronized void init(Path mountPath,
      ProvidedVolumeInfo volInfo) {
    // Allow setting capacity while adding mount.
    double cacheCapacityFraction = -1;
    long cacheCapacityAbs = -1;
    Map<String, String> mapConf = volInfo.getConfig();
    if (mapConf.containsKey(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION)) {
      cacheCapacityFraction = Double.parseDouble(
          mapConf.get(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION));
    }
    if (mapConf.containsKey(DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES)) {
      cacheCapacityAbs =
          Long.parseLong(mapConf.get(DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES));
    }
    // if any of these are set, add it to the map tracking space.
    if (cacheCapacityFraction >= 0 || cacheCapacityAbs >= 0) {
      setCacheCapacity(cacheCapacityFraction, cacheCapacityAbs);
    }
  }

  private List<BlockInfo> findBlocksToEvict(long cacheAllowed) {
    List<BlockInfo> blockInfos = new ArrayList<>();
    long cacheSpaceToReclaim = cacheUsed - cacheAllowed;
    // choose blocks to remove in a random order.
    for (BlockInfo blockInfo : cachedBlocks.keySet()) {
      cacheSpaceToReclaim -=
          cachedBlocks.get(blockInfo).size() * blockInfo.getNumBytes();
      blockInfos.add(blockInfo);
      if (cacheSpaceToReclaim <= 0) {
        break;
      }
    }
    return blockInfos;
  }

  @Override
  public synchronized void evictBlocks() {
    checkPreConditions();
    // if the active evicts the blocks, the standby will update itself as well
    // on receiving heartbeats from datanodes.
    long cacheSpaceAllowed = getCacheSpaceAllowed();
    if (cacheUsed > cacheSpaceAllowed) {
      // reached the threshold to start eviction; find blocks to evict.
      List<BlockInfo> blocksToEvict = findBlocksToEvict(cacheSpaceAllowed);
      for (BlockInfo block : blocksToEvict) {
        // remove the block from all local Datanodes.
        for (DatanodeStorageInfo node : cachedBlocks.get(block)) {
          blockManager.processExtraRedundancyBlock(block,
              blockManager.getExpectedRedundancyNum(block),
              node.getDatanodeDescriptor(), null);
          cacheUsed -= block.getNumBytes();
          blocksEvicted++;
        }
        cachedBlocks.remove(block);
      }
    }
  }

  private synchronized void checkPreConditions() {
    Preconditions.checkArgument(cacheMonitor != null);
    assert namesystem.hasWriteLock();
  }

  @Override
  public synchronized long getCacheUsedForProvided() {
    return cacheUsed;
  }

  @Override
  public synchronized long getCacheCapacityForProvided() {
    return getCacheCapacity(maxCacheSpaceBytes, maxCacheFraction,
        namesystem.getCapacityTotal());
  }

  private static long getCacheCapacity(long maxBytes, double maxFraction,
      long totalCapacity) {
    return (long) Math.min(maxBytes, maxFraction * totalCapacity);
  }

  /**
   * @return the max cache space allowed
   */
  @VisibleForTesting
  public synchronized long getCacheSpaceAllowed() {
    return (long) (cacheEvictionThreshold * getCacheCapacityForProvided());
  }

  @VisibleForTesting
  public synchronized long getNumBlocksEvicted() {
    return blocksEvicted;
  }

  @VisibleForTesting
  public synchronized List<BlockInfo> getBlocksCached() {
    return new ArrayList<>(cachedBlocks.keySet());
  }
}
