/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.slab.SlabCache;
import org.apache.hadoop.util.StringUtils;

/**
 * DoubleBlockCache is an abstraction layer that combines two caches, the
 * smaller onHeapCache and the larger offHeapCache. CacheBlock attempts to cache
 * the block in both caches, while readblock reads first from the faster on heap
 * cache before looking for the block in the off heap cache. Metrics are the
 * combined size and hits and misses of both caches.
 *
 **/
public class DoubleBlockCache implements BlockCache, HeapSize {

  static final Log LOG = LogFactory.getLog(DoubleBlockCache.class.getName());

  private final LruBlockCache onHeapCache;
  private final SlabCache offHeapCache;
  private final CacheStats stats;

  /**
   * Default constructor. Specify maximum size and expected average block size
   * (approximation is fine).
   * <p>
   * All other factors will be calculated based on defaults specified in this
   * class.
   *
   * @param onHeapSize maximum size of the onHeapCache, in bytes.
   * @param offHeapSize maximum size of the offHeapCache, in bytes.
   * @param onHeapBlockSize average block size of the on heap cache.
   * @param offHeapBlockSize average block size for the off heap cache
   * @param conf configuration file. currently used only by the off heap cache.
   */
  public DoubleBlockCache(long onHeapSize, long offHeapSize,
      long onHeapBlockSize, long offHeapBlockSize, Configuration conf) {

    LOG.info("Creating on-heap cache of size "
        + StringUtils.humanReadableInt(onHeapSize)
        + "bytes with an average block size of "
        + StringUtils.humanReadableInt(onHeapBlockSize) + " bytes.");
    onHeapCache = new LruBlockCache(onHeapSize, onHeapBlockSize);

    LOG.info("Creating off-heap cache of size "
        + StringUtils.humanReadableInt(offHeapSize)
        + "bytes with an average block size of "
        + StringUtils.humanReadableInt(offHeapBlockSize) + " bytes.");
    offHeapCache = new SlabCache(offHeapSize, offHeapBlockSize);

    offHeapCache.addSlabByConf(conf);
    this.stats = new CacheStats();
  }

  @Override
  public void cacheBlock(String blockName, Cacheable buf, boolean inMemory) {
    onHeapCache.cacheBlock(blockName, buf, inMemory);
    offHeapCache.cacheBlock(blockName, buf);
  }

  @Override
  public void cacheBlock(String blockName, Cacheable buf) {
    onHeapCache.cacheBlock(blockName, buf);
    offHeapCache.cacheBlock(blockName, buf);
  }

  @Override
  public Cacheable getBlock(String blockName, boolean caching) {
    Cacheable cachedBlock;

    if ((cachedBlock = onHeapCache.getBlock(blockName, caching)) != null) {
      stats.hit(caching);
      return cachedBlock;

    } else if ((cachedBlock = offHeapCache.getBlock(blockName, caching)) != null) {
      if (caching) {
        onHeapCache.cacheBlock(blockName, cachedBlock);
      }
      stats.hit(caching);
      return cachedBlock;
    }

    stats.miss(caching);
    return null;
  }

  @Override
  public boolean evictBlock(String blockName) {
    stats.evict();
    boolean cacheA = onHeapCache.evictBlock(blockName);
    boolean cacheB = offHeapCache.evictBlock(blockName);
    boolean evicted = cacheA || cacheB;
    if (evicted) {
      stats.evicted();
    }
    return evicted;
  }

  @Override
  public CacheStats getStats() {
    return this.stats;
  }

  @Override
  public void shutdown() {
    onHeapCache.shutdown();
    offHeapCache.shutdown();
  }

  @Override
  public long heapSize() {
    return onHeapCache.heapSize() + offHeapCache.heapSize();
  }

  public long size() {
    return onHeapCache.size() + offHeapCache.size();
  }

  public long getFreeSize() {
    return onHeapCache.getFreeSize() + offHeapCache.getFreeSize();
  }

  public long getCurrentSize() {
    return onHeapCache.getCurrentSize() + offHeapCache.getCurrentSize();
  }

  public long getEvictedCount() {
    return onHeapCache.getEvictedCount() + offHeapCache.getEvictedCount();
  }

  @Override
  public int evictBlocksByPrefix(String prefix) {
    onHeapCache.evictBlocksByPrefix(prefix);
    offHeapCache.evictBlocksByPrefix(prefix);
    return 0;
  }

  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(
      Configuration conf) throws IOException {
    return onHeapCache.getBlockCacheColumnFamilySummaries(conf);
  }

}
