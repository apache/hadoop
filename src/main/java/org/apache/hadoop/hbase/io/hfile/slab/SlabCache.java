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

package org.apache.hadoop.hbase.io.hfile.slab;

import java.math.BigDecimal;
import java.util.Map.Entry;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * SlabCache is composed of multiple SingleSizeCaches. It uses a TreeMap in
 * order to determine where a given element fits. Redirects gets and puts to the
 * correct SingleSizeCache.
 *
 **/
public class SlabCache implements SlabItemEvictionWatcher, BlockCache, HeapSize {

  private final ConcurrentHashMap<String, SingleSizeCache> backingStore;
  private final TreeMap<Integer, SingleSizeCache> sizer;
  static final Log LOG = LogFactory.getLog(SlabCache.class);
  static final int STAT_THREAD_PERIOD_SECS = 60 * 5;

  private final ScheduledExecutorService scheduleThreadPool = Executors
      .newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setNameFormat("Slab Statistics #%d")
              .build());

  long size;
  private final CacheStats stats;
  final SlabStats slabstats;
  private final long avgBlockSize;
  private static final long CACHE_FIXED_OVERHEAD = ClassSize.estimateBase(
      SlabCache.class, false);

  /**
   * Default constructor, creates an empty SlabCache.
   *
   * @param size Total size allocated to the SlabCache. (Bytes)
   * @param avgBlockSize Average size of a block being cached.
   **/

  public SlabCache(long size, long avgBlockSize) {
    this.avgBlockSize = avgBlockSize;
    this.size = size;
    this.stats = new CacheStats();
    this.slabstats = new SlabStats();
    backingStore = new ConcurrentHashMap<String, SingleSizeCache>();
    sizer = new TreeMap<Integer, SingleSizeCache>();
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        STAT_THREAD_PERIOD_SECS, STAT_THREAD_PERIOD_SECS, TimeUnit.SECONDS);

  }

  /**
   * A way of allocating the desired amount of Slabs of each particular size.
   *
   * This reads two lists from conf, hbase.offheap.slab.proportions and
   * hbase.offheap.slab.sizes.
   *
   * The first list is the percentage of our total space we allocate to the
   * slabs.
   *
   * The second list is blocksize of the slabs in bytes. (E.g. the slab holds
   * blocks of this size).
   *
   * @param Configuration file.
   */
  public void addSlabByConf(Configuration conf) {
    // Proportions we allocate to each slab of the total size.
    String[] porportions = conf.getStrings(
        "hbase.offheapcache.slab.proportions", "0.80", "0.20");
    String[] sizes = conf.getStrings("hbase.offheapcache.slab.sizes", new Long(
        avgBlockSize * 11 / 10).toString(), new Long(avgBlockSize * 21 / 10)
        .toString());

    if (porportions.length != sizes.length) {
      throw new IllegalArgumentException(
          "SlabCache conf not "
              + "initialized, error in configuration. hbase.offheap.slab.proportions specifies "
              + porportions.length
              + " slabs while hbase.offheap.slab.sizes specifies "
              + sizes.length + " slabs "
              + "offheapslabporportions and offheapslabsizes");
    }
    /* We use BigDecimals instead of floats because float rounding is annoying */

    BigDecimal[] parsedProportions = stringArrayToBigDecimalArray(porportions);
    BigDecimal[] parsedSizes = stringArrayToBigDecimalArray(sizes);

    BigDecimal sumProportions = new BigDecimal(0);
    for (BigDecimal b : parsedProportions) {
      /* Make sure all proportions are greater than 0 */
      Preconditions
          .checkArgument(b.compareTo(BigDecimal.ZERO) == 1,
              "Proportions in hbase.offheap.slab.proportions must be greater than 0!");
      sumProportions = sumProportions.add(b);
    }

    /* If the sum is greater than 1 */
    Preconditions
        .checkArgument(sumProportions.compareTo(BigDecimal.ONE) != 1,
            "Sum of all proportions in hbase.offheap.slab.proportions must be less than 1");

    /* If the sum of all proportions is less than 0.99 */
    if (sumProportions.compareTo(new BigDecimal("0.99")) == -1) {
      LOG.warn("Sum of hbase.offheap.slab.proportions is less than 0.99! Memory is being wasted");
    }
    for (int i = 0; i < parsedProportions.length; i++) {
      int blockSize = parsedSizes[i].intValue();
      int numBlocks = new BigDecimal(this.size).multiply(parsedProportions[i])
          .divide(parsedSizes[i], BigDecimal.ROUND_DOWN).intValue();
      addSlab(blockSize, numBlocks);
    }
  }

  /**
   * Gets the size of the slab cache a ByteBuffer of this size would be
   * allocated to.
   *
   * @param size Size of the ByteBuffer we are checking.
   *
   * @return the Slab that the above bytebuffer would be allocated towards. If
   *         object is too large, returns null.
   */
  Entry<Integer, SingleSizeCache> getHigherBlock(int size) {
    return sizer.higherEntry(size - 1);
  }

  private BigDecimal[] stringArrayToBigDecimalArray(String[] parsee) {
    BigDecimal[] parsed = new BigDecimal[parsee.length];
    for (int i = 0; i < parsee.length; i++) {
      parsed[i] = new BigDecimal(parsee[i].trim());
    }
    return parsed;
  }

  private void addSlab(int blockSize, int numBlocks) {
    sizer.put(blockSize, new SingleSizeCache(blockSize, numBlocks, this));
  }

  /**
   * Cache the block with the specified name and buffer. First finds what size
   * SingleSlabCache it should fit in. If the block doesn't fit in any, it will
   * return without doing anything.
   * <p>
   * It is assumed this will NEVER be called on an already cached block. If that
   * is done, it is assumed that you are reinserting the same exact block due to
   * a race condition, and will throw a runtime exception.
   *
   * @param blockName block name
   * @param cachedItem block buffer
   */
  public void cacheBlock(String blockName, Cacheable cachedItem) {
    Entry<Integer, SingleSizeCache> scacheEntry = getHigherBlock(cachedItem
        .getSerializedLength());

    this.slabstats.addin(cachedItem.getSerializedLength());

    if (scacheEntry == null) {
      return; // we can't cache, something too big.
    }

    SingleSizeCache scache = scacheEntry.getValue();
    scache.cacheBlock(blockName, cachedItem); // if this
                                              // fails, due to
                                              // block already
    // being there, exception will be thrown
    backingStore.put(blockName, scache);
  }

  /**
   * We don't care about whether its in memory or not, so we just pass the call
   * through.
   */
  public void cacheBlock(String blockName, Cacheable buf, boolean inMemory) {
    cacheBlock(blockName, buf);
  }

  public CacheStats getStats() {
    return this.stats;
  }

  /**
   * Get the buffer of the block with the specified name.
   *
   * @param blockName block name
   * @return buffer of specified block name, or null if not in cache
   */
  public Cacheable getBlock(String key, boolean caching) {
    SingleSizeCache cachedBlock = backingStore.get(key);
    if (cachedBlock == null) {
      return null;
    }

    Cacheable contentBlock = cachedBlock.getBlock(key, caching);

    if (contentBlock != null) {
      stats.hit(caching);
    } else {
      stats.miss(caching);
    }
    return contentBlock;
  }

  /**
   * Evicts a block from the cache. This is public, and thus contributes to the
   * the evict counter.
   */
  public boolean evictBlock(String key) {
    stats.evict();
    return onEviction(key, true);
  }

  @Override
  public boolean onEviction(String key, boolean callAssignedCache) {
    SingleSizeCache cacheEntry = backingStore.remove(key);
    if (cacheEntry == null) {
      return false;
    }
    /* we need to bump up stats.evict, as this call came from the assignedCache. */
    if (callAssignedCache == false) {
      stats.evict();
    }
    stats.evicted();
    if (callAssignedCache) {
      cacheEntry.evictBlock(key);
    }
    return true;
  }

  /**
   * Sends a shutdown to all SingleSizeCache's contained by this cache.F
   */
  public void shutdown() {
    for (SingleSizeCache s : sizer.values()) {
      s.shutdown();
    }
  }

  public long heapSize() {
    long childCacheSize = 0;
    for (SingleSizeCache s : sizer.values()) {
      childCacheSize += s.heapSize();
    }
    return SlabCache.CACHE_FIXED_OVERHEAD + childCacheSize;
  }

  public long size() {
    return this.size;
  }

  public long getFreeSize() {
    return 0; // this cache, by default, allocates all its space.
  }

  public long getCurrentSize() {
    return size;
  }

  public long getEvictedCount() {
    return stats.getEvictedCount();
  }

  /*
   * Statistics thread. Periodically prints the cache statistics to the log.
   */
  static class StatisticsThread extends Thread {
    SlabCache ourcache;

    public StatisticsThread(SlabCache slabCache) {
      super("SlabCache.StatisticsThread");
      setDaemon(true);
      this.ourcache = slabCache;
    }

    @Override
    public void run() {
      ourcache.slabstats.logStats(ourcache);
    }

  }

  /**
   * Just like CacheStats, but more Slab specific. Finely grained profiling of
   * sizes we store using logs.
   *
   */
  static class SlabStats {
    // the maximum size somebody will ever try to cache, then we multiply by 10
    // so we have finer grained stats.
    private final int MULTIPLIER = 10;
    private final int NUMDIVISIONS = (int) (Math.log(Integer.MAX_VALUE) * MULTIPLIER);
    private final AtomicLong[] counts = new AtomicLong[NUMDIVISIONS];

    public SlabStats() {
      for (int i = 0; i < NUMDIVISIONS; i++) {
        counts[i] = new AtomicLong();
      }
    }

    public void addin(int size) {
      int index = (int) (Math.log(size) * MULTIPLIER);
      counts[index].incrementAndGet();
    }

    public AtomicLong[] getUsage() {
      return counts;
    }

    public void logStats(SlabCache slabCache) {
      for (SingleSizeCache s : slabCache.sizer.values()) {
        s.logStats();
      }
      AtomicLong[] fineGrainedStats = getUsage();
      int multiplier = MULTIPLIER;
      SlabCache.LOG.info("Current heap size is: "
          + StringUtils.humanReadableInt(slabCache.heapSize()));
      for (int i = 0; i < fineGrainedStats.length; i++) {
        double lowerbound = Math.pow(Math.E, (double) i / (double) multiplier
            - 0.5);
        double upperbound = Math.pow(Math.E, (double) i / (double) multiplier
            + 0.5);

        SlabCache.LOG.info("From  "
            + StringUtils.humanReadableInt((long) lowerbound) + "- "
            + StringUtils.humanReadableInt((long) upperbound) + ": "
            + StringUtils.humanReadableInt(fineGrainedStats[i].get())
            + " requests");

      }
    }
  }

  public int evictBlocksByPrefix(String prefix) {
    int numEvicted = 0;
    for (String key : backingStore.keySet()) {
      if (key.startsWith(prefix)) {
        if (evictBlock(key))
          ++numEvicted;
      }
    }
    return numEvicted;
  }

  /*
   * Not implemented. Extremely costly to do this from the off heap cache, you'd
   * need to copy every object on heap once
   */
  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(
      Configuration conf) {
    throw new UnsupportedOperationException();
  }

}
