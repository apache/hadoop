/**
 * Copyright 2009 The Apache Software Foundation
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

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;

/**
 * A block cache implementation that is memory-aware using {@link HeapSize},
 * memory-bound using an LRU eviction algorithm, and concurrent: backed by a
 * {@link ConcurrentHashMap} and with a non-blocking eviction thread giving
 * constant-time {@link #cacheBlock} and {@link #getBlock} operations.<p>
 *
 * Contains three levels of block priority to allow for
 * scan-resistance and in-memory families.  A block is added with an inMemory
 * flag if necessary, otherwise a block becomes a single access priority.  Once
 * a blocked is accessed again, it changes to multiple access.  This is used
 * to prevent scans from thrashing the cache, adding a least-frequently-used
 * element to the eviction algorithm.<p>
 *
 * Each priority is given its own chunk of the total cache to ensure
 * fairness during eviction.  Each priority will retain close to its maximum
 * size, however, if any priority is not using its entire chunk the others
 * are able to grow beyond their chunk size.<p>
 *
 * Instantiated at a minimum with the total size and average block size.
 * All sizes are in bytes.  The block size is not especially important as this
 * cache is fully dynamic in its sizing of blocks.  It is only used for
 * pre-allocating data structures and in initial heap estimation of the map.<p>
 *
 * The detailed constructor defines the sizes for the three priorities (they
 * should total to the maximum size defined).  It also sets the levels that
 * trigger and control the eviction thread.<p>
 *
 * The acceptable size is the cache size level which triggers the eviction
 * process to start.  It evicts enough blocks to get the size below the
 * minimum size specified.<p>
 *
 * Eviction happens in a separate thread and involves a single full-scan
 * of the map.  It determines how many bytes must be freed to reach the minimum
 * size, and then while scanning determines the fewest least-recently-used
 * blocks necessary from each of the three priorities (would be 3 times bytes
 * to free).  It then uses the priority chunk sizes to evict fairly according
 * to the relative sizes and usage.
 */
public class LruBlockCache implements BlockCache, HeapSize {

  static final Log LOG = LogFactory.getLog(LruBlockCache.class);

  /** Default Configuration Parameters*/

  /** Backing Concurrent Map Configuration */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  static final int DEFAULT_CONCURRENCY_LEVEL = 16;

  /** Eviction thresholds */
  static final float DEFAULT_MIN_FACTOR = 0.75f;
  static final float DEFAULT_ACCEPTABLE_FACTOR = 0.85f;

  /** Priority buckets */
  static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  static final float DEFAULT_MULTI_FACTOR = 0.50f;
  static final float DEFAULT_MEMORY_FACTOR = 0.25f;

  /** Statistics thread */
  static final int statThreadPeriod = 60 * 5;

  /** Concurrent map (the cache) */
  private final ConcurrentHashMap<String,CachedBlock> map;

  /** Eviction lock (locked when eviction in process) */
  private final ReentrantLock evictionLock = new ReentrantLock(true);

  /** Volatile boolean to track if we are in an eviction process or not */
  private volatile boolean evictionInProgress = false;

  /** Eviction thread */
  private final EvictionThread evictionThread;

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1);

  /** Current size of cache */
  private final AtomicLong size;

  /** Current number of cached elements */
  private final AtomicLong elements;

  /** Cache access count (sequential ID) */
  private final AtomicLong count;

  /** Cache statistics */
  private final CacheStats stats;

  /** Maximum allowable size of cache (block put if size > max, evict) */
  private long maxSize;

  /** Approximate block size */
  private long blockSize;

  /** Acceptable size of cache (no evictions if size < acceptable) */
  private float acceptableFactor;

  /** Minimum threshold of cache (when evicting, evict until size < min) */
  private float minFactor;

  /** Single access bucket size */
  private float singleFactor;

  /** Multiple access bucket size */
  private float multiFactor;

  /** In-memory bucket size */
  private float memoryFactor;

  /** Overhead of the structure itself */
  private long overhead;

  /**
   * Default constructor.  Specify maximum size and expected average block
   * size (approximation is fine).
   *
   * <p>All other factors will be calculated based on defaults specified in
   * this class.
   * @param maxSize maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   */
  public LruBlockCache(long maxSize, long blockSize) {
    this(maxSize, blockSize, true);
  }

  /**
   * Constructor used for testing.  Allows disabling of the eviction thread.
   */
  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread) {
    this(maxSize, blockSize, evictionThread,
        (int)Math.ceil(1.2*maxSize/blockSize),
        DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL,
        DEFAULT_MIN_FACTOR, DEFAULT_ACCEPTABLE_FACTOR,
        DEFAULT_SINGLE_FACTOR, DEFAULT_MULTI_FACTOR,
        DEFAULT_MEMORY_FACTOR);
  }

  /**
   * Configurable constructor.  Use this constructor if not using defaults.
   * @param maxSize maximum size of this cache, in bytes
   * @param blockSize expected average size of blocks, in bytes
   * @param evictionThread whether to run evictions in a bg thread or not
   * @param mapInitialSize initial size of backing ConcurrentHashMap
   * @param mapLoadFactor initial load factor of backing ConcurrentHashMap
   * @param mapConcurrencyLevel initial concurrency factor for backing CHM
   * @param minFactor percentage of total size that eviction will evict until
   * @param acceptableFactor percentage of total size that triggers eviction
   * @param singleFactor percentage of total size for single-access blocks
   * @param multiFactor percentage of total size for multiple-access blocks
   * @param memoryFactor percentage of total size for in-memory blocks
   */
  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread,
      int mapInitialSize, float mapLoadFactor, int mapConcurrencyLevel,
      float minFactor, float acceptableFactor,
      float singleFactor, float multiFactor, float memoryFactor) {
    if(singleFactor + multiFactor + memoryFactor != 1) {
      throw new IllegalArgumentException("Single, multi, and memory factors " +
          " should total 1.0");
    }
    if(minFactor >= acceptableFactor) {
      throw new IllegalArgumentException("minFactor must be smaller than acceptableFactor");
    }
    if(minFactor >= 1.0f || acceptableFactor >= 1.0f) {
      throw new IllegalArgumentException("all factors must be < 1");
    }
    this.maxSize = maxSize;
    this.blockSize = blockSize;
    map = new ConcurrentHashMap<String,CachedBlock>(mapInitialSize,
        mapLoadFactor, mapConcurrencyLevel);
    this.minFactor = minFactor;
    this.acceptableFactor = acceptableFactor;
    this.singleFactor = singleFactor;
    this.multiFactor = multiFactor;
    this.memoryFactor = memoryFactor;
    this.stats = new CacheStats();
    this.count = new AtomicLong(0);
    this.elements = new AtomicLong(0);
    this.overhead = calculateOverhead(maxSize, blockSize, mapConcurrencyLevel);
    this.size = new AtomicLong(this.overhead);
    if(evictionThread) {
      this.evictionThread = new EvictionThread(this);
      this.evictionThread.start(); // FindBugs SC_START_IN_CTOR
    } else {
      this.evictionThread = null;
    }
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        statThreadPeriod, statThreadPeriod, TimeUnit.SECONDS);
  }

  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    if(this.size.get() > acceptableSize() && !evictionInProgress) {
      runEviction();
    }
  }

  // BlockCache implementation

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NEVER be called on an already cached block.  If
   * that is done, it is assumed that you are reinserting the same exact
   * block due to a race condition and will update the buffer but not modify
   * the size of the cache.
   * @param blockName block name
   * @param buf block buffer
   * @param inMemory if block is in-memory
   */
  public void cacheBlock(String blockName, ByteBuffer buf, boolean inMemory) {
    CachedBlock cb = map.get(blockName);
    if(cb != null) {
      throw new RuntimeException("Cached an already cached block");
    }
    cb = new CachedBlock(blockName, buf, count.incrementAndGet(), inMemory);
    long newSize = size.addAndGet(cb.heapSize());
    map.put(blockName, cb);
    elements.incrementAndGet();
    if(newSize > acceptableSize() && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NEVER be called on an already cached block.  If
   * that is done, it is assumed that you are reinserting the same exact
   * block due to a race condition and will update the buffer but not modify
   * the size of the cache.
   * @param blockName block name
   * @param buf block buffer
   */
  public void cacheBlock(String blockName, ByteBuffer buf) {
    cacheBlock(blockName, buf, false);
  }

  /**
   * Get the buffer of the block with the specified name.
   * @param blockName block name
   * @return buffer of specified block name, or null if not in cache
   */
  public ByteBuffer getBlock(String blockName) {
    CachedBlock cb = map.get(blockName);
    if(cb == null) {
      stats.miss();
      return null;
    }
    stats.hit();
    cb.access(count.incrementAndGet());
    return cb.getBuffer();
  }

  protected long evictBlock(CachedBlock block) {
    map.remove(block.getName());
    size.addAndGet(-1 * block.heapSize());
    elements.decrementAndGet();
    stats.evicted();
    return block.heapSize();
  }

  /**
   * Multi-threaded call to run the eviction process.
   */
  private void runEviction() {
    if(evictionThread == null) {
      evict();
    } else {
      evictionThread.evict();
    }
  }

  /**
   * Eviction method.
   */
  void evict() {

    // Ensure only one eviction at a time
    if(!evictionLock.tryLock()) return;

    try {
      evictionInProgress = true;
      long currentSize = this.size.get();
      long bytesToFree = currentSize - minSize();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Block cache LRU eviction started; Attempting to free " +
          StringUtils.byteDesc(bytesToFree) + " of total=" +
          StringUtils.byteDesc(currentSize));
      }

      if(bytesToFree <= 0) return;

      // Instantiate priority buckets
      BlockBucket bucketSingle = new BlockBucket(bytesToFree, blockSize,
          singleSize());
      BlockBucket bucketMulti = new BlockBucket(bytesToFree, blockSize,
          multiSize());
      BlockBucket bucketMemory = new BlockBucket(bytesToFree, blockSize,
          memorySize());

      // Scan entire map putting into appropriate buckets
      for(CachedBlock cachedBlock : map.values()) {
        switch(cachedBlock.getPriority()) {
          case SINGLE: {
            bucketSingle.add(cachedBlock);
            break;
          }
          case MULTI: {
            bucketMulti.add(cachedBlock);
            break;
          }
          case MEMORY: {
            bucketMemory.add(cachedBlock);
            break;
          }
        }
      }

      PriorityQueue<BlockBucket> bucketQueue =
        new PriorityQueue<BlockBucket>(3);

      bucketQueue.add(bucketSingle);
      bucketQueue.add(bucketMulti);
      bucketQueue.add(bucketMemory);

      int remainingBuckets = 3;
      long bytesFreed = 0;

      BlockBucket bucket;
      while((bucket = bucketQueue.poll()) != null) {
        long overflow = bucket.overflow();
        if(overflow > 0) {
          long bucketBytesToFree = Math.min(overflow,
            (bytesToFree - bytesFreed) / remainingBuckets);
          bytesFreed += bucket.free(bucketBytesToFree);
        }
        remainingBuckets--;
      }

      if (LOG.isDebugEnabled()) {
        long single = bucketSingle.totalSize();
        long multi = bucketMulti.totalSize();
        long memory = bucketMemory.totalSize();
        LOG.debug("Block cache LRU eviction completed; " +
          "freed=" + StringUtils.byteDesc(bytesFreed) + ", " +
          "total=" + StringUtils.byteDesc(this.size.get()) + ", " +
          "single=" + StringUtils.byteDesc(single) + ", " +
          "multi=" + StringUtils.byteDesc(multi) + ", " +
          "memory=" + StringUtils.byteDesc(memory));
      }
    } finally {
      stats.evict();
      evictionInProgress = false;
      evictionLock.unlock();
    }
  }

  /**
   * Used to group blocks into priority buckets.  There will be a BlockBucket
   * for each priority (single, multi, memory).  Once bucketed, the eviction
   * algorithm takes the appropriate number of elements out of each according
   * to configuration parameters and their relatives sizes.
   */
  private class BlockBucket implements Comparable<BlockBucket> {

    private CachedBlockQueue queue;
    private long totalSize = 0;
    private long bucketSize;

    public BlockBucket(long bytesToFree, long blockSize, long bucketSize) {
      this.bucketSize = bucketSize;
      queue = new CachedBlockQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(CachedBlock block) {
      totalSize += block.heapSize();
      queue.add(block);
    }

    public long free(long toFree) {
      LinkedList<CachedBlock> blocks = queue.get();
      long freedBytes = 0;
      for(CachedBlock cb: blocks) {
        freedBytes += evictBlock(cb);
        if(freedBytes >= toFree) {
          return freedBytes;
        }
      }
      return freedBytes;
    }

    public long overflow() {
      return totalSize - bucketSize;
    }

    public long totalSize() {
      return totalSize;
    }

    public int compareTo(BlockBucket that) {
      if(this.overflow() == that.overflow()) return 0;
      return this.overflow() > that.overflow() ? 1 : -1;
    }
  }

  /**
   * Get the maximum size of this cache.
   * @return max size in bytes
   */
  public long getMaxSize() {
    return this.maxSize;
  }

  /**
   * Get the current size of this cache.
   * @return current size in bytes
   */
  public long getCurrentSize() {
    return this.size.get();
  }

  /**
   * Get the current size of this cache.
   * @return current size in bytes
   */
  public long getFreeSize() {
    return getMaxSize() - getCurrentSize();
  }

  /**
   * Get the size of this cache (number of cached blocks)
   * @return number of cached blocks
   */
  public long size() {
    return this.elements.get();
  }

  /**
   * Get the number of eviction runs that have occurred
   */
  public long getEvictionCount() {
    return this.stats.getEvictionCount();
  }

  /**
   * Get the number of blocks that have been evicted during the lifetime
   * of this cache.
   */
  public long getEvictedCount() {
    return this.stats.getEvictedCount();
  }

  /*
   * Eviction thread.  Sits in waiting state until an eviction is triggered
   * when the cache size grows above the acceptable level.<p>
   *
   * Thread is triggered into action by {@link LruBlockCache#runEviction()}
   */
  private static class EvictionThread extends Thread {
    private WeakReference<LruBlockCache> cache;

    public EvictionThread(LruBlockCache cache) {
      super("LruBlockCache.EvictionThread");
      setDaemon(true);
      this.cache = new WeakReference<LruBlockCache>(cache);
    }

    @Override
    public void run() {
      while(true) {
        synchronized(this) {
          try {
            this.wait();
          } catch(InterruptedException e) {}
        }
        LruBlockCache cache = this.cache.get();
        if(cache == null) break;
        cache.evict();
      }
    }
    public void evict() {
      synchronized(this) {
        this.notify(); // FindBugs NN_NAKED_NOTIFY
      }
    }
  }

  /*
   * Statistics thread.  Periodically prints the cache statistics to the log.
   */
  static class StatisticsThread extends Thread {
    LruBlockCache lru;

    public StatisticsThread(LruBlockCache lru) {
      super("LruBlockCache.StatisticsThread");
      setDaemon(true);
      this.lru = lru;
    }
    @Override
    public void run() {
      lru.logStats();
    }
  }

  public void logStats() {
    if (!LOG.isDebugEnabled()) return;
    // Log size
    long totalSize = heapSize();
    long freeSize = maxSize - totalSize;
    LruBlockCache.LOG.debug("LRU Stats: " +
        "total=" + StringUtils.byteDesc(totalSize) + ", " +
        "free=" + StringUtils.byteDesc(freeSize) + ", " +
        "max=" + StringUtils.byteDesc(this.maxSize) + ", " +
        "blocks=" + size() +", " +
        "accesses=" + stats.getRequestCount() + ", " +
        "hits=" + stats.getHitCount() + ", " +
        "hitRatio=" + StringUtils.formatPercent(stats.getHitRatio(), 2) + "%, " +
        "evictions=" + stats.getEvictionCount() + ", " +
        "evicted=" + stats.getEvictedCount() + ", " +
        "evictedPerRun=" + stats.evictedPerEviction());
  }

  /**
   * Get counter statistics for this cache.
   *
   * <p>Includes: total accesses, hits, misses, evicted blocks, and runs
   * of the eviction processes.
   */
  public CacheStats getStats() {
    return this.stats;
  }

  public static class CacheStats {
    private final AtomicLong accessCount = new AtomicLong(0);
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    private final AtomicLong evictedCount = new AtomicLong(0);

    public void miss() {
      missCount.incrementAndGet();
      accessCount.incrementAndGet();
    }

    public void hit() {
      hitCount.incrementAndGet();
      accessCount.incrementAndGet();
    }

    public void evict() {
      evictionCount.incrementAndGet();
    }

    public void evicted() {
      evictedCount.incrementAndGet();
    }

    public long getRequestCount() {
      return accessCount.get();
    }

    public long getMissCount() {
      return missCount.get();
    }

    public long getHitCount() {
      return hitCount.get();
    }

    public long getEvictionCount() {
      return evictionCount.get();
    }

    public long getEvictedCount() {
      return evictedCount.get();
    }

    public double getHitRatio() {
      return ((float)getHitCount()/(float)getRequestCount());
    }

    public double getMissRatio() {
      return ((float)getMissCount()/(float)getRequestCount());
    }

    public double evictedPerEviction() {
      return (float)((float)getEvictedCount()/(float)getEvictionCount());
    }
  }

  public final static long CACHE_FIXED_OVERHEAD = ClassSize.align(
      (3 * Bytes.SIZEOF_LONG) + (8 * ClassSize.REFERENCE) +
      (5 * Bytes.SIZEOF_FLOAT) + Bytes.SIZEOF_BOOLEAN
      + ClassSize.OBJECT);

  // HeapSize implementation
  public long heapSize() {
    return getCurrentSize();
  }

  public static long calculateOverhead(long maxSize, long blockSize, int concurrency){
    // FindBugs ICAST_INTEGER_MULTIPLY_CAST_TO_LONG
    return CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP +
        ((long)Math.ceil(maxSize*1.2/blockSize)
            * ClassSize.CONCURRENT_HASHMAP_ENTRY) +
        (concurrency * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
  }

  // Simple calculators of sizes given factors and maxSize

  private long acceptableSize() {
    return (long)Math.floor(this.maxSize * this.acceptableFactor);
  }
  private long minSize() {
    return (long)Math.floor(this.maxSize * this.minFactor);
  }
  private long singleSize() {
    return (long)Math.floor(this.maxSize * this.singleFactor * this.minFactor);
  }
  private long multiSize() {
    return (long)Math.floor(this.maxSize * this.multiFactor * this.minFactor);
  }
  private long memorySize() {
    return (long)Math.floor(this.maxSize * this.memoryFactor * this.minFactor);
  }

  public void shutdown() {
    this.scheduleThreadPool.shutdown();
  }
}
