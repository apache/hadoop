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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.MapEvictionListener;
import com.google.common.collect.MapMaker;

/**
 * SingleSizeCache is a slab allocated cache that caches elements up to a single
 * size. It uses a slab allocator (Slab.java) to divide a direct bytebuffer,
 * into evenly sized blocks. Any cached data will take up exactly 1 block. An
 * exception will be thrown if the cached data cannot fit into the blockSize of
 * this SingleSizeCache.
 *
 * Eviction and LRUness is taken care of by Guava's MapMaker, which creates a
 * ConcurrentLinkedHashMap.
 *
 **/
public class SingleSizeCache implements BlockCache {
  private final Slab backingStore;
  private final ConcurrentMap<String, CacheablePair> backingMap;
  private final int numBlocks;
  private final int blockSize;
  private final CacheStats stats;
  private final SlabItemEvictionWatcher evictionWatcher;
  private AtomicLong size;
  private AtomicLong timeSinceLastAccess;
  public final static long CACHE_FIXED_OVERHEAD = ClassSize
      .align((2 * Bytes.SIZEOF_INT) + (5 * ClassSize.REFERENCE)
          + +ClassSize.OBJECT);

  static final Log LOG = LogFactory.getLog(SingleSizeCache.class);

  /**
   * Default constructor. Specify the size of the blocks, number of blocks, and
   * the SlabCache this cache will be assigned to.
   *
   *
   * @param blockSize the size of each block, in bytes
   *
   * @param numBlocks the number of blocks of blockSize this cache will hold.
   *
   * @param master the SlabCache this SingleSlabCache is assigned to.
   */
  public SingleSizeCache(int blockSize, int numBlocks,
      SlabItemEvictionWatcher master) {
    this.blockSize = blockSize;
    this.numBlocks = numBlocks;
    backingStore = new Slab(blockSize, numBlocks);
    this.stats = new CacheStats();
    this.evictionWatcher = master;
    this.size = new AtomicLong(CACHE_FIXED_OVERHEAD + backingStore.heapSize());
    this.timeSinceLastAccess = new AtomicLong();

    // This evictionListener is called whenever the cache automatically evicts
    // something.
    MapEvictionListener<String, CacheablePair> listener = new MapEvictionListener<String, CacheablePair>() {
      @Override
      public void onEviction(String key, CacheablePair value) {
        try {
          value.evictionLock.writeLock().lock();
          timeSinceLastAccess.set(System.nanoTime()
              - value.recentlyAccessed.get());
          backingStore.free(value.serializedData);
          stats.evict();
          /**
           * We may choose to run this cache alone, without the SlabCache on
           * top, no evictionWatcher in that case
           */
          if (evictionWatcher != null) {
            evictionWatcher.onEviction(key, false);
          }
          size.addAndGet(-1 * value.heapSize());
          stats.evicted();
        } finally {
          value.evictionLock.writeLock().unlock();
        }
      }
    };

    backingMap = new MapMaker().maximumSize(numBlocks - 1)
        .evictionListener(listener).makeMap();

  }

  @Override
  public synchronized void cacheBlock(String blockName, Cacheable toBeCached) {
    ByteBuffer storedBlock;

    /*
     * Spinlock if empty, Guava Mapmaker guarantees that we will not store more
     * items than the memory we have allocated, but the Slab Allocator may still
     * be empty if we have not yet completed eviction
     */
    do {
      storedBlock = backingStore.alloc(toBeCached.getSerializedLength());
    } while (storedBlock == null);

    CacheablePair newEntry = new CacheablePair(toBeCached.getDeserializer(),
        storedBlock);

    CacheablePair alreadyCached = backingMap.putIfAbsent(blockName, newEntry);

    if (alreadyCached != null) {
      backingStore.free(storedBlock);
      throw new RuntimeException("already cached " + blockName);
    }
    toBeCached.serialize(storedBlock);
    newEntry.recentlyAccessed.set(System.nanoTime());
    this.size.addAndGet(newEntry.heapSize());
  }

  @Override
  public Cacheable getBlock(String key, boolean caching) {
    CacheablePair contentBlock = backingMap.get(key);
    if (contentBlock == null) {
      stats.miss(caching);
      return null;
    }

    stats.hit(caching);
    // If lock cannot be obtained, that means we're undergoing eviction.
    if (contentBlock.evictionLock.readLock().tryLock()) {
      try {
        contentBlock.recentlyAccessed.set(System.nanoTime());
        return contentBlock.deserializer
            .deserialize(contentBlock.serializedData);
      } catch (IOException e) {
        e.printStackTrace();
        LOG.warn("Deserializer throwing ioexception, possibly deserializing wrong object buffer");
        return null;
      } finally {
        contentBlock.evictionLock.readLock().unlock();
      }
    }
    return null;
  }

  /**
   * Evicts the block
   *
   * @param key the key of the entry we are going to evict
   * @return the evicted ByteBuffer
   */
  public boolean evictBlock(String key) {
    stats.evict();
    CacheablePair evictedBlock = backingMap.remove(key);
    if (evictedBlock != null) {
      try {
        evictedBlock.evictionLock.writeLock().lock();
        backingStore.free(evictedBlock.serializedData);
        evictionWatcher.onEviction(key, false);
        stats.evicted();
        size.addAndGet(-1 * evictedBlock.heapSize());
      } finally {
        evictedBlock.evictionLock.writeLock().unlock();
      }
    }
    return evictedBlock != null;

  }

  public void logStats() {

    long milliseconds = (long)this.timeSinceLastAccess.get() / 1000000;

    LOG.info("For Slab of size " + this.blockSize + ": "
        + this.getOccupiedSize() / this.blockSize
        + " occupied, out of a capacity of " + this.numBlocks
        + " blocks. HeapSize is "
        + StringUtils.humanReadableInt(this.heapSize()) + " bytes." + ", "
        + "churnTime=" + StringUtils.formatTime(milliseconds));

    LOG.debug("Slab Stats: " + "accesses="
        + stats.getRequestCount()
        + ", "
        + "hits="
        + stats.getHitCount()
        + ", "
        + "hitRatio="
        + (stats.getHitCount() == 0 ? "0" : (StringUtils.formatPercent(
            stats.getHitRatio(), 2) + "%, "))
        + "cachingAccesses="
        + stats.getRequestCachingCount()
        + ", "
        + "cachingHits="
        + stats.getHitCachingCount()
        + ", "
        + "cachingHitsRatio="
        + (stats.getHitCachingCount() == 0 ? "0" : (StringUtils.formatPercent(
            stats.getHitCachingRatio(), 2) + "%, ")) + "evictions="
        + stats.getEvictionCount() + ", " + "evicted="
        + stats.getEvictedCount() + ", " + "evictedPerRun="
        + stats.evictedPerEviction());

  }

  public void shutdown() {
    backingStore.shutdown();
  }

  public long heapSize() {
    return this.size() + backingStore.heapSize();
  }

  public long size() {
    return this.blockSize * this.numBlocks;
  }

  public long getFreeSize() {
    return backingStore.getBlocksRemaining() * blockSize;
  }

  public long getOccupiedSize() {
    return (numBlocks - backingStore.getBlocksRemaining()) * blockSize;
  }

  public long getEvictedCount() {
    return stats.getEvictedCount();
  }

  public CacheStats getStats() {
    return this.stats;
  }

  /* Since its offheap, it doesn't matter if its in memory or not */
  @Override
  public void cacheBlock(String blockName, Cacheable buf, boolean inMemory) {
    this.cacheBlock(blockName, buf);
  }

  /*
   * This is never called, as evictions are handled in the SlabCache layer,
   * implemented in the event we want to use this as a standalone cache.
   */
  @Override
  public int evictBlocksByPrefix(String prefix) {
    int evictedCount = 0;
    for (String e : backingMap.keySet()) {
      if (e.startsWith(prefix)) {
        this.evictBlock(e);
      }
    }
    return evictedCount;
  }

  @Override
  public long getCurrentSize() {
    return 0;
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

  /* Just a pair class, holds a reference to the parent cacheable */
  private class CacheablePair implements HeapSize {
    final CacheableDeserializer<Cacheable> deserializer;
    final ByteBuffer serializedData;
    final ReentrantReadWriteLock evictionLock;
    AtomicLong recentlyAccessed;

    private CacheablePair(CacheableDeserializer<Cacheable> deserializer,
        ByteBuffer serializedData) {
      this.recentlyAccessed = new AtomicLong();
      this.deserializer = deserializer;
      this.serializedData = serializedData;
      evictionLock = new ReentrantReadWriteLock();
    }

    /*
     * Heapsize overhead of this is the default object overhead, the heapsize of
     * the serialized object, and the cost of a reference to the bytebuffer,
     * which is already accounted for in SingleSizeCache
     */
    @Override
    public long heapSize() {
      return ClassSize.align(ClassSize.OBJECT + ClassSize.REFERENCE * 3
          + ClassSize.REENTRANT_LOCK);
    }
  }
}
