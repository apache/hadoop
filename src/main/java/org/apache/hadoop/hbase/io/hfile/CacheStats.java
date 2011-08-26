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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class that implements cache metrics.
 */
public class CacheStats {
  /** The number of getBlock requests that were cache hits */
  private final AtomicLong hitCount = new AtomicLong(0);
  /**
   * The number of getBlock requests that were cache hits, but only from
   * requests that were set to use the block cache.  This is because all reads
   * attempt to read from the block cache even if they will not put new blocks
   * into the block cache.  See HBASE-2253 for more information.
   */
  private final AtomicLong hitCachingCount = new AtomicLong(0);
  /** The number of getBlock requests that were cache misses */
  private final AtomicLong missCount = new AtomicLong(0);
  /**
   * The number of getBlock requests that were cache misses, but only from
   * requests that were set to use the block cache.
   */
  private final AtomicLong missCachingCount = new AtomicLong(0);
  /** The number of times an eviction has occurred */
  private final AtomicLong evictionCount = new AtomicLong(0);
  /** The total number of blocks that have been evicted */
  private final AtomicLong evictedBlockCount = new AtomicLong(0);

  public void miss(boolean caching) {
    missCount.incrementAndGet();
    if (caching) missCachingCount.incrementAndGet();
  }

  public void hit(boolean caching) {
    hitCount.incrementAndGet();
    if (caching) hitCachingCount.incrementAndGet();
  }

  public void evict() {
    evictionCount.incrementAndGet();
  }

  public void evicted() {
    evictedBlockCount.incrementAndGet();
  }

  public long getRequestCount() {
    return getHitCount() + getMissCount();
  }

  public long getRequestCachingCount() {
    return getHitCachingCount() + getMissCachingCount();
  }

  public long getMissCount() {
    return missCount.get();
  }

  public long getMissCachingCount() {
    return missCachingCount.get();
  }

  public long getHitCount() {
    return hitCount.get();
  }

  public long getHitCachingCount() {
    return hitCachingCount.get();
  }

  public long getEvictionCount() {
    return evictionCount.get();
  }

  public long getEvictedCount() {
    return evictedBlockCount.get();
  }

  public double getHitRatio() {
    return ((float)getHitCount()/(float)getRequestCount());
  }

  public double getHitCachingRatio() {
    return ((float)getHitCachingCount()/(float)getRequestCachingCount());
  }

  public double getMissRatio() {
    return ((float)getMissCount()/(float)getRequestCount());
  }

  public double getMissCachingRatio() {
    return ((float)getMissCachingCount()/(float)getRequestCachingCount());
  }

  public double evictedPerEviction() {
    return ((float)getEvictedCount()/(float)getEvictionCount());
  }
}