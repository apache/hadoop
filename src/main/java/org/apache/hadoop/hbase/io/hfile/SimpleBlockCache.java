/**
 * Copyright 2008 The Apache Software Foundation
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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache.CacheStats;


/**
 * Simple one RFile soft reference cache.
 */
public class SimpleBlockCache implements BlockCache {
  private static class Ref extends SoftReference<HeapSize> {
    public String blockId;
    public Ref(String blockId, HeapSize block, ReferenceQueue q) {
      super(block, q);
      this.blockId = blockId;
    }
  }
  private Map<String,Ref> cache =
    new HashMap<String,Ref>();

  private ReferenceQueue q = new ReferenceQueue();
  public int dumps = 0;

  /**
   * Constructor
   */
  public SimpleBlockCache() {
    super();
  }

  void processQueue() {
    Ref r;
    while ( (r = (Ref)q.poll()) != null) {
      cache.remove(r.blockId);
      dumps++;
    }
  }

  /**
   * @return the size
   */
  public synchronized long size() {
    processQueue();
    return cache.size();
  }

  public synchronized HeapSize getBlock(String blockName, boolean caching) {
    processQueue(); // clear out some crap.
    Ref ref = cache.get(blockName);
    if (ref == null)
      return null;
    return ref.get();
  }

  public synchronized void cacheBlock(String blockName, HeapSize block) {
    cache.put(blockName, new Ref(blockName, block, q));
  }

  public synchronized void cacheBlock(String blockName, HeapSize block,
      boolean inMemory) {
    cache.put(blockName, new Ref(blockName, block, q));
  }

  @Override
  public boolean evictBlock(String blockName) {
    return cache.remove(blockName) != null;
  }

  public void shutdown() {
    // noop
  }

  @Override
  public CacheStats getStats() {
    // TODO: implement this if we ever actually use this block cache
    return null;
  }

  @Override
  public long getFreeSize() {
    // TODO: implement this if we ever actually use this block cache
    return 0;
  }

  @Override
  public long getCurrentSize() {
    // TODO: implement this if we ever actually use this block cache
    return 0;
  }

  @Override
  public long getEvictedCount() {
    // TODO: implement this if we ever actually use this block cache
    return 0;
  }

  @Override
  public int evictBlocksByPrefix(String string) {
    throw new UnsupportedOperationException();
  }
 
  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(Configuration conf) {
    throw new UnsupportedOperationException();
  }

}

