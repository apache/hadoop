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

package org.apache.hadoop.runc.squashfs.data;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class DataBlockCache {

  public static final DataBlockCache NO_CACHE = new DataBlockCache(0);

  private final LruBlockCache cache;
  private final int cacheSize;
  private final AtomicLong cacheHits = new AtomicLong(0L);
  private final AtomicLong cacheMisses = new AtomicLong(0L);

  public DataBlockCache(int cacheSize) {
    this.cache = cacheSize < 1 ? null : new LruBlockCache(cacheSize);
    this.cacheSize = cacheSize;
  }

  public synchronized void put(Key key, DataBlock block) {
    if (cache != null) {
      cache.put(key, block);
    }
  }

  public synchronized DataBlock get(Key key) {
    if (cache == null) {
      cacheMisses.incrementAndGet();
      return null;
    }

    DataBlock block = cache.get(key);
    if (block != null) {
      cacheHits.incrementAndGet();
    } else {
      cacheMisses.incrementAndGet();
    }
    return block;
  }

  public long getCacheHits() {
    return cacheHits.get();
  }

  public long getCacheMisses() {
    return cacheMisses.get();
  }

  public synchronized int getCacheLoad() {
    return cache == null ? 0 : cache.size();
  }

  public void resetStatistics() {
    cacheHits.set(0L);
    cacheMisses.set(0L);
  }

  public synchronized void clearCache() {
    if (cache != null) {
      cache.clear();
    }
    resetStatistics();
  }

  public static final class Key {

    private final int tag;
    private final boolean compressed;
    private final long fileOffset;
    private final long dataSize;
    private final int expectedSize;

    public Key(
        int tag,
        boolean compressed,
        long fileOffset,
        int dataSize,
        int expectedSize) {
      this.tag = tag;
      this.compressed = compressed;
      this.fileOffset = fileOffset;
      this.dataSize = dataSize;
      this.expectedSize = expectedSize;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag, compressed, fileOffset, dataSize, expectedSize);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Key)) {
        return false;
      }
      Key o = (Key) obj;

      return (tag == o.tag) &&
          (compressed == o.compressed) &&
          (fileOffset == o.fileOffset) &&
          (dataSize == o.dataSize) &&
          (expectedSize == o.expectedSize);
    }

    @Override
    public String toString() {
      return String
          .format("%d-%s-%d-%d-%d", tag, compressed, fileOffset, dataSize,
              expectedSize);
    }
  }

  private static class LruBlockCache extends LinkedHashMap<Key, DataBlock> {

    private static final long serialVersionUID = -156607843124781789L;

    private final int cacheSize;

    LruBlockCache(int cacheSize) {
      super(16, 0.75f, true);
      this.cacheSize = cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Key, DataBlock> eldest) {
      return size() > cacheSize;
    }

  }

  @Override
  public String toString() {
    return String
        .format("data-block-cache { capacity=%d, size=%d, hits=%d, misses=%d }",
            cacheSize, getCacheLoad(), getCacheHits(), getCacheMisses());
  }

}
