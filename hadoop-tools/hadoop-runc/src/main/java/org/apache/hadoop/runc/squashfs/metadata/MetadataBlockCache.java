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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class MetadataBlockCache implements MetadataBlockReader {

  private static final int DEFAULT_CACHE_SIZE = 32; // 256 KB

  private final int cacheSize;
  private final TaggedMetadataBlockReader taggedReader;
  private final boolean shouldClose;
  private final LruBlockCache cache;
  private final AtomicLong cacheHits = new AtomicLong(0L);
  private final AtomicLong cacheMisses = new AtomicLong(0L);

  public MetadataBlockCache(TaggedMetadataBlockReader reader) {
    this(reader, DEFAULT_CACHE_SIZE);
  }

  public MetadataBlockCache(TaggedMetadataBlockReader reader, int cacheSize) {
    this(reader, cacheSize, true);
  }

  public MetadataBlockCache(TaggedMetadataBlockReader reader,
      boolean shouldClose) {
    this(reader, DEFAULT_CACHE_SIZE, shouldClose);
  }

  public MetadataBlockCache(TaggedMetadataBlockReader reader, int cacheSize,
      boolean shouldClose) {
    this.cacheSize = cacheSize;
    this.taggedReader = reader;
    this.cache = new LruBlockCache(cacheSize < 1 ? 1 : cacheSize);
    this.shouldClose = shouldClose;
  }

  public synchronized void add(int tag, MetadataBlockReader reader) {
    this.taggedReader.add(tag, reader);
  }

  @Override
  public SuperBlock getSuperBlock(int tag) {
    return taggedReader.getSuperBlock(tag);
  }

  @Override
  public MetadataBlock read(int tag, long fileOffset)
      throws IOException, SquashFsException {
    Key key = new Key(tag, fileOffset);

    MetadataBlock block;

    synchronized (this) {
      block = cache.get(key);
    }

    if (block != null) {
      cacheHits.incrementAndGet();
    } else {
      cacheMisses.incrementAndGet();
      block = taggedReader.read(tag, fileOffset);
      synchronized (this) {
        cache.put(key, block);
      }
    }

    return block;
  }

  @Override
  public void close() throws IOException {
    if (shouldClose) {
      taggedReader.close();
    }
  }

  public long getCacheHits() {
    return cacheHits.get();
  }

  public long getCacheMisses() {
    return cacheMisses.get();
  }

  public synchronized int getCacheLoad() {
    return cache.size();
  }

  public void resetStatistics() {
    cacheHits.set(0L);
    cacheMisses.set(0L);
  }

  public synchronized void clearCache() {
    cache.clear();
    resetStatistics();
  }

  @Override
  public String toString() {
    return String.format(
        "metadata-block-cache { capacity=%d, size=%d, hits=%d, misses=%d }",
        cacheSize, getCacheLoad(), getCacheHits(), getCacheMisses());
  }

  public static final class Key {

    private final int tag;
    private final long fileOffset;

    public Key(int tag, long fileOffset) {
      this.tag = tag;
      this.fileOffset = fileOffset;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag, fileOffset);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Key)) {
        return false;
      }
      Key o = (Key) obj;

      return (tag == o.tag) && (fileOffset == o.fileOffset);
    }

    @Override
    public String toString() {
      return String.format("%d.%d", tag, fileOffset);
    }
  }

  private static class LruBlockCache extends LinkedHashMap<Key, MetadataBlock> {

    private static final long serialVersionUID = 7509410739092012261L;

    private final int cacheSize;

    LruBlockCache(int cacheSize) {
      super(16, 0.75f, true);
      this.cacheSize = cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Key, MetadataBlock> eldest) {
      return size() > cacheSize;
    }

  }
}
