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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache.CacheStats;

/**
 * Block cache interface. Anything that implements the {@link HeapSize}
 * interface can be put in the cache, because item size is all the cache
 * cares about. We might move to a more specialized "cacheable" interface
 * in the future.
 *
 * TODO: Add filename or hash of filename to block cache key.
 */
public interface BlockCache {
  /**
   * Add block to cache.
   * @param blockName Zero-based file block number.
   * @param buf The block contents wrapped in a ByteBuffer.
   * @param inMemory Whether block should be treated as in-memory
   */
  public void cacheBlock(String blockName, HeapSize buf, boolean inMemory);

  /**
   * Add block to cache (defaults to not in-memory).
   * @param blockName Zero-based file block number.
   * @param buf The block contents wrapped in a ByteBuffer.
   */
  public void cacheBlock(String blockName, HeapSize buf);

  /**
   * Fetch block from cache.
   * @param blockName Block number to fetch.
   * @param caching Whether this request has caching enabled (used for stats)
   * @return Block or null if block is not in the cache.
   */
  public HeapSize getBlock(String blockName, boolean caching);

  /**
   * Evict block from cache.
   * @param blockName Block name to evict
   * @return true if block existed and was evicted, false if not
   */
  public boolean evictBlock(String blockName);

  /**
   * Evicts all blocks with name starting with the given prefix. This is
   * necessary in cases we need to evict all blocks that belong to a particular
   * HFile. In HFile v2 all blocks consist of the storefile name (UUID), an
   * underscore, and the block offset in the file. An efficient implementation
   * would avoid scanning all blocks in the cache.
   *
   * @return the number of blocks evicted
   */
  public int evictBlocksByPrefix(String string);

  /**
   * Get the statistics for this block cache.
   * @return
   */
  public CacheStats getStats();

  /**
   * Shutdown the cache.
   */
  public void shutdown();

  public long size();

  public long getFreeSize();

  public long getCurrentSize();

  public long getEvictedCount();
  
  /**
   * Performs a BlockCache summary and returns a List of BlockCacheColumnFamilySummary objects.
   * This method could be fairly heavyweight in that it evaluates the entire HBase file-system
   * against what is in the RegionServer BlockCache. 
   * <br><br>
   * The contract of this interface is to return the List in sorted order by Table name, then
   * ColumnFamily.
   * 
   * @param conf HBaseConfiguration
   * @return List of BlockCacheColumnFamilySummary
   * @throws IOException exception
   */
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(Configuration conf) throws IOException;
}
