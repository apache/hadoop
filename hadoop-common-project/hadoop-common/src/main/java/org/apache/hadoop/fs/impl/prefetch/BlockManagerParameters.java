/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * This class is used to provide parameters to {@link BlockManager}.
 */
@InterfaceAudience.Private
public final class BlockManagerParameters {

  /**
   * Asynchronous tasks are performed in this pool.
   */
  private ExecutorServiceFuturePool futurePool;

  /**
   * Information about each block of the underlying file.
   */
  private BlockData blockData;

  /**
   * Size of the in-memory cache in terms of number of blocks.
   */
  private int bufferPoolSize;

  /**
   * Statistics for the stream.
   */
  private PrefetchingStatistics prefetchingStatistics;

  /**
   * The configuration object.
   */
  private Configuration conf;

  /**
   * The local dir allocator instance.
   */
  private LocalDirAllocator localDirAllocator;

  /**
   * Max blocks count to be kept in cache at any time.
   */
  private int maxBlocksCount;

  /**
   * Tracker with statistics to update.
   */
  private DurationTrackerFactory trackerFactory;

  /**
   * @return The Executor future pool to perform async prefetch tasks.
   */
  public ExecutorServiceFuturePool getFuturePool() {
    return futurePool;
  }

  /**
   * @return The object holding blocks data info for the underlying file.
   */
  public BlockData getBlockData() {
    return blockData;
  }

  /**
   * @return The size of the in-memory cache.
   */
  public int getBufferPoolSize() {
    return bufferPoolSize;
  }

  /**
   * @return The prefetching statistics for the stream.
   */
  public PrefetchingStatistics getPrefetchingStatistics() {
    return prefetchingStatistics;
  }

  /**
   * @return The configuration object.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * @return The local dir allocator instance.
   */
  public LocalDirAllocator getLocalDirAllocator() {
    return localDirAllocator;
  }

  /**
   * @return The max blocks count to be kept in cache at any time.
   */
  public int getMaxBlocksCount() {
    return maxBlocksCount;
  }

  /**
   * @return The duration tracker with statistics to update.
   */
  public DurationTrackerFactory getTrackerFactory() {
    return trackerFactory;
  }

  /**
   * Sets the executor service future pool that is later used to perform
   * async prefetch tasks.
   *
   * @param pool The future pool.
   * @return The builder.
   */
  public BlockManagerParameters withFuturePool(
      final ExecutorServiceFuturePool pool) {
    this.futurePool = pool;
    return this;
  }

  /**
   * Sets the object holding blocks data info for the underlying file.
   *
   * @param data The block data object.
   * @return The builder.
   */
  public BlockManagerParameters withBlockData(
      final BlockData data) {
    this.blockData = data;
    return this;
  }

  /**
   * Sets the in-memory cache size as number of blocks.
   *
   * @param poolSize The buffer pool size as number of blocks.
   * @return The builder.
   */
  public BlockManagerParameters withBufferPoolSize(
      final int poolSize) {
    this.bufferPoolSize = poolSize;
    return this;
  }

  /**
   * Sets the prefetching statistics for the stream.
   *
   * @param statistics The prefetching statistics.
   * @return The builder.
   */
  public BlockManagerParameters withPrefetchingStatistics(
      final PrefetchingStatistics statistics) {
    this.prefetchingStatistics = statistics;
    return this;
  }

  /**
   * Sets the configuration object.
   *
   * @param configuration The configuration object.
   * @return The builder.
   */
  public BlockManagerParameters withConf(
      final Configuration configuration) {
    this.conf = configuration;
    return this;
  }

  /**
   * Sets the local dir allocator for round-robin disk allocation
   * while creating files.
   *
   * @param dirAllocator The local dir allocator object.
   * @return The builder.
   */
  public BlockManagerParameters withLocalDirAllocator(
      final LocalDirAllocator dirAllocator) {
    this.localDirAllocator = dirAllocator;
    return this;
  }

  /**
   * Sets the max blocks count to be kept in cache at any time.
   *
   * @param blocksCount The max blocks count.
   * @return The builder.
   */
  public BlockManagerParameters withMaxBlocksCount(
      final int blocksCount) {
    this.maxBlocksCount = blocksCount;
    return this;
  }

  /**
   * Sets the duration tracker with statistics to update.
   *
   * @param factory The tracker factory object.
   * @return The builder.
   */
  public BlockManagerParameters withTrackerFactory(
      final DurationTrackerFactory factory) {
    this.trackerFactory = factory;
    return this;
  }

}
