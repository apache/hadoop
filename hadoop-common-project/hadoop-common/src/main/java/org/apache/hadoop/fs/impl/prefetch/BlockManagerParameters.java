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

  public ExecutorServiceFuturePool getFuturePool() {
    return futurePool;
  }

  public BlockData getBlockData() {
    return blockData;
  }

  public int getBufferPoolSize() {
    return bufferPoolSize;
  }

  public PrefetchingStatistics getPrefetchingStatistics() {
    return prefetchingStatistics;
  }

  public Configuration getConf() {
    return conf;
  }

  public LocalDirAllocator getLocalDirAllocator() {
    return localDirAllocator;
  }

  public int getMaxBlocksCount() {
    return maxBlocksCount;
  }

  public DurationTrackerFactory getTrackerFactory() {
    return trackerFactory;
  }

  public BlockManagerParameters setFuturePool(
      ExecutorServiceFuturePool pool) {
    this.futurePool = pool;
    return this;
  }

  public BlockManagerParameters setBlockData(BlockData data) {
    this.blockData = data;
    return this;
  }

  public BlockManagerParameters setBufferPoolSize(int poolSize) {
    this.bufferPoolSize = poolSize;
    return this;
  }

  public BlockManagerParameters setPrefetchingStatistics(
      PrefetchingStatistics statistics) {
    this.prefetchingStatistics = statistics;
    return this;
  }

  public BlockManagerParameters setConf(Configuration configuration) {
    this.conf = configuration;
    return this;
  }

  public BlockManagerParameters setLocalDirAllocator(
      LocalDirAllocator dirAllocator) {
    this.localDirAllocator = dirAllocator;
    return this;
  }

  public BlockManagerParameters setMaxBlocksCount(int blocksCount) {
    this.maxBlocksCount = blocksCount;
    return this;
  }

  public BlockManagerParameters setTrackerFactory(
      DurationTrackerFactory factory) {
    this.trackerFactory = factory;
    return this;
  }

}
