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
 * This class is used to provide params to {@link BlockManager}.
 */
@InterfaceAudience.Private
public class BlockManagerParams {

  /**
   * Asynchronous tasks are performed in this pool.
   */
  private final ExecutorServiceFuturePool futurePool;
  /**
   * Information about each block of the underlying file.
   */
  private final BlockData blockData;
  /**
   * Size of the in-memory cache in terms of number of blocks.
   */
  private final int bufferPoolSize;
  /**
   * Statistics for the stream.
   */
  private final PrefetchingStatistics prefetchingStatistics;
  /**
   * The configuration object.
   */
  private final Configuration conf;
  /**
   * The local dir allocator instance.
   */
  private final LocalDirAllocator localDirAllocator;
  /**
   * Max blocks count to be kept in cache at any time.
   */
  private final int maxBlocksCount;
  /**
   * Tracker with statistics to update.
   */
  private final DurationTrackerFactory trackerFactory;

  private BlockManagerParams(ExecutorServiceFuturePool futurePool, BlockData blockData,
      int bufferPoolSize, PrefetchingStatistics prefetchingStatistics, Configuration conf,
      LocalDirAllocator localDirAllocator, int maxBlocksCount,
      DurationTrackerFactory trackerFactory) {
    this.futurePool = futurePool;
    this.blockData = blockData;
    this.bufferPoolSize = bufferPoolSize;
    this.prefetchingStatistics = prefetchingStatistics;
    this.conf = conf;
    this.localDirAllocator = localDirAllocator;
    this.maxBlocksCount = maxBlocksCount;
    this.trackerFactory = trackerFactory;
  }

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

  public static class BlockManagerParamsBuilder {
    private ExecutorServiceFuturePool futurePool;
    private BlockData blockData;
    private int bufferPoolSize;
    private PrefetchingStatistics prefetchingStatistics;
    private Configuration conf;
    private LocalDirAllocator localDirAllocator;
    private int maxBlocksCount;
    private DurationTrackerFactory trackerFactory;

    public BlockManagerParamsBuilder setFuturePool(ExecutorServiceFuturePool futurePool) {
      this.futurePool = futurePool;
      return this;
    }

    public BlockManagerParamsBuilder setBlockData(BlockData blockData) {
      this.blockData = blockData;
      return this;
    }

    public BlockManagerParamsBuilder setBufferPoolSize(int bufferPoolSize) {
      this.bufferPoolSize = bufferPoolSize;
      return this;
    }

    public BlockManagerParamsBuilder setPrefetchingStatistics(
        PrefetchingStatistics prefetchingStatistics) {
      this.prefetchingStatistics = prefetchingStatistics;
      return this;
    }

    public BlockManagerParamsBuilder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public BlockManagerParamsBuilder setLocalDirAllocator(LocalDirAllocator localDirAllocator) {
      this.localDirAllocator = localDirAllocator;
      return this;
    }

    public BlockManagerParamsBuilder setMaxBlocksCount(int maxBlocksCount) {
      this.maxBlocksCount = maxBlocksCount;
      return this;
    }

    public BlockManagerParamsBuilder setTrackerFactory(DurationTrackerFactory trackerFactory) {
      this.trackerFactory = trackerFactory;
      return this;
    }

    public BlockManagerParams build() {
      return new BlockManagerParams(futurePool, blockData, bufferPoolSize, prefetchingStatistics,
          conf, localDirAllocator, maxBlocksCount, trackerFactory);
    }
  }
}
