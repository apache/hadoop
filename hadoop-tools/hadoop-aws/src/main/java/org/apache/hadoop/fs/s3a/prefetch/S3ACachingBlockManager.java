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

package org.apache.hadoop.fs.s3a.prefetch;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.impl.prefetch.BlockData;
import org.apache.hadoop.fs.impl.prefetch.CachingBlockManager;
import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_PREFETCH_MAX_BLOCKS_COUNT;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_MAX_BLOCKS_COUNT;

/**
 * Provides access to S3 file one block at a time.
 */
public class S3ACachingBlockManager extends CachingBlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ACachingBlockManager.class);

  /**
   * Reader that reads from S3 file.
   */
  private final S3ARemoteObjectReader reader;

  /**
   * Constructs an instance of a {@code S3ACachingBlockManager}.
   *
   * @param futurePool asynchronous tasks are performed in this pool.
   * @param reader reader that reads from S3 file.
   * @param blockData information about each block of the S3 file.
   * @param bufferPoolSize size of the in-memory cache in terms of number of blocks.
   * @param streamStatistics statistics for this stream.
   * @param conf the configuration.
   * @param localDirAllocator the local dir allocator instance.
   * @throws IllegalArgumentException if reader is null.
   */
  public S3ACachingBlockManager(
      ExecutorServiceFuturePool futurePool,
      S3ARemoteObjectReader reader,
      BlockData blockData,
      int bufferPoolSize,
      S3AInputStreamStatistics streamStatistics,
      Configuration conf,
      LocalDirAllocator localDirAllocator) {

    super(futurePool,
        blockData,
        bufferPoolSize,
        streamStatistics,
        conf,
        localDirAllocator,
        conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT));

    Validate.checkNotNull(reader, "reader");

    this.reader = reader;
  }

  protected S3ARemoteObjectReader getReader() {
    return this.reader;
  }

  /**
   * Reads into the given {@code buffer} {@code size} bytes from the underlying file
   * starting at {@code startOffset}.
   *
   * @param buffer the buffer to read data in to.
   * @param startOffset the offset at which reading starts.
   * @param size the number bytes to read.
   * @return number of bytes read.
   */
  @Override
  public int read(ByteBuffer buffer, long startOffset, int size)
      throws IOException {
    return this.reader.read(buffer, startOffset, size);
  }

  @Override
  public synchronized void close() {
    this.reader.close();

    super.close();
  }
}
