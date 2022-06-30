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

package org.apache.hadoop.fs.s3a.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.CachingBlockManager;
import org.apache.hadoop.fs.common.Validate;

/**
 * Provides access to S3 file one block at a time.
 */
public class S3CachingBlockManager extends CachingBlockManager {
  private static final Logger LOG = LoggerFactory.getLogger(S3CachingBlockManager.class);

  /**
   * Reader that reads from S3 file.
   */
  private final S3Reader reader;

  /**
   * Constructs an instance of a {@code S3CachingBlockManager}.
   *
   * @param threadPool asynchronous tasks are performed in this pool.
   * @param reader reader that reads from S3 file.
   * @param blockData information about each block of the S3 file.
   * @param bufferPoolSize size of the in-memory cache in terms of number of blocks.
   *
   * @throws IllegalArgumentException if reader is null.
   */
  public S3CachingBlockManager(
      ExecutorService threadPool,
      S3Reader reader,
      BlockData blockData,
      int bufferPoolSize) {
    super(threadPool, blockData, bufferPoolSize);

    Validate.checkNotNull(reader, "reader");

    this.reader = reader;
  }

  protected S3Reader getReader() {
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
  public int read(ByteBuffer buffer, long startOffset, int size) throws IOException {
    return this.reader.read(buffer, startOffset, size);
  }

  @Override
  public synchronized void close() {
    this.reader.close();

    super.close();
  }
}
