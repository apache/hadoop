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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNegative;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNull;

/**
 * Provides read access to the underlying file one block at a time.
 *
 * This class is the simplest form of a {@code BlockManager} that does
 * perform prefetching or caching.
 */
public abstract class BlockManager implements Closeable {

  /**
   * Information about each block of the underlying file.
   */
  private final BlockData blockData;

  /**
   * Constructs an instance of {@code BlockManager}.
   *
   * @param blockData information about each block of the underlying file.
   *
   * @throws IllegalArgumentException if blockData is null.
   */
  public BlockManager(BlockData blockData) {
    checkNotNull(blockData, "blockData");

    this.blockData = blockData;
  }

  /**
   * Gets block data information.
   *
   * @return instance of {@code BlockData}.
   */
  public BlockData getBlockData() {
    return blockData;
  }

  /**
   * Gets the block having the given {@code blockNumber}.
   *
   * The entire block is read into memory and returned as a {@code BufferData}.
   * The blocks are treated as a limited resource and must be released when
   * one is done reading them.
   *
   * @param blockNumber the number of the block to be read and returned.
   * @return {@code BufferData} having data from the given block.
   *
   * @throws IOException if there an error reading the given block.
   * @throws IllegalArgumentException if blockNumber is negative.
   */
  public BufferData get(int blockNumber) throws IOException {
    checkNotNegative(blockNumber, "blockNumber");

    int size = blockData.getSize(blockNumber);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    long startOffset = blockData.getStartOffset(blockNumber);
    read(buffer, startOffset, size);
    buffer.flip();
    return new BufferData(blockNumber, buffer);
  }

  /**
   * Reads into the given {@code buffer} {@code size} bytes from the underlying file
   * starting at {@code startOffset}.
   *
   * @param buffer the buffer to read data in to.
   * @param startOffset the offset at which reading starts.
   * @param size the number bytes to read.
   * @return number of bytes read.
   * @throws IOException if there an error reading the given block.
   */
  public abstract int read(ByteBuffer buffer, long startOffset, int size) throws IOException;

  /**
   * Releases resources allocated to the given block.
   *
   * @param data the {@code BufferData} to release.
   *
   * @throws IllegalArgumentException if data is null.
   */
  public void release(BufferData data) {
    checkNotNull(data, "data");

    // Do nothing because we allocate a new buffer each time.
  }

  /**
   * Requests optional prefetching of the given block.
   *
   * @param blockNumber the id of the block to prefetch.
   *
   * @throws IllegalArgumentException if blockNumber is negative.
   */
  public void requestPrefetch(int blockNumber) {
    checkNotNegative(blockNumber, "blockNumber");

    // Do nothing because we do not support prefetches.
  }

  /**
   * Requests cancellation of any previously issued prefetch requests.
   */
  public void cancelPrefetches() {
    // Do nothing because we do not support prefetches.
  }

  /**
   * Requests that the given block should be copied to the cache. Optional operation.
   *
   * @param data the {@code BufferData} instance to optionally cache.
   */
  public void requestCaching(BufferData data) {
    // Do nothing because we do not support caching.
  }

  @Override
  public void close() {
  }
}
