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

import org.apache.hadoop.fs.impl.prefetch.BlockData;
import org.apache.hadoop.fs.impl.prefetch.BlockManager;
import org.apache.hadoop.fs.impl.prefetch.Validate;

/**
 * Provides read access to S3 file one block at a time.
 *
 * A naive implementation of a {@code BlockManager} that provides no prefetching or caching.
 * Useful baseline for comparing performance difference against {@code S3ACachingBlockManager}.
 */
public class S3ABlockManager extends BlockManager {

  /**
   * Reader that reads from S3 file.
   */
  private final S3ARemoteObjectReader reader;

  /**
   * Constructs an instance of {@code S3ABlockManager}.
   *
   * @param reader a reader that reads from S3 file.
   * @param blockData information about each block of the S3 file.
   *
   * @throws IllegalArgumentException if reader is null.
   * @throws IllegalArgumentException if blockData is null.
   */
  public S3ABlockManager(S3ARemoteObjectReader reader, BlockData blockData) {
    super(blockData);

    Validate.checkNotNull(reader, "reader");

    this.reader = reader;
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
    return reader.read(buffer, startOffset, size);
  }

  @Override
  public void close() {
    reader.close();
  }
}
