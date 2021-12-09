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

import com.twitter.util.FuturePool;
import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.BlockManager;
import org.apache.hadoop.fs.common.BufferData;
import org.apache.hadoop.fs.common.Validate;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides an {@code InputStream} that allows reading from an S3 file.
 * Prefetched blocks are cached to local disk if a seek away from the
 * current block is issued.
 */
public class S3CachingInputStream extends S3InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3CachingInputStream.class);

  // Number of blocks queued for prefching.
  private int numBlocksToPrefetch;

  private BlockManager blockManager;

  public S3CachingInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client) {
    super(context, s3Attributes, client);

    this.numBlocksToPrefetch = this.getContext().getPrefetchBlockCount();
    int bufferPoolSize = this.numBlocksToPrefetch + 1;
    this.blockManager = this.createBlockManager(
        this.getContext().getFuturePool(),
        this.getReader(),
        this.getBlockData(),
        bufferPoolSize);
    int fileSize = (int) s3Attributes.getLen();
    LOG.debug("Created caching input stream for {} (size = {})", this.getName(), fileSize);
  }

  /**
   * Moves the current read position so that the next read will occur at {@code pos}.
   *
   * @throws IllegalArgumentException if pos is outside of the range [0, file size].
   */
  @Override
  public void seek(long pos) throws IOException {
    Validate.checkWithinRange(pos, "pos", 0, this.getFile().size());

    if (!this.getFilePosition().setAbsolute(pos)) {
      LOG.info("seek({})", getOffsetStr(pos));
      if (this.getFilePosition().isValid()) {
        if (!this.getFilePosition().bufferFullyRead()) {
          this.blockManager.requestCaching(this.getFilePosition().data());
        } else {
          this.blockManager.release(this.getFilePosition().data());
        }
        this.getFilePosition().invalidate();
        this.blockManager.cancelPrefetches();
      }
      this.setSeekTargetPos(pos);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.blockManager.close();
    LOG.info("closed: {}", this.getName());
  }

  @Override
  protected boolean ensureCurrentBuffer() {
    if (this.isClosed()) {
      return false;
    }

    if (this.getFilePosition().isValid() && this.getFilePosition().buffer().hasRemaining()) {
      return true;
    }

    long readPos;
    int prefetchCount;

    if (this.getFilePosition().isValid()) {
      // A sequential read results in a prefetch.
      readPos = this.getFilePosition().absolute();
      prefetchCount = this.numBlocksToPrefetch;
    } else {
      // A seek invalidates the current position.
      // We prefetch only 1 block immediately after a seek operation.
      readPos = this.getSeekTargetPos();
      prefetchCount = 1;
    }

    if (!this.getBlockData().isValidOffset(readPos)) {
      return false;
    }

    if (this.getFilePosition().isValid()) {
      if (this.getFilePosition().bufferFullyRead()) {
        this.blockManager.release(this.getFilePosition().data());
      } else {
        this.blockManager.requestCaching(this.getFilePosition().data());
      }
    }

    int toBlockNumber = this.getBlockData().getBlockNumber(readPos);
    long startOffset = this.getBlockData().getStartOffset(toBlockNumber);

    for (int i = 1; i <= prefetchCount; i++) {
      int b = toBlockNumber + i;
      if (b < this.getBlockData().getNumBlocks()) {
        this.blockManager.requestPrefetch(b);
      }
    }

    BufferData data = null;

    try {
      data = this.blockManager.get(toBlockNumber);
      this.getFilePosition().setData(data, startOffset, readPos);
      return true;
    } catch (Exception e) {
      LOG.error("prefetchCount = {}", prefetchCount);
      LOG.error("data = {}", data);
      LOG.error("Error fetching buffer", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    if (this.isClosed()) {
      return "closed";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("fpos = (%s)%n", this.getFilePosition()));
    sb.append(this.blockManager.toString());
    return sb.toString();
  }

  // @VisibleForTesting
  protected BlockManager createBlockManager(
      FuturePool futurePool,
      S3Reader reader,
      BlockData blockData,
      int bufferPoolSize) {
    return new S3CachingBlockManager(futurePool, reader, blockData, bufferPoolSize);
  }
}
