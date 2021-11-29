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

import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.BlockManager;
import org.apache.hadoop.fs.common.BufferData;
import org.apache.hadoop.fs.common.Validate;

import com.amazonaws.services.s3.AmazonS3;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.FuturePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides an {@link InputStream} that allows reading from an S3 file.
 * Prefetched blocks are cached to local disk if a seek away from the
 * current block is issued.
 */
public class S3CachingInputStream extends S3InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3CachingInputStream.class);

  // Number of blocks queued for prefching.
  private int numBlocksToPrefetch;

  private BlockManager blockManager;

  public S3CachingInputStream(
      FuturePool futurePool,
      int bufferSize,
      int numBlocksToPrefetch,
      String bucket,
      String key,
      long fileSize,
      AmazonS3 client) {
    super(futurePool, bufferSize, bucket, key, fileSize, client);

    this.numBlocksToPrefetch = numBlocksToPrefetch;
    S3Reader reader = new S3Reader(this.s3File);
    int bufferPoolSize = numBlocksToPrefetch + 1;
    this.blockManager =
        this.createBlockManager(futurePool, reader, this.blockData, bufferPoolSize);
    LOG.debug("Created caching input stream for {}/{} (size = {})", bucket, key, fileSize);
  }

  /**
   * Moves the current read position so that the next read will occur at {@code pos}.
   */
  @Override
  public void seek(long pos) throws IOException {
    Validate.checkWithinRange(pos, "pos", 0, this.s3File.size());

    if (!this.fpos.setAbsolute(pos)) {
      LOG.info("seek({})", getOffsetStr(pos));
      if (this.fpos.isValid()) {
        if (!this.fpos.bufferFullyRead()) {
          this.blockManager.requestCaching(fpos.data());
        } else {
          this.blockManager.release(fpos.data());
        }
        this.fpos.invalidate();
        this.blockManager.cancelPrefetches();
      }
      this.seekTargetPos = pos;
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.blockData = null;
    this.blockManager.close();
    LOG.info("closed: {}", this.name);
  }

  @Override
  protected boolean ensureCurrentBuffer() {
    if (this.closed) {
      return false;
    }

    if (this.fpos.isValid() && this.fpos.buffer().hasRemaining()) {
      return true;
    }

    long readPos;
    int prefetchCount;

    if (this.fpos.isValid()) {
      // A sequential read results in a prefetch.
      readPos = this.fpos.absolute();
      prefetchCount = this.numBlocksToPrefetch;
    } else {
      // A seek invalidates the current position.
      // We prefetch only 1 block immediately after a seek operation.
      readPos = this.seekTargetPos;
      prefetchCount = 1;
    }

    if (!this.blockData.isValidOffset(readPos)) {
      return false;
    }

    if (this.fpos.isValid()) {
      if (this.fpos.bufferFullyRead()) {
        this.blockManager.release(this.fpos.data());
      } else {
        this.blockManager.requestCaching(this.fpos.data());
      }
    }

    int toBlockNumber = this.blockData.getBlockNumber(readPos);
    long startOffset = this.blockData.getStartOffset(toBlockNumber);

    for (int i = 1; i <= prefetchCount; i++) {
      int b = toBlockNumber + i;
      if (b < this.blockData.numBlocks) {
        this.blockManager.requestPrefetch(b);
      }
    }

    BufferData data = null;

    try {
      data = this.blockManager.get(toBlockNumber);
      this.fpos.setData(data, startOffset, readPos);
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
    if (this.closed) {
      return "closed";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("fpos = (%s)\n", this.fpos));
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
