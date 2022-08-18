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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.prefetch.BlockData;
import org.apache.hadoop.fs.impl.prefetch.BlockManager;
import org.apache.hadoop.fs.impl.prefetch.BufferData;
import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;

import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BLOCK_ACQUIRE_AND_READ;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.invokeTrackingDuration;

/**
 * Provides an {@code InputStream} that allows reading from an S3 file.
 * Prefetched blocks are cached to local disk if a seek away from the
 * current block is issued.
 */
public class S3ACachingInputStream extends S3ARemoteInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ACachingInputStream.class);

  /**
   * Number of blocks queued for prefching.
   */
  private final int numBlocksToPrefetch;

  private final BlockManager blockManager;

  /**
   * Initializes a new instance of the {@code S3ACachingInputStream} class.
   *
   * @param context read-specific operation context.
   * @param s3Attributes attributes of the S3 object being read.
   * @param client callbacks used for interacting with the underlying S3 client.
   * @param streamStatistics statistics for this stream.
   *
   * @throws IllegalArgumentException if context is null.
   * @throws IllegalArgumentException if s3Attributes is null.
   * @throws IllegalArgumentException if client is null.
   */
  public S3ACachingInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client,
      S3AInputStreamStatistics streamStatistics) {
    super(context, s3Attributes, client, streamStatistics);

    this.numBlocksToPrefetch = this.getContext().getPrefetchBlockCount();
    int bufferPoolSize = this.numBlocksToPrefetch + 1;
    this.blockManager = this.createBlockManager(
        this.getContext().getFuturePool(),
        this.getReader(),
        this.getBlockData(),
        bufferPoolSize);
    int fileSize = (int) s3Attributes.getLen();
    LOG.debug("Created caching input stream for {} (size = {})", this.getName(),
        fileSize);
  }

  /**
   * Moves the current read position so that the next read will occur at {@code pos}.
   *
   * @param pos the next read will take place at this position.
   *
   * @throws IllegalArgumentException if pos is outside of the range [0, file size].
   */
  @Override
  public void seek(long pos) throws IOException {
    throwIfClosed();
    throwIfInvalidSeek(pos);

    // The call to setAbsolute() returns true if the target position is valid and
    // within the current block. Therefore, no additional work is needed when we get back true.
    if (!getFilePosition().setAbsolute(pos)) {
      LOG.info("seek({})", getOffsetStr(pos));
      // We could be here in two cases:
      // -- the target position is invalid:
      //    We ignore this case here as the next read will return an error.
      // -- it is valid but outside of the current block.
      if (getFilePosition().isValid()) {
        // There are two cases to consider:
        // -- the seek was issued after this buffer was fully read.
        //    In this case, it is very unlikely that this buffer will be needed again;
        //    therefore we release the buffer without caching.
        // -- if we are jumping out of the buffer before reading it completely then
        //    we will likely need this buffer again (as observed empirically for Parquet)
        //    therefore we issue an async request to cache this buffer.
        if (!getFilePosition().bufferFullyRead()) {
          blockManager.requestCaching(getFilePosition().data());
        } else {
          blockManager.release(getFilePosition().data());
        }
        getFilePosition().invalidate();
        blockManager.cancelPrefetches();
      }
      setSeekTargetPos(pos);
    }
  }

  @Override
  public void close() throws IOException {
    // Close the BlockManager first, cancelling active prefetches,
    // deleting cached files and freeing memory used by buffer pool.
    blockManager.close();
    super.close();
    LOG.info("closed: {}", getName());
  }

  @Override
  protected boolean ensureCurrentBuffer() throws IOException {
    if (isClosed()) {
      return false;
    }

    if (getFilePosition().isValid() && getFilePosition()
        .buffer()
        .hasRemaining()) {
      return true;
    }

    long readPos;
    int prefetchCount;

    if (getFilePosition().isValid()) {
      // A sequential read results in a prefetch.
      readPos = getFilePosition().absolute();
      prefetchCount = numBlocksToPrefetch;
    } else {
      // A seek invalidates the current position.
      // We prefetch only 1 block immediately after a seek operation.
      readPos = getSeekTargetPos();
      prefetchCount = 1;
    }

    if (!getBlockData().isValidOffset(readPos)) {
      return false;
    }

    if (getFilePosition().isValid()) {
      if (getFilePosition().bufferFullyRead()) {
        blockManager.release(getFilePosition().data());
      } else {
        blockManager.requestCaching(getFilePosition().data());
      }
    }

    int toBlockNumber = getBlockData().getBlockNumber(readPos);
    long startOffset = getBlockData().getStartOffset(toBlockNumber);

    for (int i = 1; i <= prefetchCount; i++) {
      int b = toBlockNumber + i;
      if (b < getBlockData().getNumBlocks()) {
        blockManager.requestPrefetch(b);
      }
    }

    BufferData data = invokeTrackingDuration(
        getS3AStreamStatistics()
            .trackDuration(STREAM_READ_BLOCK_ACQUIRE_AND_READ),
        () -> blockManager.get(toBlockNumber));

    getFilePosition().setData(data, startOffset, readPos);
    return true;
  }

  @Override
  public String toString() {
    if (isClosed()) {
      return "closed";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("fpos = (%s)%n", getFilePosition()));
    sb.append(blockManager.toString());
    return sb.toString();
  }

  protected BlockManager createBlockManager(
      ExecutorServiceFuturePool futurePool,
      S3ARemoteObjectReader reader,
      BlockData blockData,
      int bufferPoolSize) {
    return new S3ACachingBlockManager(futurePool, reader, blockData,
        bufferPoolSize,
        getS3AStreamStatistics());
  }
}
