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

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.impl.prefetch.BlockManager;
import org.apache.hadoop.fs.impl.prefetch.BlockManagerParameters;
import org.apache.hadoop.fs.impl.prefetch.BufferData;
import org.apache.hadoop.fs.impl.prefetch.FilePosition;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_PREFETCH_MAX_BLOCKS_COUNT;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_MAX_BLOCKS_COUNT;
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

  public static final int BLOCKS_TO_PREFETCH_AFTER_SEEK = 1;

  /**
   * Number of blocks queued for prefetching.
   */
  private final int numBlocksToPrefetch;

  private final Configuration conf;

  private final LocalDirAllocator localDirAllocator;

  private BlockManager blockManager;

  /**
   * Initializes a new instance of the {@code S3ACachingInputStream} class.
   *
   * @param context read-specific operation context.
   * @param s3Attributes attributes of the S3 object being read.
   * @param client callbacks used for interacting with the underlying S3 client.
   * @param streamStatistics statistics for this stream.
   * @param conf the configuration.
   * @param localDirAllocator the local dir allocator instance.
   * @throws IllegalArgumentException if context is null.
   * @throws IllegalArgumentException if s3Attributes is null.
   * @throws IllegalArgumentException if client is null.
   */
  public S3ACachingInputStream(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client,
      S3AInputStreamStatistics streamStatistics,
      Configuration conf,
      LocalDirAllocator localDirAllocator) {

    super(context, s3Attributes, client, streamStatistics);
    this.conf = conf;
    this.localDirAllocator = localDirAllocator;
    this.numBlocksToPrefetch = getContext().getPrefetchBlockCount();
    demandCreateBlockManager();

    int fileSize = (int) s3Attributes.getLen();
    LOG.debug("Created caching input stream for {} (size = {})", this.getName(),
        fileSize);
    streamStatistics.setPrefetchState(
        true,
        numBlocksToPrefetch,
        context.getPrefetchBlockSize());
  }

  /**
   * Demand create the block manager.
   */
  private synchronized void demandCreateBlockManager() {
    if (blockManager == null) {
      LOG.debug("{}: creating block manager", getName());
      int bufferPoolSize = this.numBlocksToPrefetch + BLOCKS_TO_PREFETCH_AFTER_SEEK;
      final S3AReadOpContext readOpContext = this.getContext();
      BlockManagerParameters blockManagerParamsBuilder =
          new BlockManagerParameters()
              .withPath(readOpContext.getPath())
              .withFuturePool(readOpContext.getFuturePool())
              .withBlockData(getBlockData())
              .withBufferPoolSize(bufferPoolSize)
              .withConf(conf)
              .withLocalDirAllocator(localDirAllocator)
              .withMaxBlocksCount(
                  conf.getInt(PREFETCH_MAX_BLOCKS_COUNT, DEFAULT_PREFETCH_MAX_BLOCKS_COUNT))
              .withPrefetchingStatistics(getS3AStreamStatistics())
              .withTrackerFactory(getS3AStreamStatistics());
      blockManager = createBlockManager(blockManagerParamsBuilder, getReader());
    }
  }

  @Override
  public void close() throws IOException {
    // Close the BlockManager first, cancelling active prefetches,
    // deleting cached files and freeing memory used by buffer pool.
    if (!isClosed()) {
      closeBlockManager();
      super.close();
      LOG.info("closed: {}", getName());
    }
  }

  /**
   * Close the stream and the block manager.
   * @param unbuffer is this an unbuffer operation?
   * @return true if the stream was closed.
   */
  @Override
  protected boolean closeStream(final boolean unbuffer) {
    final boolean b = super.closeStream(unbuffer);
    closeBlockManager();
    return b;
  }

  /**
   * Close the block manager and set to null, if
   * it is not already in this state.
   */
  private synchronized void closeBlockManager() {
    if (blockManager != null) {
      blockManager.close();
    }
  }

  @Override
  protected boolean ensureCurrentBuffer() throws IOException {
    if (isClosed()) {
      return false;
    }
    demandCreateBlockManager();

    long readPos = getNextReadPos();
    if (!getBlockData().isValidOffset(readPos)) {
      // the block exists
      return false;
    }

    // Determine whether this is an out of order read.
    FilePosition filePosition = getFilePosition();
    boolean outOfOrderRead = !filePosition.setAbsolute(readPos);

    if (!outOfOrderRead && filePosition.buffer().hasRemaining()) {
      // Use the current buffer.
      return true;
    }

    boolean resetPrefetching;

    // We are jumping out of the current buffer.
    // if the buffer data is valid, decide whether to cache it or not.
    if (filePosition.isValid()) {

      // There are two cases to consider:
      if (filePosition.bufferFullyRead()) {
        // This buffer was fully read:
        // it is very unlikely that this buffer will be needed again;
        // therefore we release the buffer without caching.
        blockManager.release(filePosition.data());
      } else {
        // there's been a partial read.
        // We will likely need this buffer again (as observed empirically for Parquet)
        // therefore we issue an async request to cache this buffer.
        blockManager.requestCaching(filePosition.data());
      }
      filePosition.invalidate();
      // prefetch policy is based on read order
      resetPrefetching = outOfOrderRead;
    } else {
      // the data wasn't valid.
      // do not treat this as an OOO read: leave all
      // TODO: get the policy right. we want to
      // leave prefetches alone if they are equal to or later than the current read pos
      // but do abort any which have been skipped.
      resetPrefetching = false;
    }

    int prefetchCount;
    if (resetPrefetching) {
      LOG.debug("lazy-seek({})", getOffsetStr(readPos));
      blockManager.cancelPrefetches(BlockManager.CancelReason.RandomIO);

      // We prefetch only 1 block immediately after a seek operation.
      prefetchCount = BLOCKS_TO_PREFETCH_AFTER_SEEK;
    } else {
      // A sequential read results in a prefetch.
      // but
      prefetchCount = numBlocksToPrefetch;
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
        getS3AStreamStatistics().trackDuration(STREAM_READ_BLOCK_ACQUIRE_AND_READ),
        () -> blockManager.get(toBlockNumber));

    filePosition.setData(data, startOffset, readPos);
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%s%n", super.toString()));
    if (isClosed()) {
      sb.append("closed");
    } else {
      sb.append("file position: ").append(getFilePosition());
      // block manager may be null.
      sb.append("; block manager: ").append(blockManager);
    }
    return sb.toString();
  }

  /**
   * Construct an instance of a {@code S3ACachingBlockManager}.
   *
   * @param blockManagerParameters block manager parameters
   * @param reader block reader
   * @return the block manager
   * @throws IllegalArgumentException if reader is null.
   */
  protected BlockManager createBlockManager(
      @Nonnull final BlockManagerParameters blockManagerParameters,
      final S3ARemoteObjectReader reader) {
    return new S3ACachingBlockManager(blockManagerParameters, reader);
  }
}
