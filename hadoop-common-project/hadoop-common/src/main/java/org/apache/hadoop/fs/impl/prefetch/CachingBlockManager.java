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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNegative;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * Provides read access to the underlying file one block at a time.
 * Improve read performance by prefetching and locally caching blocks.
 */
public abstract class CachingBlockManager extends BlockManager {
  private static final Logger LOG = LoggerFactory.getLogger(CachingBlockManager.class);

  private static final LogExactlyOnce LOG_CACHING_DISABLED = new LogExactlyOnce(LOG);
  private static final int TIMEOUT_MINUTES = 60;

  /**
   * Asynchronous tasks are performed in this pool.
   */
  private final ExecutorServiceFuturePool futurePool;

  /**
   * Pool of shared ByteBuffer instances.
   */
  private BufferPool bufferPool;

  /**
   * Size of the in-memory cache in terms of number of blocks.
   * Total memory consumption is up to bufferPoolSize * blockSize.
   */
  private final int bufferPoolSize;

  /**
   * Local block cache.
   */
  private BlockCache cache;

  /**
   * Error counts. For testing purposes.
   */
  private final AtomicInteger numCachingErrors;
  private final AtomicInteger numReadErrors;

  /**
   * Operations performed by this block manager.
   */
  private final BlockOperations ops;

  /**
   * True if the manager has been closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * If a single caching operation takes more than this time (in seconds),
   * we disable caching to prevent further perf degradation due to caching.
   */
  private static final int SLOW_CACHING_THRESHOLD = 5;

  /**
   * Once set to true, any further caching requests will be ignored.
   */
  private final AtomicBoolean cachingDisabled;

  private final PrefetchingStatistics prefetchingStatistics;

  private final Configuration conf;

  private final LocalDirAllocator localDirAllocator;

  /**
   * The path of the underlying file; for logging.
   */
  private Path path;

  /**
   * Constructs an instance of a {@code CachingBlockManager}.
   *
   * @param blockManagerParameters params for block manager.
   * @throws IllegalArgumentException if bufferPoolSize is zero or negative.
   */
  public CachingBlockManager(@Nonnull final BlockManagerParameters blockManagerParameters) {
    super(blockManagerParameters.getBlockData());

    Validate.checkPositiveInteger(blockManagerParameters.getBufferPoolSize(), "bufferPoolSize");

    this.path = requireNonNull(blockManagerParameters.getPath(), "block manager path");
    this.futurePool = requireNonNull(blockManagerParameters.getFuturePool(), "future pool");
    this.bufferPoolSize = blockManagerParameters.getBufferPoolSize();
    this.numCachingErrors = new AtomicInteger();
    this.numReadErrors = new AtomicInteger();
    this.cachingDisabled = new AtomicBoolean();
    this.prefetchingStatistics = requireNonNull(
        blockManagerParameters.getPrefetchingStatistics(), "prefetching statistics");
    this.conf = requireNonNull(blockManagerParameters.getConf(), "configuratin");

    if (getBlockData().getFileSize() > 0) {
      this.bufferPool = new BufferPool(bufferPoolSize, getBlockData().getBlockSize(),
          this.prefetchingStatistics);
      this.cache = createCache(blockManagerParameters.getMaxBlocksCount(),
          blockManagerParameters.getTrackerFactory());
    }

    this.ops = new BlockOperations();
    this.ops.setDebug(LOG.isDebugEnabled());
    this.localDirAllocator = blockManagerParameters.getLocalDirAllocator();
    prefetchingStatistics.setPrefetchDiskCachingState(true);
  }

  /**
   * Gets the block having the given {@code blockNumber}.
   *
   * @throws IllegalArgumentException if blockNumber is negative.
   */
  @Override
  public BufferData get(int blockNumber) throws IOException {
    checkNotNegative(blockNumber, "blockNumber");

    BufferData data;
    final int maxRetryDelayMs = bufferPoolSize * 120 * 1000;
    final int statusUpdateDelayMs = 120 * 1000;
    Retryer retryer = new Retryer(10, maxRetryDelayMs, statusUpdateDelayMs);
    boolean done;

    do {
      if (isClosed()) {
        throw new IOException("this stream is already closed");
      }

      data = bufferPool.acquire(blockNumber);
      done = getInternal(data);

      if (retryer.updateStatus()) {
        LOG.warn("waiting to get block: {}", blockNumber);
        LOG.info("state = {}", this.toString());
      }
    }
    while (!done && retryer.continueRetry());

    if (done) {
      return data;
    } else {
      String message = String.format("Wait failed for get(%d)", blockNumber);
      throw new IllegalStateException(message);
    }
  }

  private boolean getInternal(BufferData data) throws IOException {
    Validate.checkNotNull(data, "data");

    // Opportunistic check without locking.
    if (data.stateEqualsOneOf(
        BufferData.State.PREFETCHING,
        BufferData.State.CACHING,
        BufferData.State.DONE)) {
      return false;
    }

    synchronized (data) {
      // Reconfirm state after locking.
      if (data.stateEqualsOneOf(
          BufferData.State.PREFETCHING,
          BufferData.State.CACHING,
          BufferData.State.DONE)) {
        return false;
      }

      int blockNumber = data.getBlockNumber();
      if (data.getState() == BufferData.State.READY) {
        BlockOperations.Operation op = ops.getPrefetched(blockNumber);
        ops.end(op);
        return true;
      }

      data.throwIfStateIncorrect(BufferData.State.EMPTY);
      read(data);
      return true;
    }
  }

  /**
   * Releases resources allocated to the given block.
   *
   * @throws IllegalArgumentException if data is null.
   */
  @Override
  public void release(BufferData data) {
    if (isClosed()) {
      return;
    }

    Validate.checkNotNull(data, "data");

    BlockOperations.Operation op = ops.release(data.getBlockNumber());
    bufferPool.release(data);
    ops.end(op);
  }

  @Override
  public synchronized void close() {
    if (closed.getAndSet(true)) {
      return;
    }

    final BlockOperations.Operation op = ops.close();

    // Cancel any prefetches in progress.
    cancelPrefetches(CancelReason.Close);

    cleanupWithLogger(LOG, cache);

    ops.end(op);
    LOG.info(ops.getSummary(false));

    bufferPool.close();
    bufferPool = null;
  }

  /**
   * Requests optional prefetching of the given block.
   * The block is prefetched only if we can acquire a free buffer.
   *
   * @throws IllegalArgumentException if blockNumber is negative.
   */
  @Override
  public void requestPrefetch(int blockNumber) {
    checkNotNegative(blockNumber, "blockNumber");

    if (isClosed()) {
      return;
    }

    // We initiate a prefetch only if we can acquire a buffer from the shared pool.
    LOG.debug("{}: Requesting prefetch for block {}", path, blockNumber);
    BufferData data = bufferPool.tryAcquire(blockNumber);
    if (data == null) {
      LOG.debug("{}: no buffer acquired for block {}", path, blockNumber);
      return;
    }
    LOG.debug("{}: acquired {}", path, data);

    // Opportunistic check without locking.
    if (!data.stateEqualsOneOf(BufferData.State.EMPTY)) {
      // The block is ready or being prefetched/cached.
      return;
    }

    synchronized (data) {
      // Reconfirm state after locking.
      if (!data.stateEqualsOneOf(BufferData.State.EMPTY)) {
        // The block is ready or being prefetched/cached.
        return;
      }

      BlockOperations.Operation op = ops.requestPrefetch(blockNumber);
      PrefetchTask prefetchTask = new PrefetchTask(data, this, Instant.now());
      Future<Void> prefetchFuture = futurePool.executeFunction(prefetchTask);
      data.setPrefetch(prefetchFuture);
      ops.end(op);
    }
  }

  /**
   * Requests cancellation of any previously issued prefetch requests.
   * If the reason was switching to random IO, any active prefetched blocks
   * are still cached.
   * @param reason why were prefetches cancelled?
   */
  @Override
  public void cancelPrefetches(final CancelReason reason) {
    LOG.debug("{}: Cancelling prefetches: {}", path, reason);
    BlockOperations.Operation op = ops.cancelPrefetches();

    if (reason == CancelReason.RandomIO) {
      for (BufferData data : bufferPool.getAll()) {
        // We add blocks being prefetched to the local cache so that the prefetch is not wasted.
        // this only done if the reason is random IO-related, not due to close/unbuffer
        if (data.stateEqualsOneOf(BufferData.State.PREFETCHING, BufferData.State.READY)) {
          requestCaching(data);
        }
      }
    } else {
      // free the buffers
      bufferPool.getAll().forEach(BufferData::setDone);
    }

    ops.end(op);
  }

  private void read(BufferData data) throws IOException {
    synchronized (data) {
      try {
        readBlock(data, false, BufferData.State.EMPTY);
      } catch (IOException e) {
        LOG.debug("{}: error reading block {}", path, data.getBlockNumber(), e);
        throw e;
      }
    }
  }

  private void prefetch(BufferData data, Instant taskQueuedStartTime) throws IOException {
    synchronized (data) {
      prefetchingStatistics.executorAcquired(
          Duration.between(taskQueuedStartTime, Instant.now()));
      readBlock(
          data,
          true,
          BufferData.State.PREFETCHING,
          BufferData.State.CACHING);
    }
  }

  private void readBlock(BufferData data, boolean isPrefetch, BufferData.State... expectedState)
      throws IOException {

    if (isClosed()) {
      return;
    }

    BlockOperations.Operation op = null;
    DurationTracker tracker = null;

    int bytesFetched = 0;
    // to be filled in later.
    long offset = 0;
    int size = 0;
    synchronized (data) {
      try {
        if (data.stateEqualsOneOf(BufferData.State.DONE, BufferData.State.READY)) {
          // DONE  : Block was released, likely due to caching being disabled on slow perf.
          // READY : Block was already fetched by another thread. No need to re-read.
          return;
        }

        data.throwIfStateIncorrect(expectedState);
        int blockNumber = data.getBlockNumber();
        final BlockData blockData = getBlockData();

        offset = blockData.getStartOffset(data.getBlockNumber());
        size = blockData.getSize(data.getBlockNumber());

        // Prefer reading from cache over reading from network.
        if (cache.containsBlock(blockNumber)) {
          op = ops.getCached(blockNumber);
          cache.get(blockNumber, data.getBuffer());
          data.setReady(expectedState);
          return;
        }

        if (isPrefetch) {
          tracker = prefetchingStatistics.prefetchOperationStarted();
          op = ops.prefetch(data.getBlockNumber());
        } else {
          tracker = prefetchingStatistics.blockFetchOperationStarted();
          op = ops.getRead(data.getBlockNumber());
        }


        ByteBuffer buffer = data.getBuffer();
        buffer.clear();
        read(buffer, offset, size);
        buffer.flip();
        data.setReady(expectedState);
        bytesFetched = size;
        LOG.debug("Completed {} of block {} [{}-{}]",
            isPrefetch ? "prefetch" : "read",
            data.getBlockNumber(),
            offset, offset + size);
      } catch (Exception e) {
        LOG.debug("Failure in {} of block {} [{}-{}]",
            isPrefetch ? "prefetch" : "read",
            data.getBlockNumber(),
            offset, offset + size,
            e);
        if (isPrefetch && tracker != null) {
          tracker.failed();
        }

        numReadErrors.incrementAndGet();
        data.setDone();
        throw e;
      } finally {
        if (op != null) {
          ops.end(op);
        }

        // update the statistics
        prefetchingStatistics.fetchOperationCompleted(isPrefetch, bytesFetched);
        if (tracker != null) {
          tracker.close();
          LOG.debug("fetch completed: {}", tracker);
        }
      }
    }
  }

  /**
   * True if the manager has been closed.
   */
  private boolean isClosed() {
    return closed.get();
  }

  /**
   * Disable caching; updates stream statistics and logs exactly once
   * at info.
   * @param endOp operation which measured the duration of the write.
   */
  private void disableCaching(final BlockOperations.End endOp) {
    if (!cachingDisabled.getAndSet(true)) {
      String message = String.format(
          "%s: Caching disabled because of slow operation (%.1f sec)",
          path, endOp.duration());
      LOG_CACHING_DISABLED.info(message);
      prefetchingStatistics.setPrefetchDiskCachingState(false);
    }
  }

  private boolean isCachingDisabled() {
    return cachingDisabled.get();
  }

  /**
   * Read task that is submitted to the future pool.
   */
  private class PrefetchTask implements Supplier<Void> {
    private final BufferData data;
    private final CachingBlockManager blockManager;
    private final Instant taskQueuedStartTime;

    PrefetchTask(BufferData data, CachingBlockManager blockManager, Instant taskQueuedStartTime) {
      this.data = data;
      this.blockManager = blockManager;
      this.taskQueuedStartTime = taskQueuedStartTime;
    }

    @Override
    public Void get() {
      try {
        blockManager.prefetch(data, taskQueuedStartTime);
      } catch (Exception e) {
        LOG.info("{}: error prefetching block {}. {}", path, data.getBlockNumber(), e.getMessage());
        LOG.debug("{}: error prefetching block {}", path, data.getBlockNumber(), e);
      }
      return null;
    }
  }

  /**
   * Required state of a block for it to be cacheable.
   */
  private static final BufferData.State[] EXPECTED_STATE_AT_CACHING = {
      BufferData.State.PREFETCHING, BufferData.State.READY
  };

  /**
   * Requests that the given block should be copied to the local cache.
   * If the stream is closed or caching disabled, the request is denied.
   * <p>
   * If the block is in being prefetched, the
   * <p>
   * The block must not be accessed by the caller after calling this method
   * because it may be released asynchronously relative to the caller.
   * <p>
   * @param data the block to cache..
   * @throws IllegalArgumentException if data is null.
   */
  @Override
  public void requestCaching(BufferData data) {
    Validate.checkNotNull(data, "data");

    final int blockNumber = data.getBlockNumber();
    LOG.debug("{}: Block {}: request caching of {}", path, blockNumber, data);

    if (isClosed() || isCachingDisabled()) {
      data.setDone();
      return;
    }

    // Opportunistic check without locking.
    if (!data.stateEqualsOneOf(EXPECTED_STATE_AT_CACHING)) {
      LOG.debug("{}: Block {}: Block in wrong state to cache: {}",
          path, blockNumber, data.getState());
      return;
    }

    synchronized (data) {
      // Reconfirm state after locking.
      if (!data.stateEqualsOneOf(EXPECTED_STATE_AT_CACHING)) {
        LOG.debug("{}: Block {}: Block in wrong state to cache: {}",
            path, blockNumber, data.getState());
        return;
      }

      if (cache.containsBlock(data.getBlockNumber())) {
        LOG.debug("{}: Block {}: Block is already in cache", path, blockNumber);
        data.setDone();
        return;
      }

      BufferData.State state = data.getState();

      BlockOperations.Operation op = ops.requestCaching(data.getBlockNumber());
      Future<Void> blockFuture;
      if (state == BufferData.State.PREFETCHING) {
        blockFuture = data.getActionFuture();
      } else {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.complete(null);
        blockFuture = cf;
      }

      CachePutTask task =
          new CachePutTask(data, blockFuture, this, Instant.now());
      Future<Void> actionFuture = futurePool.executeFunction(task);
      data.setCaching(actionFuture);
      ops.end(op);
    }
  }

  private void addToCacheAndRelease(BufferData data, Future<Void> blockFuture,
      Instant taskQueuedStartTime) {
    prefetchingStatistics.executorAcquired(
        Duration.between(taskQueuedStartTime, Instant.now()));

    if (isClosed()) {
      return;
    }

    final int blockNumber = data.getBlockNumber();
    LOG.debug("{}: Block {}: Preparing to cache block", path, blockNumber);

    if (isCachingDisabled()) {
      LOG.debug("{}: Block {}: Preparing caching disabled, not prefetching", path, blockNumber);
      data.setDone();
      return;
    }
    LOG.debug("{}: Block {}: awaiting any read to complete", path, blockNumber);

    try {
      // wait for data; state of caching may change during this period.
      awaitFuture(blockFuture, TIMEOUT_MINUTES, TimeUnit.MINUTES);
      if (data.stateEqualsOneOf(BufferData.State.DONE)) {
        // There was an error during prefetch.
        LOG.debug("{}: Block {}: prefetch failure", path, blockNumber);
        return;
      }
    } catch (IOException | TimeoutException e) {
      LOG.info("{}: Error fetching block: {}. {}", path, data, e.toString());
      LOG.debug("{}: Error fetching block: {}", path, data, e);
      data.setDone();
      return;
    }

    if (isCachingDisabled()) {
      // caching was disabled while waiting fro the read to complete.
      LOG.debug("{}: Block {}: caching disabled while reading data", path, blockNumber);
      data.setDone();
      return;
    }

    BlockOperations.Operation op = null;

    synchronized (data) {
      try {
        if (data.stateEqualsOneOf(BufferData.State.DONE)) {
          LOG.debug("{}: Block {}: block no longer in use; not adding", path, blockNumber);
          return;
        }

        if (cache.containsBlock(blockNumber)) {
          LOG.debug("{}: Block {}: already in cache; not adding", path, blockNumber);
          data.setDone();
          return;
        }

        op = ops.addToCache(blockNumber);
        ByteBuffer buffer = data.getBuffer().duplicate();
        buffer.rewind();
        cachePut(blockNumber, buffer);
        data.setDone();
      } catch (Exception e) {
        numCachingErrors.incrementAndGet();
        LOG.info("{}: error adding block to cache: {}. {}", path, data, e.getMessage());
        LOG.debug("{}: error adding block to cache: {}", path, data, e);
        data.setDone();
      }

      if (op != null) {
        BlockOperations.End endOp = (BlockOperations.End) ops.end(op);
        if (endOp.duration() > SLOW_CACHING_THRESHOLD) {
          disableCaching(endOp);
        }
      }
    }
  }

  protected BlockCache createCache(int maxBlocksCount, DurationTrackerFactory trackerFactory) {
    return new SingleFilePerBlockCache(prefetchingStatistics, maxBlocksCount, trackerFactory);
  }

  protected void cachePut(int blockNumber, ByteBuffer buffer) throws IOException {
    if (isClosed()) {
      return;
    }
    LOG.debug("{}: Block {}: Caching", path, buffer);

    cache.put(blockNumber, buffer, conf, localDirAllocator);
  }

  private static class CachePutTask implements Supplier<Void> {
    private final BufferData data;

    // Block being asynchronously fetched.
    private final Future<Void> blockFuture;

    // Block manager that manages this block.
    private final CachingBlockManager blockManager;

    private final Instant taskQueuedStartTime;

    CachePutTask(
        BufferData data,
        Future<Void> blockFuture,
        CachingBlockManager blockManager,
        Instant taskQueuedStartTime) {
      this.data = data;
      this.blockFuture = blockFuture;
      this.blockManager = blockManager;
      this.taskQueuedStartTime = taskQueuedStartTime;
    }

    @Override
    public Void get() {
      blockManager.addToCacheAndRelease(data, blockFuture, taskQueuedStartTime);
      return null;
    }
  }

  /**
   * Number of ByteBuffers available to be acquired.
   *
   * @return the number of available buffers.
   */
  public int numAvailable() {
    return bufferPool.numAvailable();
  }

  /**
   * Number of caching operations completed.
   *
   * @return the number of cached buffers.
   */
  public int numCached() {
    return cache.size();
  }

  /**
   * Number of errors encountered when caching.
   *
   * @return the number of errors encountered when caching.
   */
  public int numCachingErrors() {
    return numCachingErrors.get();
  }

  /**
   * Number of errors encountered when reading.
   *
   * @return the number of errors encountered when reading.
   */
  public int numReadErrors() {
    return numReadErrors.get();
  }

  BufferData getData(int blockNumber) {
    return bufferPool.tryAcquire(blockNumber);
  }

  @Override
  public String toString() {

    String sb = "path: " + path + "; "
        + "; cache(" + cache + "); "
        + "pool: " + bufferPool
        + "; numReadErrors: " + numReadErrors.get()
        + "; numCachingErrors: " + numCachingErrors.get();

    return sb;
  }
}
