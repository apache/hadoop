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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNegative;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkState;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.Preconditions.checkNotNull;

/**
 * Manages a fixed pool of {@code ByteBuffer} instances.
 * <p>
 * Avoids creating a new buffer if a previously created buffer is already available.
 */
public class BufferPool implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

  /**
   * Max number of buffers in this pool.
   */
  private final int size;

  /**
   * Size in bytes of each buffer.
   */
  private final int bufferSize;

  /*
   Invariants for internal state.
   -- a buffer is either in this.pool or in this.allocated
   -- transition between this.pool <==> this.allocated must be atomic
   -- only one buffer allocated for a given blockNumber
  */


  /**
   * Underlying bounded resource pool.
   */
  private BoundedResourcePool<ByteBuffer> pool;

  /**
   * Allows associating metadata to each buffer in the pool.
   */
  private Map<BufferData, ByteBuffer> allocated;

  /**
   * Prefetching stats.
   */
  private PrefetchingStatistics prefetchingStatistics;

  /**
   * Initializes a new instance of the {@code BufferPool} class.
   * @param size number of buffer in this pool.
   * @param bufferSize size in bytes of each buffer.
   * @param prefetchingStatistics statistics for this stream.
   * @throws IllegalArgumentException if size is zero or negative.
   * @throws IllegalArgumentException if bufferSize is zero or negative.
   */
  public BufferPool(int size,
      int bufferSize,
      PrefetchingStatistics prefetchingStatistics) {
    Validate.checkPositiveInteger(size, "size");
    Validate.checkPositiveInteger(bufferSize, "bufferSize");

    this.size = size;
    this.bufferSize = bufferSize;
    this.allocated = new IdentityHashMap<BufferData, ByteBuffer>();
    this.prefetchingStatistics = requireNonNull(prefetchingStatistics);
    this.pool = new BoundedResourcePool<ByteBuffer>(size) {
      @Override
      public ByteBuffer createNew() {
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        prefetchingStatistics.memoryAllocated(bufferSize);
        return buffer;
      }
    };
  }

  /**
   * Gets a list of all blocks in this pool.
   * @return a list of all blocks in this pool.
   */
  public List<BufferData> getAll() {
    synchronized (allocated) {
      return Collections.unmodifiableList(new ArrayList<>(allocated.keySet()));
    }
  }

  /**
   * Acquires a {@code ByteBuffer}; blocking if necessary until one becomes available.
   * @param blockNumber the id of the block to acquire.
   * @return the acquired block's {@code BufferData}.
   */
  public synchronized BufferData acquire(int blockNumber) {
    BufferData data;
    final int maxRetryDelayMs = 600 * 1000;
    final int statusUpdateDelayMs = 120 * 1000;
    Retryer retryer = new Retryer(10, maxRetryDelayMs, statusUpdateDelayMs);

    do {
      if (retryer.updateStatus()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("waiting to acquire block: {}", blockNumber);
          LOG.debug("state = {}", this);
        }
        releaseReadyBlock(blockNumber);
      }
      data = tryAcquire(blockNumber);
    }
    while ((data == null) && retryer.continueRetry());

    if (data != null) {
      return data;
    } else {
      String message =
          String.format("Wait failed for acquire(%d)", blockNumber);
      throw new IllegalStateException(message);
    }
  }

  /**
   * Acquires a buffer if one is immediately available. Otherwise returns null.
   * @param blockNumber the id of the block to try acquire.
   * @return the acquired block's {@code BufferData} or null.
   */
  public synchronized BufferData tryAcquire(int blockNumber) {
    return acquireHelper(blockNumber, false);
  }

  private synchronized BufferData acquireHelper(int blockNumber,
      boolean canBlock) {
    checkNotNegative(blockNumber, "blockNumber");

    releaseDoneBlocks();

    BufferData data = find(blockNumber);
    if (data != null) {
      return data;
    }

    ByteBuffer buffer = canBlock ? pool.acquire() : pool.tryAcquire();
    if (buffer == null) {
      return null;
    }

    buffer.clear();
    data = new BufferData(blockNumber, buffer.duplicate());

    synchronized (allocated) {
      checkState(find(blockNumber) == null, "buffer data already exists");

      allocated.put(data, buffer);
    }

    return data;
  }

  /**
   * Releases resources for any blocks marked as 'done'.
   */
  private synchronized void releaseDoneBlocks() {
    for (BufferData data : getAll()) {
      if (data.stateEqualsOneOf(BufferData.State.DONE)) {
        release(data);
      }
    }
  }

  /**
   * If no blocks were released after calling releaseDoneBlocks() a few times,
   * we may end up waiting forever. To avoid that situation, we try releasing
   * a 'ready' block farthest away from the given block.
   */
  private synchronized void releaseReadyBlock(int blockNumber) {
    BufferData releaseTarget = null;
    for (BufferData data : getAll()) {
      if (data.stateEqualsOneOf(BufferData.State.READY)) {
        if (releaseTarget == null) {
          releaseTarget = data;
        } else {
          if (distance(data, blockNumber) > distance(releaseTarget,
              blockNumber)) {
            releaseTarget = data;
          }
        }
      }
    }

    if (releaseTarget != null) {
      LOG.warn("releasing 'ready' block: {}", releaseTarget);
      releaseTarget.setDone();
    }
  }

  private int distance(BufferData data, int blockNumber) {
    return Math.abs(data.getBlockNumber() - blockNumber);
  }

  /**
   * Releases a previously acquired resource.
   * @param data the {@code BufferData} instance to release.
   * @throws IllegalArgumentException if data is null.
   * @throws IllegalArgumentException if data cannot be released due to its state.
   */
  public synchronized void release(BufferData data) {
    checkNotNull(data, "data");

    synchronized (data) {
      checkArgument(
          canRelease(data),
          String.format("Unable to release buffer: %s", data));

      ByteBuffer buffer = allocated.get(data);
      if (buffer == null) {
        // Likely released earlier.
        return;
      }
      buffer.clear();
      pool.release(buffer);
      allocated.remove(data);
    }

    releaseDoneBlocks();
  }

  @Override
  public synchronized void close() {
    for (BufferData data : getAll()) {
      Future<Void> actionFuture = data.getActionFuture();
      if (actionFuture != null) {
        actionFuture.cancel(true);
      }
    }

    int currentPoolSize = pool.numCreated();

    pool.close();
    pool = null;

    allocated.clear();
    allocated = null;

    prefetchingStatistics.memoryFreed(currentPoolSize * bufferSize);
  }

  // For debugging purposes.
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(pool.toString());
    sb.append("\n");
    List<BufferData> allData = new ArrayList<>(getAll());
    Collections.sort(allData,
        (d1, d2) -> d1.getBlockNumber() - d2.getBlockNumber());
    for (BufferData data : allData) {
      sb.append(data.toString());
      sb.append("\n");
    }

    return sb.toString();
  }

  // Number of ByteBuffers created so far.
  public synchronized int numCreated() {
    return pool.numCreated();
  }

  // Number of ByteBuffers available to be acquired.
  public synchronized int numAvailable() {
    releaseDoneBlocks();
    return pool.numAvailable();
  }

  private BufferData find(int blockNumber) {
    synchronized (allocated) {
      for (BufferData data : allocated.keySet()) {
        if ((data.getBlockNumber() == blockNumber)
            && !data.stateEqualsOneOf(BufferData.State.DONE)) {
          return data;
        }
      }
    }

    return null;
  }

  private boolean canRelease(BufferData data) {
    return data.stateEqualsOneOf(
        BufferData.State.DONE,
        BufferData.State.READY);
  }
}
