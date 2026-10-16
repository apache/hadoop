/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.io.ByteBufferPool;

import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper {@link ByteBufferPool} implementation that tracks whether all allocated buffers
 * are released.
 * <p>
 * It throws the related exception at {@link #close()} if any buffer remains un-released.
 * It also clears the buffers at release so if they continued being used it'll generate errors.
 * <p>
 * To be used for testing..
 * <p>
 * The stacktraces of the allocation are not stored by default because
 * it can significantly decrease the unit test performance.
 * Configuring this class to log at DEBUG will trigger their collection.
 * @see ByteBufferAllocationStacktraceException
 * <p>
 * Adapted from Parquet class {@code org.apache.parquet.bytes.TrackingByteBufferAllocator}.
 */
@VisibleForTesting
public final class TrackingByteBufferPool implements ByteBufferPool, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TrackingByteBufferPool.class);

  /**
   * Wrap an existing allocator with this tracking allocator.
   * @param allocator allocator to wrap.
   * @return a new allocator.
   */
  public static TrackingByteBufferPool wrap(ByteBufferPool allocator) {
    return new TrackingByteBufferPool(allocator);
  }

  public static class LeakDetectorHeapByteBufferPoolException
      extends RuntimeException {

    private LeakDetectorHeapByteBufferPoolException(String msg) {
      super(msg);
    }

    private LeakDetectorHeapByteBufferPoolException(String msg, Throwable cause) {
      super(msg, cause);
    }

    private LeakDetectorHeapByteBufferPoolException(
        String message,
        Throwable cause,
        boolean enableSuppression,
        boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
    }
  }

  /**
   * Strack trace of allocation as saved in the tracking map.
   */
  public static final class ByteBufferAllocationStacktraceException
      extends LeakDetectorHeapByteBufferPoolException {

    /**
     * Single stack trace instance to use when DEBUG is not enabled.
     */
    private static final ByteBufferAllocationStacktraceException WITHOUT_STACKTRACE =
        new ByteBufferAllocationStacktraceException(false);

    /**
     * Create a stack trace for the map, either using the shared static one
     * or a dynamically created one.
     * @return a stack
     */
    private static ByteBufferAllocationStacktraceException create() {
      return LOG.isDebugEnabled()
          ? new ByteBufferAllocationStacktraceException()
          : WITHOUT_STACKTRACE;
    }

    private ByteBufferAllocationStacktraceException() {
      super("Allocation stacktrace of the first ByteBuffer:");
    }

    /**
     * Private constructor to for the singleton {@link #WITHOUT_STACKTRACE},
     * telling develoers how to see a trace per buffer.
     */
    private ByteBufferAllocationStacktraceException(boolean unused) {
      super("Log org.apache.hadoop.fs.impl.TrackingByteBufferPool at DEBUG for stack traces",
          null,
          false,
          false);
    }
  }

  /**
   * Exception raised in {@link TrackingByteBufferPool#putBuffer(ByteBuffer)} if the
   * buffer to release was not in the hash map.
   */
  public static final class ReleasingUnallocatedByteBufferException
      extends LeakDetectorHeapByteBufferPoolException {

    private ReleasingUnallocatedByteBufferException(final ByteBuffer b) {
      super(String.format("Releasing a ByteBuffer instance that is not allocated"
          + " by this buffer pool or already been released: %s size %d; hash code %s",
          b, b.capacity(), identityHashCode(b)));
    }
  }

  /**
   * Exception raised in {@link TrackingByteBufferPool#close()} if there
   * was an unreleased buffer.
   */
  public static final class LeakedByteBufferException
      extends LeakDetectorHeapByteBufferPoolException {

    private final int count;

    private LeakedByteBufferException(int count, ByteBufferAllocationStacktraceException e) {
      super(count + " ByteBuffer object(s) is/are remained unreleased"
          + " after closing this buffer pool.", e);
      this.count = count;
    }

    /**
     * Get the number of unreleased buffers.
     * @return number of unreleased buffers
     */
    public int getCount() {
      return count;
    }
  }

  /**
   * Tracker of allocations.
   * <p>
   * The key maps by the object id of the buffer, and refers to either a common stack trace
   * or one dynamically created for each allocation.
   */
  private final Map<ByteBuffer, ByteBufferAllocationStacktraceException> allocated =
      new IdentityHashMap<>();

  /**
   * Wrapped buffer pool.
   */
  private final ByteBufferPool allocator;

  /**
   * Number of buffer allocations.
   * <p>
   * This is incremented in {@link #getBuffer(boolean, int)}.
   */
  private final AtomicInteger bufferAllocations = new AtomicInteger();

  /**
   * Number of buffer releases.
   * <p>
   * This is incremented in {@link #putBuffer(ByteBuffer)}.
   */
  private final AtomicInteger bufferReleases = new AtomicInteger();

  /**
   * private constructor.
   * @param allocator pool allocator.
   */
  private TrackingByteBufferPool(ByteBufferPool allocator) {
    this.allocator = allocator;
  }

  public int getBufferAllocations() {
    return bufferAllocations.get();
  }

  public int getBufferReleases() {
    return bufferReleases.get();
  }

  /**
   * Get a buffer from the pool.
   * <p>
   * This increments the {@link #bufferAllocations} counter and stores the
   * singleron or local allocation stack trace in the {@link #allocated} map.
   * @param direct whether to allocate a direct buffer or not
   * @param size size of the buffer to allocate
   * @return a ByteBuffer instance
   */
  @Override
  public synchronized ByteBuffer getBuffer(final boolean direct, final int size) {
    bufferAllocations.incrementAndGet();
    ByteBuffer buffer = allocator.getBuffer(direct, size);
    final ByteBufferAllocationStacktraceException ex =
        ByteBufferAllocationStacktraceException.create();
    allocated.put(buffer, ex);
    LOG.debug("Creating ByteBuffer:{} size {} {}",
        identityHashCode(buffer), size, buffer, ex);
    return buffer;
  }

  /**
   * Release a buffer back to the pool.
   * <p>
   * This increments the {@link #bufferReleases} counter and removes the
   * buffer from the {@link #allocated} map.
   * <p>
   * If the buffer was not allocated by this pool, it throws
   * {@link ReleasingUnallocatedByteBufferException}.
   *
   * @param buffer buffer to release
   * @throws ReleasingUnallocatedByteBufferException if the buffer was not allocated by this pool
   */
  @Override
  public synchronized void putBuffer(ByteBuffer buffer)
      throws ReleasingUnallocatedByteBufferException {

    bufferReleases.incrementAndGet();
    requireNonNull(buffer);
    LOG.debug("Releasing ByteBuffer: {}: {}", identityHashCode(buffer), buffer);
    if (allocated.remove(buffer) == null) {
      throw new ReleasingUnallocatedByteBufferException(buffer);
    }
    allocator.putBuffer(buffer);
    // Clearing the buffer so subsequent access would probably generate errors
    buffer.clear();
  }

  /**
   * Check if the buffer is in the pool.
   * @param buffer buffer
   * @return true if the buffer is in the pool
   */
  public boolean containsBuffer(ByteBuffer buffer) {
    return allocated.containsKey(requireNonNull(buffer));
  }

  /**
   * Get the number of allocated buffers.
   * @return number of allocated buffers
   */
  public int size() {
    return allocated.size();
  }

  /**
   * Expect all buffers to be released -if not, log unreleased ones
   * and then raise an exception with the stack trace of the first
   * unreleased buffer.
   * @throws LeakedByteBufferException if at least one buffer was not released
   */
  @Override
  public void close() throws LeakedByteBufferException {
    if (!allocated.isEmpty()) {
      allocated.keySet().forEach(buffer ->
          LOG.warn("Unreleased ByteBuffer {}; {}", identityHashCode(buffer), buffer));
      LeakedByteBufferException ex = new LeakedByteBufferException(
          allocated.size(),
          allocated.values().iterator().next());
      allocated.clear(); // Drop the references to the ByteBuffers, so they can be gc'd
      throw ex;
    }
  }
}
