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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.ByteBufferPool;

/**
 * A wrapper {@link ByteBufferPool} implementation that tracks whether all allocated buffers are released. It
 * throws the related exception at {@link #close()} if any buffer remains un-released. It also clears the buffers at
 * release so if they continued being used it'll generate errors.
 * <p>
 * To be used for testing only.
 * <p>
 * The stacktraces of the allocation are not stored by default because it significantly decreases the unit test
 * execution performance. Configuring this class to log at DEBUG will trigger their collection.
 * @see ByteBufferAllocationStacktraceException
 * <p>
 * Adapted from Parquet class {@code org.apache.parquet.bytes.TrackingByteBufferAllocator}.
 */
public final class TrackingByteBufferPool implements ByteBufferPool, AutoCloseable {

  /**

   */
  private static final boolean DEBUG = true;
  private static final Logger LOG = LoggerFactory.getLogger(TrackingByteBufferPool.class);

  /**
   * Wrap an existing allocator with this tracking allocator.
   * @param allocator allocator to wrap.
   * @return a new allocator.
   */
  public static TrackingByteBufferPool wrap(ByteBufferPool allocator) {
    return new TrackingByteBufferPool(allocator);
  }

  /**
   * Key for the tracker map.
   */
  private static class Key {

    private final int hashCode;
    private final ByteBuffer buffer;

    Key(ByteBuffer buffer) {
      hashCode = System.identityHashCode(buffer);
      this.buffer = buffer;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return this.buffer == key.buffer;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return buffer.toString();
    }
  }

  public static class LeakDetectorHeapByteBufferPoolException extends RuntimeException {

    private LeakDetectorHeapByteBufferPoolException(String msg) {
      super(msg);
    }

    private LeakDetectorHeapByteBufferPoolException(String msg, Throwable cause) {
      super(msg, cause);
    }

    private LeakDetectorHeapByteBufferPoolException(
        String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
    }
  }

  /**
   * Strack trace of allocation as saved in the tracking map.
   */
  public static final class ByteBufferAllocationStacktraceException
      extends LeakDetectorHeapByteBufferPoolException {

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

    private ByteBufferAllocationStacktraceException(boolean unused) {
      super(
          "Log org.apache.hadoop.fs.impl.TrackingByteBufferPool at DEBUG for full stack traces",
          null,
          false,
          false);
    }
  }

  /**
   * Exception raised in {@link TrackingByteBufferPool#putBuffer(ByteBuffer)} if the
   * buffer to release was not in the hash map.
   */
  public static final class ReleasingUnallocatedByteBufferException extends LeakDetectorHeapByteBufferPoolException {

    private ReleasingUnallocatedByteBufferException() {
      super("Releasing a ByteBuffer instance that is not allocated by this buffer pool or already been released");
    }
  }

  /**
   * Exception raised in {@link TrackingByteBufferPool#close()} if there was an unreleased buffer.
   */
  public static class LeakedByteBufferException extends LeakDetectorHeapByteBufferPoolException {

    private LeakedByteBufferException(int count, ByteBufferAllocationStacktraceException e) {
      super(count + " ByteBuffer object(s) is/are remained unreleased after closing this buffer pool.", e);
    }
  }

  /**
   * Tracker of allocations.
   * <p>
   * The key maps by the object id of the buffer, and refers to either a common stack trace
   * or one dynamically created for each allocation.
   */
  private final Map<Key, ByteBufferAllocationStacktraceException> allocated = new HashMap<>();

  /**
   * Wrapped buffer pool.
   */
  private final ByteBufferPool allocator;

  /**
   * private constructor.
   * @param allocator pool allocator.
   */
  private TrackingByteBufferPool(ByteBufferPool allocator) {
    this.allocator = allocator;
  }

  @Override
  public ByteBuffer getBuffer(final boolean direct, final int size) {
    ByteBuffer buffer = allocator.getBuffer(direct, size);
    final ByteBufferAllocationStacktraceException ex = ByteBufferAllocationStacktraceException.create();
    final Key key = new Key(buffer);
    allocated.put(key, ex);
    LOG.debug("Creating ByteBuffer:{} size {} {}", key.hashCode(), size, buffer, ex);
    return buffer;
  }

  @Override
  public void putBuffer(ByteBuffer b) throws ReleasingUnallocatedByteBufferException {
    Objects.requireNonNull(b);
    final Key key = new Key(b);
    LOG.debug("Releasing ByteBuffer: {}: {}", key.hashCode(), b);
    if (allocated.remove(key) == null) {
      throw new ReleasingUnallocatedByteBufferException();
    }
    allocator.putBuffer(b);
    // Clearing the buffer so subsequent access would probably generate errors
    b.clear();
  }

  /**
   * Expect all buffers to be released -if not, log unreleased ones
   * and then raise an exception with the stack trace of the first
   * unreleased buffer.
   * @throws LeakedByteBufferException if at least one was unsued.
   */
  @Override
  public void close() throws LeakedByteBufferException {
    if (!allocated.isEmpty()) {
      allocated.keySet().forEach(key ->
          LOG.warn("Unreleased ByteBuffer {}; {}", key.hashCode(), key));
      LeakedByteBufferException ex = new LeakedByteBufferException(
          allocated.size(),
          allocated.values().iterator().next());
      allocated.clear(); // Drop the references to the ByteBuffers, so they can be gc'd
      throw ex;
    }
  }
}
