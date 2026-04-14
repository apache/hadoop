/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.enums.BufferType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.impl.CombinedFileRange;

import static org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus.READ_FAILED;

public class ReadBuffer {

  private AbfsInputStream stream;
  private String eTag;
  private String path;                   // path of the file this buffer is for
  private long offset;                   // offset within the file for the buffer
  private int length;                    // actual length, set after the buffer is filles
  private int requestedLength;           // requested length of the read
  private byte[] buffer;                 // the buffer itself
  private int bufferindex = -1;          // index in the buffers array in Buffer manager
  private ReadBufferStatus status;             // status of the buffer
  private CountDownLatch latch = null;   // signaled when the buffer is done reading, so any client
  // waiting on this buffer gets unblocked
  private TracingContext tracingContext;

  // fields to help with eviction logic
  private long timeStamp = 0;  // tick at which buffer became available to read
  private boolean isFirstByteConsumed = false;
  private boolean isLastByteConsumed = false;
  private boolean isAnyByteConsumed = false;
  private AtomicInteger refCount = new AtomicInteger(0);
  private BufferType bufferType = BufferType.NORMAL;
  // list of combined file ranges for vectored read.
  private List<CombinedFileRange> vectoredUnits = new ArrayList<>();
  // Allocator used for vectored fan-out; captured at queue time */
  private IntFunction<ByteBuffer> allocator;
  // Tracks whether fanOut has already been executed
  private final AtomicBoolean fanOutDone = new AtomicBoolean(false);

  private IOException errException = null;

  public AbfsInputStream getStream() {
    return stream;
  }

  public String getETag() {
    return eTag;
  }

  public String getPath() {
    return path;
  }

  public void setStream(AbfsInputStream stream) {
    this.stream = stream;
  }

  public void setETag(String eTag) {
    this.eTag = eTag;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setTracingContext(TracingContext tracingContext) {
    this.tracingContext = tracingContext;
  }

  public TracingContext getTracingContext() {
    return tracingContext;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public int getRequestedLength() {
    return requestedLength;
  }

  public void setRequestedLength(int requestedLength) {
    this.requestedLength = requestedLength;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public void setBuffer(byte[] buffer) {
    this.buffer = buffer;
  }

  public int getBufferindex() {
    return bufferindex;
  }

  public void setBufferindex(int bufferindex) {
    this.bufferindex = bufferindex;
  }

  public IOException getErrException() {
    return errException;
  }

  public void setErrException(final IOException errException) {
    this.errException = errException;
  }

  public ReadBufferStatus getStatus() {
    return status;
  }

  public void setStatus(ReadBufferStatus status) {
    this.status = status;
    if (status == READ_FAILED) {
      bufferindex = -1;
    }
  }

  public void startReading() {
    refCount.getAndIncrement();
  }

  public void endReading() {
    if (refCount.decrementAndGet() < 0) {
      throw new IllegalStateException("ReadBuffer refCount cannot be negative");
    }
  }

  public int getRefCount() {
    return refCount.get();
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public boolean isFirstByteConsumed() {
    return isFirstByteConsumed;
  }

  public void setFirstByteConsumed(boolean isFirstByteConsumed) {
    this.isFirstByteConsumed = isFirstByteConsumed;
  }

  public boolean isLastByteConsumed() {
    return isLastByteConsumed;
  }

  public void setLastByteConsumed(boolean isLastByteConsumed) {
    this.isLastByteConsumed = isLastByteConsumed;
  }

  public boolean isAnyByteConsumed() {
    return isAnyByteConsumed;
  }

  public void setAnyByteConsumed(boolean isAnyByteConsumed) {
    this.isAnyByteConsumed = isAnyByteConsumed;
  }

  public boolean isFullyConsumed() {
    return isFirstByteConsumed() && isLastByteConsumed();
  }

  /**
   * Add a logical vectored read unit associated with this buffer.
   *
   * <p>Each {@link CombinedFileRange} represents a logical range requested
   * by the caller that maps to this buffer-sized physical read. Multiple
   * logical units may be attached to a single buffer when their ranges
   * fall within the same physical read span.</p>
   *
   * @param u logical vectored read unit to associate with this buffer
   */
  void addVectoredUnit(CombinedFileRange u) {
    vectoredUnits.add(u);
  }

  /**
   * Get all logical vectored read units attached to this buffer.
   *
   * <p>The returned list contains the {@link CombinedFileRange}s whose
   * requested data resides within the physical data stored in this buffer.
   * These units will receive slices of the buffer during vectored fan-out.</p>
   *
   * @return list of logical vectored units mapped to this buffer
   */
  List<CombinedFileRange> getVectoredUnits() {
    return vectoredUnits;
  }

  /**
   * Clear all vectored units associated with this buffer.
   *
   * <p>This method removes all logical read units that were previously
   * attached to this buffer. It is typically invoked when the buffer
   * is being reset or returned to the buffer pool for reuse.</p>
   */
  void clearVectoredUnits() {
    if (vectoredUnits != null) {
      vectoredUnits.clear();
    }
  }

  /**
   * Return the type of this buffer.
   *
   * <p>The buffer type indicates how the buffer is used during the
   * read pipeline (for example, normal read buffers versus vectored
   * read buffers).</p>
   *
   * @return the {@link BufferType} of this buffer
   */
  public BufferType getBufferType() {
    return bufferType;
  }

  /**
   * Set the type of this buffer.
   *
   * <p>The buffer type determines the role of the buffer within the
   * read pipeline, such as whether it is used for normal reads or
   * vectored read operations.</p>
   *
   * @param bufferType buffer type to assign
   */
  public void setBufferType(final BufferType bufferType) {
    this.bufferType = bufferType;
  }

  /**
   * Set the allocator associated with this buffer.
   *
   * <p>The same allocator instance may be shared across multiple buffers
   * belonging to a single vectored read operation. It is captured at
   * queue time so it is available when the asynchronous read completes.</p>
   *
   * @param allocator allocator used for vectored fan-out
   */
  public void setAllocator(IntFunction<ByteBuffer> allocator) {
    this.allocator = allocator;
  }

  /**
   * Return the allocator associated with this buffer.
   *
   * @return allocator used for vectored fan-out, or {@code null} for
   *         non-vectored buffers
   */
  public IntFunction<ByteBuffer> getAllocator() {
    return allocator;
  }

  /**
   * Attempt to execute vectored fan-out exactly once for this buffer.
   *
   * @return {@code true} if the caller should perform fan-out; {@code false}
   *         if fan-out has already been executed
   */
  boolean tryFanOut() {
    return fanOutDone.compareAndSet(false, true);
  }

  /**
   * @return {@code true} if vectored fan-out has already been executed
   *         for this buffer; {@code false} otherwise
   */
  boolean isFanOutDone() {
    return fanOutDone.get();
  }
}
