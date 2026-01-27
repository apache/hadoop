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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.azurebfs.enums.VectoredReadStrategy;
import org.apache.hadoop.fs.impl.CombinedFileRange;

/**
 * Handles vectored read operations by coordinating with a ReadBufferManager
 * and applying the configured VectoredReadStrategy.
 * This class acts as the orchestration layer that decides how vectored reads
 * are executed, while delegating buffer management and read behavior to
 * dedicated components.
 */
class VectoredReadHandler {

  /**
   * Manages allocation, lifecycle, and reuse of read buffers
   * used during vectored read operations.
   */
  private final ReadBufferManager readBufferManager;

  /**
   * Strategy defining how vectored reads should be performed.
   */
  private final VectoredReadStrategy strategy;

  /**
   * Creates a VectoredReadHandler using the provided ReadBufferManager.
   * The vectored read strategy is obtained from the manager to ensure
   * consistent configuration across the read pipeline.
   *
   * @param readBufferManager manager responsible for buffer handling
   *                          and providing the vectored read strategy
   */
  VectoredReadHandler(ReadBufferManager readBufferManager) {
    this.readBufferManager = readBufferManager;
    this.strategy = readBufferManager.getVectoredReadStrategy();
  }

  /**
   * Perform a vectored read over multiple logical file ranges.
   *
   * <p>Logical ranges are first merged using a span-first strategy determined
   * by the configured {@link VectoredReadStrategy}. The merged ranges are then
   * split into buffer-sized physical read units and queued for asynchronous
   * execution. If a pooled buffer is unavailable, the read falls back to a
   * direct read path.</p>
   *
   * @param stream    input stream for the file being read
   * @param ranges    logical file ranges to read; each range will be completed
   *                  with data or failure via its associated future
   * @param allocator allocator used to create buffers for direct reads and
   *                  vectored fan-out
   */
  public void readVectored(
      AbfsInputStream stream,
      List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocator) {

    /* Initialize a future for each logical file range */
    for (FileRange r : ranges) {
      r.setData(new CompletableFuture<>());
    }

    /* Select the maximum allowed merge span based on the configured strategy */
    int maxSpan =
        (strategy == VectoredReadStrategy.TPS_OPTIMIZED)
            ? readBufferManager.getMaxSeekForVectoredReads()
            : readBufferManager.getMaxSeekForVectoredReadsThroughput();

    /* Merge logical ranges using a span-first coalescing strategy */
    List<CombinedFileRange> merged =
        mergeBySpanAndGap(ranges, maxSpan);

    /* Read buffer size acts as a hard upper bound for physical reads */
    int readBufferSize = ReadBufferManager.getReadAheadBlockSize();

    /* Split merged ranges into buffer-sized chunks and queue each for read */
    for (CombinedFileRange unit : merged) {
      List<CombinedFileRange> chunks =
          splitByBufferSize(unit, readBufferSize);

      for (CombinedFileRange chunk : chunks) {
        try {
          boolean queued = queueVectoredRead(stream, chunk, allocator);
          if (!queued) {
            /* Fall back to direct read if no buffer is available */
            directRead(stream, chunk, allocator);
          }
        } catch (Exception e) {
          /* Propagate failure to all logical ranges in this unit */
          failUnit(chunk, e);
        }
      }
    }
  }

  /**
   * Queues a vectored read request with the buffer manager.
   * @return true if successfully queued, false if the queue is full and fallback is required.
   */
  @VisibleForTesting
  boolean queueVectoredRead(AbfsInputStream stream, CombinedFileRange unit, IntFunction<ByteBuffer> allocator) {
    return getReadBufferManager().queueVectoredRead(stream, unit, stream.getTracingContext(), allocator);
  }

  /**
   * Accesses the shared manager responsible for coordinating asynchronous read buffers.
   * @return the {@link ReadBufferManager} instance.
   */
  public ReadBufferManager getReadBufferManager() {
    return readBufferManager;
  }

  /**
   * Split a merged logical range into buffer-sized physical read units.
   *
   * <p>The input {@link CombinedFileRange} may span more bytes than the
   * configured read buffer size. This method divides it into multiple
   * {@link CombinedFileRange} instances, each limited to {@code bufferSize}
   * and containing only the logical {@link FileRange}s that intersect its span.</p>
   *
   * @param unit       merged logical range to be split
   * @param bufferSize maximum size (in bytes) of each physical read unit
   * @return a list of buffer-sized {@link CombinedFileRange} instances
   */
  private List<CombinedFileRange> splitByBufferSize(
      CombinedFileRange unit,
      int bufferSize) {

    List<CombinedFileRange> parts = new ArrayList<>();

    long unitStart = unit.getOffset();
    long unitEnd = unitStart + unit.getLength();
    long start = unitStart;

    /* Create buffer-sized slices covering the merged unit span */
    while (start < unitEnd) {
      long partEnd = Math.min(start + bufferSize, unitEnd);

      /* Initialize a physical read unit for the span [start, partEnd) */
      CombinedFileRange part =
          new CombinedFileRange(start, partEnd,
              unit.getUnderlying().get(0));

      /* Remove the constructor-added range and attach only overlapping ranges */
      part.getUnderlying().clear();

      /* Attach logical ranges that intersect this physical read unit */
      for (FileRange r : unit.getUnderlying()) {
        long rStart = r.getOffset();
        long rEnd = rStart + r.getLength();

        if (rEnd > start && rStart < partEnd) {
          part.getUnderlying().add(r);
        }
      }

      parts.add(part);
      start = partEnd;
    }

    return parts;
  }

   /**
   * Merge logical {@link FileRange}s into {@link CombinedFileRange}s using a
   * span-first coalescing strategy.
   *
   * <p>Ranges are merged as long as the total span from the first offset to the
   * end of the last range does not exceed {@code maxSpan}. Gaps between ranges
   * are ignored.</p>
   *
   * @param ranges  logical file ranges to merge
   * @param maxSpan maximum allowed span (in bytes) for a combined read
   * @return merged {@link CombinedFileRange}s covering the input ranges
   */
  private List<CombinedFileRange> mergeBySpanAndGap(
      List<? extends FileRange> ranges,
      int maxSpan) {

    /* Sort ranges by starting offset for span-based merging */
    ranges.sort(Comparator.comparingLong(FileRange::getOffset));

    List<CombinedFileRange> out = new ArrayList<>();
    CombinedFileRange current = null;

    for (FileRange r : ranges) {
      long rOffset = r.getOffset();
      long rEnd = rOffset + r.getLength();

      /* Initialize the first combined range */
      if (current == null) {
        current = new CombinedFileRange(rOffset, rEnd, r);
        continue;
      }

      /* Check whether adding this range keeps the total span within the limit */
      long newSpan = rEnd - current.getOffset();

      if (newSpan <= maxSpan) {
        current.setLength((int) newSpan);
        current.getUnderlying().add(r);
      } else {
        /* Span exceeded; finalize current range and start a new one */
        out.add(current);
        current = new CombinedFileRange(rOffset, rEnd, r);
      }
    }

    /* Add the final combined range, if any */
    if (current != null) {
      out.add(current);
    }

    return out;
  }


  /**
   * Fan out data from a completed physical read buffer to all logical
   * {@link FileRange}s associated with the vectored read.
   *
   * <p>For each logical range, the corresponding slice of data is copied
   * into a newly allocated {@link ByteBuffer} and the range's future is
   * completed. Ranges whose futures are cancelled are skipped.</p>
   *
   * @param buffer completed read buffer containing the physical data
   * @param bytesRead number of bytes actually read into the buffer
   */
  void fanOut(ReadBuffer buffer, int bytesRead) {
    List<CombinedFileRange> units = buffer.getVectoredUnits();
    if (units == null) {
      return;
    }
    /* Distribute buffer data to all logical ranges attached to this buffer */
    for (CombinedFileRange unit : units) {
      for (FileRange r : unit.getUnderlying()) {
        /* Skip ranges whose futures have been cancelled */
        if (r.getData().isCancelled()) {
          continue;
        }
        try {
          /* Compute offset of the logical range relative to the buffer */
          long rel = r.getOffset() - buffer.getOffset();
          /* Determine how many bytes are available for this range */
          int available =
              (int) Math.max(
                  0,
                  Math.min(r.getLength(), bytesRead - rel));
          /* Allocate output buffer and copy available data */
          ByteBuffer bb = buffer.getAllocator().apply(r.getLength());
          if (available > 0) {
            bb.put(buffer.getBuffer(), (int) rel, available);
          }
          bb.flip();
          r.getData().complete(bb);
        } catch (Exception e) {
          /* Propagate failure to the affected logical range */
          r.getData().completeExceptionally(e);
        }
      }
    }
  }

  /**
   * Fail all logical {@link FileRange}s associated with a single combined
   * vectored read unit.
   *
   * @param unit combined file range whose logical ranges should be failed
   * @param t    failure cause to propagate to waiting futures
   */
  private void failUnit(CombinedFileRange unit, Throwable t) {
    for (FileRange r : unit.getUnderlying()) {
      r.getData().completeExceptionally(t);
    }
  }


  /**
   * Completes all logical {@link FileRange} futures associated with a vectored
   * {@link ReadBuffer} exceptionally when the backend read fails.
   *
   * @param buffer the vectored read buffer
   * @param t      the failure cause to propagate to waiting futures
   */
  void failBufferFutures(ReadBuffer buffer, Throwable t) {
    List<CombinedFileRange> units = buffer.getVectoredUnits();
    if (units == null) {
      return;
    }

    /* Propagate failure to all logical ranges attached to this buffer */
    for (CombinedFileRange unit : units) {
      for (FileRange r : unit.getUnderlying()) {
        CompletableFuture<ByteBuffer> future = r.getData();
        if (future != null && !future.isDone()) {
          future.completeExceptionally(t);
        }
      }
    }
  }

  /**
   * Perform a synchronous direct read for a vectored unit when no pooled
   * read buffer is available.
   *
   * <p>This method reads the required byte range directly from the backend
   * and completes all associated logical {@link FileRange} futures. It is
   * used as a fallback path when vectored buffering cannot be used.</p>
   *
   * @param stream    input stream for the file being read
   * @param unit      combined file range to read directly
   * @param allocator allocator used to create output buffers for logical ranges
   * @throws IOException if memory pressure is high or the backend read fails
   */
  void directRead(
      AbfsInputStream stream,
      CombinedFileRange unit,
      IntFunction<ByteBuffer> allocator) throws IOException {
    /* Read the entire combined range into a temporary buffer */
    byte[] tmp = new byte[unit.getLength()];
    stream.readRemote(unit.getOffset(), tmp, 0, unit.getLength(),
        stream.getTracingContext());

    /* Fan out data to individual logical ranges */
    for (FileRange r : unit.getUnderlying()) {
      ByteBuffer bb = allocator.apply(r.getLength());
      bb.put(tmp,
          (int) (r.getOffset() - unit.getOffset()),
          r.getLength());
      bb.flip();
      r.getData().complete(bb);
    }
  }
}

