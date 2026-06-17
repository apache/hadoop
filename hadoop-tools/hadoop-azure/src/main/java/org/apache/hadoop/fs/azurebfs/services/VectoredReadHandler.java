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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;
import org.apache.hadoop.fs.azurebfs.enums.VectoredReadStrategy;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.impl.CombinedFileRange;

/**
 * Handles vectored read operations by coordinating with a ReadBufferManager
 * and applying the configured VectoredReadStrategy.
 * This class acts as the orchestration layer that decides how vectored reads
 * are executed, while delegating buffer management and read behavior to
 * dedicated components.
 */
class VectoredReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(VectoredReadHandler.class);

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
   * Shared reconstruction buffers for ranges that span multiple chunks.
   * Keyed by the original FileRange instance.
   */
  private final ConcurrentHashMap<RangeKey, ByteBuffer> partialBuffers =
      new ConcurrentHashMap<>();

  /**
   * Tracks remaining bytes to be received for each logical range.
   * Keyed by the original FileRange instance.
   */
  private final ConcurrentHashMap<RangeKey, AtomicInteger> pendingBytes =
      new ConcurrentHashMap<>();

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
    LOG.debug("VectoredReadHandler initialized with strategy={}", strategy);
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
      IntFunction<ByteBuffer> allocator) throws EOFException {
    LOG.debug("readVectored invoked: path={}, rangeCount={}",
        stream.getPath(), ranges.size());

    /* Initialize a future for each logical file range */
    long fileLength = stream.getContentLength();
    List<FileRange> validRanges = validateAndPrepareRanges(ranges, fileLength);

    /* Select the maximum allowed merge span based on the configured strategy */
    int maxSpan =
        (strategy == VectoredReadStrategy.TPS_OPTIMIZED)
            ? readBufferManager.getMaxReadSizeForVectoredReads()
            : readBufferManager.getMaxReadSizeForeVectoredReadsThroughput();

    LOG.debug("readVectored: path={}, strategy={}, maxSpan={}",
        stream.getPath(), strategy, maxSpan);

    /* Merge logical ranges using a span-first coalescing strategy */
    List<CombinedFileRange> merged = mergeBySpanAndGap(validRanges, maxSpan, fileLength);

    LOG.debug("readVectored: path={}, mergedRangeCount={}",
        stream.getPath(), merged.size());

    /* Read buffer size acts as a hard upper bound for physical reads */
    int readBufferSize = ReadBufferManager.getReadAheadBlockSize();

    /* Split merged ranges into buffer-sized chunks and queue each for read */
    for (CombinedFileRange unit : merged) {
      List<CombinedFileRange> chunks = splitByBufferSize(unit, readBufferSize);

      LOG.debug("readVectored: path={}, mergedOffset={}, mergedLength={}, chunkCount={}",
          stream.getPath(), unit.getOffset(), unit.getLength(), chunks.size());

      for (CombinedFileRange chunk : chunks) {
        try {
          VectoredReadUtils.validateRangeRequest(chunk);
          boolean queued = queueVectoredRead(stream, chunk, allocator);
          if (!queued) {
            LOG.debug("readVectored: buffer pool exhausted, falling back to directRead: path={}, offset={}, length={}",
                stream.getPath(), chunk.getOffset(), chunk.getLength());
            directRead(stream, chunk, allocator);
          }
        } catch (Exception e) {
          LOG.warn("readVectored: chunk read failed, failing underlying ranges:"
                  + " path={}, offset={}, length={}",
              stream.getPath(), chunk.getOffset(), chunk.getLength(), e);
          failUnit(chunk, e);
        }
      }
    }
  }

  /**
   * Queues a vectored read request with the buffer manager.
   *
   * @return true if successfully queued, false if the queue is full and fallback is required.
   */
  @VisibleForTesting
  boolean queueVectoredRead(AbfsInputStream stream, CombinedFileRange unit,
      IntFunction<ByteBuffer> allocator) {
    LOG.debug("queueVectoredRead: path={}, offset={}, length={}",
        stream.getPath(), unit.getOffset(), unit.getLength());
    TracingContext tracingContext = stream.getTracingContext();
    tracingContext.setReadType(ReadType.VECTORED_READ);
    return getReadBufferManager().queueVectoredRead(stream, unit, tracingContext, allocator);
  }

  /**
   * Accesses the shared manager responsible for coordinating asynchronous read buffers.
   *
   * @return the {@link ReadBufferManager} instance.
   */
  public ReadBufferManager getReadBufferManager() {
    return readBufferManager;
  }

  /**
   * Validates and prepares a list of {@link FileRange} instances for vectored reads.
   *
   * <p>This method performs the following steps for each input range:
   * <ul>
   *   <li>Validates the range using {@link VectoredReadUtils#validateRangeRequest(FileRange)}</li>
   *   <li>Initializes a {@link CompletableFuture} to hold the read result</li>
   *   <li>Checks that the range falls within the bounds of the file</li>
   * </ul>
   *
   * <p>Ranges that are invalid (e.g., negative offset/length or exceeding file bounds)
   * are not included in the returned list. Instead, their associated future is
   * completed exceptionally with an {@link EOFException}.
   *
   * @param ranges     the input list of logical file ranges to validate and prepare
   * @param fileLength the total length of the file, used for bounds checking
   * @return a list of valid {@link FileRange} instances ready for vectored read processing
   */
  private List<FileRange> validateAndPrepareRanges(
      List<? extends FileRange> ranges, long fileLength) throws EOFException {

    List<FileRange> validRanges = new ArrayList<>();

    for (FileRange r : ranges) {
      VectoredReadUtils.validateRangeRequest(r);
      r.setData(new CompletableFuture<>());

      long offset = r.getOffset();
      long length = r.getLength();

      if (offset < 0 || length < 0 || offset > fileLength || length > fileLength - offset) {
        r.getData().completeExceptionally(new EOFException(
            "Invalid range: offset=" + offset + ", length=" + length + ", fileLength=" + fileLength));
        continue;
      }
      validRanges.add(r);
    }
    return validRanges;
  }

  /**
   * Splits a merged logical {@link CombinedFileRange} into smaller
   * buffer-sized physical read units.
   *
   * <p>Each resulting unit will have a maximum size equal to the provided
   * {@code bufferSize}. Any handling of multi-chunk ranges or reassembly
   * of underlying {@link FileRange} data is delegated to the fan-out logic.</p>
   *
   * @param unit       the combined logical range to be split
   * @param bufferSize the maximum size (in bytes) of each physical read unit
   * @return a list of buffer-sized {@link CombinedFileRange} instances
   */
  private List<CombinedFileRange> splitByBufferSize(
      CombinedFileRange unit,
      int bufferSize) {
    LOG.debug("splitByBufferSize: offset={}, length={}, bufferSize={}",
        unit.getOffset(), unit.getLength(), bufferSize);

    List<CombinedFileRange> parts = new ArrayList<>();
    long unitStart = unit.getOffset();
    long unitEnd = unitStart + unit.getLength();
    long start = unitStart;

    while (start < unitEnd) {
      long partEnd = Math.min(start + bufferSize, unitEnd);

      CombinedFileRange part =
          new CombinedFileRange(start, partEnd, unit.getUnderlying().get(0));
      part.getUnderlying().clear();

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

    LOG.debug("splitByBufferSize: offset={}, produced {} parts",
        unit.getOffset(), parts.size());
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
      int maxSpan, long  fileLength) throws EOFException {

    LOG.debug("mergeBySpanAndGap: rangeCount={}, maxSpan={}", ranges.size(), maxSpan);
    List<? extends FileRange> sortedRanges = VectoredReadUtils.validateAndSortRanges(
        ranges, Optional.of(fileLength));

    List<CombinedFileRange> out = new ArrayList<>();
    CombinedFileRange current = null;

    for (FileRange r : sortedRanges) {
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

    LOG.debug("mergeBySpanAndGap: produced {} combined ranges", out.size());
    return out;
  }

  /**
   * Distributes data from a physical read buffer into the corresponding
   * logical {@link FileRange}s.
   *
   * <p>This method performs a "fan-out" operation where a single physical
   * read (represented by {@link ReadBuffer}) may contain data for multiple
   * logical ranges. The relevant portions are copied into per-range buffers
   * and completed once fully populated.</p>
   *
   * <p>Partial reads are accumulated using {@code partialBuffers} and
   * {@code pendingBytes}. A range is only completed when all expected
   * bytes have been received.</p>
   *
   * <p>Thread safety:
   * <ul>
   *   <li>Each logical range buffer is synchronized independently</li>
   *   <li>Writes use {@code System.arraycopy} directly into the backing array
   *       to avoid shared {@link ByteBuffer} position mutation</li>
   * </ul>
   * </p>
   *
   * @param buffer    the physical read buffer containing merged data
   * @param bytesRead number of valid bytes in the buffer
   */
  void fanOut(ReadBuffer buffer, int bytesRead) {
    LOG.debug("fanOut: path={}, bufferOffset={}, bytesRead={}",
        buffer.getPath(), buffer.getOffset(), bytesRead);

    List<CombinedFileRange> units = buffer.getVectoredUnits();
    if (units == null) {
      LOG.warn("fanOut: no vectored units found for path={}, offset={}",
          buffer.getPath(), buffer.getOffset());
      return;
    }

    long bufferStart = buffer.getOffset();
    long bufferEnd = bufferStart + bytesRead;

    /* Iterate over all combined logical units mapped to this buffer */
    for (CombinedFileRange unit : units) {
      /* Each unit may contain multiple logical FileRanges */
      for (FileRange r : unit.getUnderlying()) {
        CompletableFuture<ByteBuffer> future = r.getData();
        RangeKey key = new RangeKey(r);

        /* Skip already completed or cancelled ranges */
        if (future.isCancelled()) {
          LOG.debug("fanOut: range cancelled, cleaning up: path={}, rangeOffset={}",
              buffer.getPath(), r.getOffset());
          partialBuffers.remove(key);
          pendingBytes.remove(key);
          continue;
        }
        if (future.isDone()) {
          continue;
        }

        try {
          long rangeStart = r.getOffset();
          long rangeEnd = rangeStart + r.getLength();

          /* Compute overlap between buffer and logical range */
          long overlapStart = Math.max(rangeStart, bufferStart);
          long overlapEnd = Math.min(rangeEnd, bufferEnd);

          /* No overlap nothing to copy */
          if (overlapStart >= overlapEnd) {
            LOG.debug("fanOut: no overlap for path={}, rangeOffset={}, bufferOffset={}",
                buffer.getPath(), r.getOffset(), bufferStart);
            continue;
          }

          int srcOffset = (int) (overlapStart - bufferStart);
          int destOffset = (int) (overlapStart - rangeStart);
          int length = (int) (overlapEnd - overlapStart);

          LOG.debug("fanOut: copying path={}, rangeOffset={}, rangeLength={},"
                  + " bufferOffset={}, srcOffset={}, destOffset={}, length={}",
              buffer.getPath(), r.getOffset(), r.getLength(),
              bufferStart, srcOffset, destOffset, length);

          /* Allocate or reuse the full buffer for this logical range */
          ByteBuffer fullBuf = partialBuffers.computeIfAbsent(
              key, k -> buffer.getAllocator().apply(r.getLength()));

          /* Track remaining bytes required to complete this range */
          AtomicInteger pending = pendingBytes.computeIfAbsent(
              key, k -> new AtomicInteger(r.getLength()));

          synchronized (fullBuf) {
            /* Double-check completion inside lock */
            if (future.isDone()) {
              continue;
            }

            ByteBuffer dst = fullBuf.duplicate();
            dst.position(destOffset);
            dst.put(buffer.getBuffer(), srcOffset, length);

            int left = pending.addAndGet(-length);

            LOG.debug("fanOut: wrote chunk: path={}, rangeOffset={}, destOffset={},"
                    + " length={}, pendingBytes={}",
                buffer.getPath(), r.getOffset(), destOffset, length, left);

            if (left < 0) {
              LOG.error("fanOut: pending bytes went negative possible duplicate write:"
                      + " path={}, rangeOffset={}, pending={}",
                  buffer.getPath(), r.getOffset(), left);
              future.completeExceptionally(new IllegalStateException(
                  "Pending bytes negative for offset=" + r.getOffset()));
              partialBuffers.remove(key);
              pendingBytes.remove(key);
              continue;
            }

            /* Complete future once all bytes are received */
            if (left == 0 && !future.isDone()) {
              /*
               * Prepare buffer for reading.
               * DO NOT use flip() because writes may arrive out-of-order.
               * Instead, explicitly expose the full buffer.
               */
              fullBuf.position(0);
              fullBuf.limit(fullBuf.capacity());

              if (fullBuf.limit() != r.getLength()) {
                LOG.warn("fanOut: buffer size mismatch: path={}, rangeOffset={},"
                        + " expected={}, actual={}",
                    buffer.getPath(), r.getOffset(), r.getLength(), fullBuf.limit());
              }

              future.complete(fullBuf);
              partialBuffers.remove(key);
              pendingBytes.remove(key);

              LOG.debug("fanOut: completed range: path={}, rangeOffset={}, rangeLength={}",
                  buffer.getPath(), r.getOffset(), r.getLength());
            }
          }
        } catch (Exception e) {
          LOG.warn("fanOut: exception processing range: path={}, rangeOffset={}",
              buffer.getPath(), r.getOffset(), e);
          partialBuffers.remove(key);
          pendingBytes.remove(key);
          if (!future.isDone()) {
            future.completeExceptionally(e);
          }
        }
      }
    }
  }

  /**
   * Fails all logical {@link FileRange}s associated with a given
   * {@link CombinedFileRange}.
   *
   * <p>This method is invoked when a vectored read for the combined unit
   * fails. It ensures that:
   * <ul>
   *   <li>Any partially accumulated buffers are cleaned up</li>
   *   <li>Pending byte tracking state is removed</li>
   *   <li>All corresponding {@link CompletableFuture}s are completed exceptionally</li>
   * </ul>
   * </p>
   *
   * @param unit the combined vectored read unit whose underlying ranges must be failed
   * @param t    the exception that caused the failure
   */
  private void failUnit(CombinedFileRange unit, Throwable t) {
    for (FileRange r : unit.getUnderlying()) {
      RangeKey key = new RangeKey(r);
      partialBuffers.remove(key);
      pendingBytes.remove(key);
      CompletableFuture<ByteBuffer> future = r.getData();
      if (future != null && !future.isDone()) {
        future.completeExceptionally(t);
      }
    }
  }

  /**
   * Fails all {@link FileRange} futures associated with the given
   * {@link ReadBuffer} and clears any partial state.
   *
   * @param buffer the read buffer whose ranges should be failed
   * @param t      the exception causing the failure
   */
  void failBufferFutures(ReadBuffer buffer, Throwable t) {
    List<CombinedFileRange> units = buffer.getVectoredUnits();
    if (units == null) {
      LOG.warn("fanOut: no vectored units found for path={}, offset={}",
          buffer.getPath(), buffer.getOffset());
      return;
    }

    for (CombinedFileRange unit : units) {
      for (FileRange r : unit.getUnderlying()) {
        RangeKey key = new RangeKey(r);
        partialBuffers.remove(key);
        pendingBytes.remove(key);
        CompletableFuture<ByteBuffer> future = r.getData();
        if (future != null && !future.isDone()) {
          future.completeExceptionally(t);
        }
      }
    }
  }

  /**
   * Performs a synchronous direct read for a {@link CombinedFileRange}
   * when pooled buffering is not available.
   *
   * <p>Data is accumulated into per-range partial buffers shared with
   * {@link #fanOut}, ensuring that ranges spanning multiple chunks are
   * correctly reassembled regardless of whether individual chunks were
   * served via the async queue or this direct fallback path.</p>
   *
   * @param stream    input stream to read from
   * @param unit      combined range to read
   * @param allocator buffer allocator for logical ranges
   * @throws IOException if the read fails
   */
  void directRead(
      AbfsInputStream stream,
      CombinedFileRange unit,
      IntFunction<ByteBuffer> allocator) throws IOException {

    LOG.debug("directRead: path={}, offset={}, length={}",
        stream.getPath(), unit.getOffset(), unit.getLength());

    /* Read entire combined range into a temporary buffer */
    byte[] tmp = new byte[unit.getLength()];
    TracingContext tracingContext = new TracingContext(stream.getTracingContext());
    tracingContext.setReadType(ReadType.VECTORED_DIRECT_READ);

    int total = 0;
    int requested = unit.getLength();
    while (total < requested) {
      int n = stream.readRemote(unit.getOffset() + total, tmp, total,
          requested - total, tracingContext);
      if (n <= 0) {
        throw new IOException(
            "Unexpected end of stream during direct read: path=" + stream.getPath()
                + ", offset=" + (unit.getOffset() + total)
                + ", requested=" + requested);
      }
      total += n;
    }

    LOG.debug("directRead: read complete: path={}, offset={}, bytesRead={}",
        stream.getPath(), unit.getOffset(), total);

    long unitStart = unit.getOffset();
    long unitEnd = unitStart + unit.getLength();

    /* Distribute data to each logical FileRange */
    for (FileRange r : unit.getUnderlying()) {
      CompletableFuture<ByteBuffer> future = r.getData();
      if (future == null || future.isDone()) {
        continue;
      }

      long rangeStart = r.getOffset();
      long rangeEnd = rangeStart + r.getLength();

      /* Compute overlap between unit and logical range */
      long overlapStart = Math.max(rangeStart, unitStart);
      long overlapEnd = Math.min(rangeEnd, unitEnd);
      if (overlapStart >= overlapEnd) {
        continue;
      }

      int srcOffset = (int) (overlapStart - unitStart);
      int destOffset = (int) (overlapStart - rangeStart);
      int length = (int) (overlapEnd - overlapStart);

      LOG.debug("directRead: copying: path={}, rangeOffset={}, rangeLength={},"
              + " srcOffset={}, destOffset={}, length={}",
          stream.getPath(), r.getOffset(), r.getLength(),
          srcOffset, destOffset, length);

      RangeKey key = new RangeKey(r);

      /*
       * Use the shared partialBuffers/pendingBytes maps so that ranges
       * spanning multiple chunks are correctly reassembled even when some
       * chunks are served via directRead and others via the async fanOut path.
       */
      ByteBuffer fullBuf = partialBuffers.computeIfAbsent(
          key, k -> allocator.apply(r.getLength()));
      AtomicInteger pending = pendingBytes.computeIfAbsent(
          key, k -> new AtomicInteger(r.getLength()));

      synchronized (fullBuf) {
        /* Re-check inside lock in case another chunk already completed this range */
        if (future.isDone()) {
          continue;
        }

        if (fullBuf.hasArray()) {
          System.arraycopy(tmp, srcOffset,
              fullBuf.array(), fullBuf.arrayOffset() + destOffset,
              length);
        } else {
          ByteBuffer dst = fullBuf.duplicate();
          dst.position(destOffset);
          dst.put(tmp, srcOffset, length);
        }

        int left = pending.addAndGet(-length);

        LOG.debug("directRead: wrote chunk: path={}, rangeOffset={}, destOffset={},"
                + " length={}, pendingBytes={}",
            stream.getPath(), r.getOffset(), destOffset, length, left);

        if (left < 0) {
          LOG.error("directRead: pending bytes went negative  possible duplicate write:"
                  + " path={}, rangeOffset={}, pending={}",
              stream.getPath(), r.getOffset(), left);
          future.completeExceptionally(new IllegalStateException(
              "Pending bytes negative in directRead for offset=" + r.getOffset()));
          partialBuffers.remove(key);
          pendingBytes.remove(key);
          continue;
        }

        if (left == 0) {
          fullBuf.position(0);
          fullBuf.limit(r.getLength());
          future.complete(fullBuf);
          partialBuffers.remove(key);
          pendingBytes.remove(key);
          LOG.debug("directRead: completed range: path={}, rangeOffset={}, rangeLength={}",
              stream.getPath(), r.getOffset(), r.getLength());
        }
      }
    }
  }

  /**
   * Identity-based key wrapper for {@link FileRange}.
   *
   * <p>This class ensures that {@link FileRange} instances are compared
   * using reference equality rather than logical equality. It is used as
   * a key in maps where multiple ranges may have identical offsets and
   * lengths but must be treated as distinct objects.</p>
   *
   * <p>Equality and hash code are based on object identity
   * ({@code ==}) via {@link System#identityHashCode(Object)}.</p>
   */
  static final class RangeKey {
    private final FileRange range;

    RangeKey(FileRange range) {
      this.range = range;
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof RangeKey && this.range == ((RangeKey) o).range);
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(range);
    }
  }
}
