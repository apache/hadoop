/*
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

package org.apache.hadoop.fs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.util.functional.Function4RaisingIOE;
import org.apache.hadoop.util.functional.FutureIO;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Utility class which implements helper methods used
 * in vectored IO implementation.
 */
@InterfaceAudience.LimitedPrivate("Filesystems")
@InterfaceStability.Unstable
public final class VectoredReadUtils {

  private static final int TMP_BUFFER_MAX_SIZE = 64 * 1024;

  private static final Logger LOG =
        LoggerFactory.getLogger(VectoredReadUtils.class);

  /**
   * Validate a single range.
   * @param range range to validate.
   * @return the range.
   * @param <T> range type
   * @throws IllegalArgumentException the range length is negative or other invalid condition
   * is met other than the those which raise EOFException or NullPointerException.
   * @throws EOFException the range offset is negative
   * @throws NullPointerException if the range is null.
   */
  public static <T extends FileRange> T validateRangeRequest(T range)
          throws EOFException {

    requireNonNull(range, "range is null");

    checkArgument(range.getLength() >= 0, "length is negative in %s", range);
    if (range.getOffset() < 0) {
      throw new EOFException("position is negative in range " + range);
    }
    return range;
  }

  /**
   * Validate a list of vectored read ranges.
   * @param ranges list of ranges.
   * @throws EOFException any EOF exception.
   */
  public static void validateVectoredReadRanges(List<? extends FileRange> ranges)
          throws EOFException {
    validateAndSortRanges(ranges, Optional.empty());
  }

  /**
   * This is the default implementation which iterates through the ranges
   * to read each synchronously, but the intent is that subclasses
   * can make more efficient readers.
   * The data or exceptions are pushed into {@link FileRange#getData()}.
   * @param stream the stream to read the data from
   * @param ranges the byte ranges to read
   * @param allocate the byte buffer allocation
   * @throws IllegalArgumentException if there are overlapping ranges or a range is invalid
   * @throws EOFException the range offset is negative
   */
  public static void readVectored(PositionedReadable stream,
                                  List<? extends FileRange> ranges,
                                  IntFunction<ByteBuffer> allocate) throws EOFException {
    for (FileRange range: validateAndSortRanges(ranges, Optional.empty())) {
      range.setData(readRangeFrom(stream, range, allocate));
    }
  }

  /**
   * Synchronously reads a range from the stream dealing with the combinations
   * of ByteBuffers buffers and PositionedReadable streams.
   * @param stream the stream to read from
   * @param range the range to read
   * @param allocate the function to allocate ByteBuffers
   * @return the CompletableFuture that contains the read data or an exception.
   * @throws IllegalArgumentException the range is invalid other than by offset or being null.
   * @throws EOFException the range offset is negative
   * @throws NullPointerException if the range is null.
   */
  public static CompletableFuture<ByteBuffer> readRangeFrom(
      PositionedReadable stream,
      FileRange range,
      IntFunction<ByteBuffer> allocate) throws EOFException {

    validateRangeRequest(range);
    CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
    try {
      ByteBuffer buffer = allocate.apply(range.getLength());
      if (stream instanceof ByteBufferPositionedReadable) {
        LOG.debug("ByteBufferPositionedReadable.readFully of {}", range);
        ((ByteBufferPositionedReadable) stream).readFully(range.getOffset(),
            buffer);
        buffer.flip();
      } else {
        // no positioned readable support; fall back to
        // PositionedReadable methods
        readNonByteBufferPositionedReadable(stream, range, buffer);
      }
      result.complete(buffer);
    } catch (IOException ioe) {
      LOG.debug("Failed to read {}", range, ioe);
      result.completeExceptionally(ioe);
    }
    return result;
  }

  /**
   * Read into a direct tor indirect buffer using {@code PositionedReadable.readFully()}.
   * @param stream stream
   * @param range file range
   * @param buffer destination buffer
   * @throws IOException IO problems.
   */
  private static void readNonByteBufferPositionedReadable(
      PositionedReadable stream,
      FileRange range,
      ByteBuffer buffer) throws IOException {
    if (buffer.isDirect()) {
      LOG.debug("Reading {} into a direct byte buffer from {}", range, stream);
      readInDirectBuffer(range,
          buffer,
          (position, buffer1, offset, length) -> {
            stream.readFully(position, buffer1, offset, length);
            return null;
          });
      buffer.flip();
    } else {
      // not a direct buffer, so read straight into the array
      LOG.debug("Reading {} into a byte buffer from {}", range, stream);
      stream.readFully(range.getOffset(), buffer.array(),
              buffer.arrayOffset(), range.getLength());
    }
  }

  /**
   * Read bytes from stream into a byte buffer using an
   * intermediate byte array.
   *   <pre>
   *     (position, buffer, buffer-offset, length): Void
   *     position:= the position within the file to read data.
   *     buffer := a buffer to read fully `length` bytes into.
   *     buffer-offset := the offset within the buffer to write data
   *     length := the number of bytes to read.
   *   </pre>
   * The passed in function MUST block until the required length of
   * data is read, or an exception is thrown.
   * @param range range to read
   * @param buffer buffer to fill.
   * @param operation operation to use for reading data.
   * @throws IOException any IOE.
   */
  public static void readInDirectBuffer(FileRange range,
      ByteBuffer buffer,
      Function4RaisingIOE<Long, byte[], Integer, Integer, Void> operation)
      throws IOException {

    LOG.debug("Reading {} into a direct buffer", range);
    validateRangeRequest(range);
    int length = range.getLength();
    if (length == 0) {
      // no-op
      return;
    }
    int readBytes = 0;
    long position = range.getOffset();
    int tmpBufferMaxSize = Math.min(TMP_BUFFER_MAX_SIZE, length);
    byte[] tmp = new byte[tmpBufferMaxSize];
    while (readBytes < length) {
      int currentLength = (readBytes + tmpBufferMaxSize) < length ?
              tmpBufferMaxSize
              : (length - readBytes);
      LOG.debug("Reading {} bytes from position {} (bytes read={}",
          currentLength, position, readBytes);
      operation.apply(position, tmp, 0, currentLength);
      buffer.put(tmp, 0, currentLength);
      position = position + currentLength;
      readBytes = readBytes + currentLength;
    }
  }

  /**
   * Is the given input list.
   * <ul>
   *   <li>already sorted by offset</li>
   *   <li>each range is more than minimumSeek apart</li>
   *   <li>the start and end of each range is a multiple of chunkSize</li>
   * </ul>
   *
   * @param input the list of input ranges.
   * @param chunkSize the size of the chunks that the offset and end must align to.
   * @param minimumSeek the minimum distance between ranges.
   * @return true if we can use the input list as is.
   */
  public static boolean isOrderedDisjoint(List<? extends FileRange> input,
                                          int chunkSize,
                                          int minimumSeek) {
    long previous = -minimumSeek;
    for (FileRange range: input) {
      long offset = range.getOffset();
      long end = range.getOffset() + range.getLength();
      if (offset % chunkSize != 0 ||
              end % chunkSize != 0 ||
              (offset - previous < minimumSeek)) {
        return false;
      }
      previous = end;
    }
    return true;
  }

  /**
   * Calculates floor value of offset based on chunk size.
   * @param offset file offset.
   * @param chunkSize file chunk size.
   * @return  floor value.
   */
  public static long roundDown(long offset, int chunkSize) {
    if (chunkSize > 1) {
      return offset - (offset % chunkSize);
    } else {
      return offset;
    }
  }

  /**
   * Calculates the ceiling value of offset based on chunk size.
   * @param offset file offset.
   * @param chunkSize file chunk size.
   * @return ceil value.
   */
  public static long roundUp(long offset, int chunkSize) {
    if (chunkSize > 1) {
      long next = offset + chunkSize - 1;
      return next - (next % chunkSize);
    } else {
      return offset;
    }
  }

  /**
   * Validate a list of ranges (including overlapping checks) and
   * return the sorted list.
   * <p>
   * Two ranges overlap when the start offset
   * of second is less than the end offset of first.
   * End offset is calculated as start offset + length.
   * @param input input list
   * @param fileLength file length if known
   * @return a new sorted list.
   * @throws IllegalArgumentException if there are overlapping ranges or
   * a range element is invalid (other than with negative offset)
   * @throws EOFException if the last range extends beyond the end of the file supplied
   *                          or a range offset is negative
   */
  public static List<? extends FileRange> validateAndSortRanges(
      final List<? extends FileRange> input,
      final Optional<Long> fileLength) throws EOFException {

    requireNonNull(input, "Null input list");

    if (input.isEmpty()) {
      // this may seem a pathological case, but it was valid
      // before and somehow Spark can call it through parquet.
      LOG.debug("Empty input list");
      return input;
    }

    final List<? extends FileRange> sortedRanges;

    if (input.size() == 1) {
      validateRangeRequest(input.get(0));
      sortedRanges = input;
    } else {
      sortedRanges = sortRangeList(input);
      FileRange prev = null;
      for (final FileRange current : sortedRanges) {
        validateRangeRequest(current);
        if (prev != null) {
          checkArgument(current.getOffset() >= prev.getOffset() + prev.getLength(),
              "Overlapping ranges %s and %s", prev, current);
        }
        prev = current;
      }
    }
    // at this point the final element in the list is the last range
    // so make sure it is not beyond the end of the file, if passed in.
    // where invalid is: starts at or after the end of the file
    if (fileLength.isPresent()) {
      final FileRange last = sortedRanges.get(sortedRanges.size() - 1);
      final Long l = fileLength.get();
      // this check is superfluous, but it allows for different exception message.
      if (last.getOffset() >= l) {
        throw new EOFException("Range starts beyond the file length (" + l + "): " + last);
      }
      if (last.getOffset() + last.getLength() > l) {
        throw new EOFException("Range extends beyond the file length (" + l + "): " + last);
      }
    }
    return sortedRanges;
  }

  /**
   * Sort the input ranges by offset; no validation is done.
   * @param input input ranges.
   * @return a new list of the ranges, sorted by offset.
   */
  public static List<? extends FileRange> sortRangeList(List<? extends FileRange> input) {
    final List<? extends FileRange> l = new ArrayList<>(input);
    l.sort(Comparator.comparingLong(FileRange::getOffset));
    return l;
  }

  /**
   * Sort the input ranges by offset; no validation is done.
   * <p>
   * This method is used externally and must be retained with
   * the signature unchanged.
   * @param input input ranges.
   * @return a new list of the ranges, sorted by offset.
   */
  @InterfaceStability.Stable
  public static FileRange[] sortRanges(List<? extends FileRange> input) {
    return sortRangeList(input).toArray(new FileRange[0]);
  }

  /**
   * Merge sorted ranges to optimize the access from the underlying file
   * system.
   * The motivations are that:
   * <ul>
   *   <li>Upper layers want to pass down logical file ranges.</li>
   *   <li>Fewer reads have better performance.</li>
   *   <li>Applications want callbacks as ranges are read.</li>
   *   <li>Some file systems want to round ranges to be at checksum boundaries.</li>
   * </ul>
   *
   * @param sortedRanges already sorted list of ranges based on offset.
   * @param chunkSize round the start and end points to multiples of chunkSize
   * @param minimumSeek the smallest gap that we should seek over in bytes
   * @param maxSize the largest combined file range in bytes
   * @return the list of sorted CombinedFileRanges that cover the input
   */
  public static List<CombinedFileRange> mergeSortedRanges(List<? extends FileRange> sortedRanges,
                                                          int chunkSize,
                                                          int minimumSeek,
                                                          int maxSize) {

    CombinedFileRange current = null;
    List<CombinedFileRange> result = new ArrayList<>(sortedRanges.size());

    // now merge together the ones that merge
    for (FileRange range: sortedRanges) {
      long start = roundDown(range.getOffset(), chunkSize);
      long end = roundUp(range.getOffset() + range.getLength(), chunkSize);
      if (current == null || !current.merge(start, end, range, minimumSeek, maxSize)) {
        current = new CombinedFileRange(start, end, range);
        result.add(current);
      }
    }
    return result;
  }

  /**
   * Slice the data that was read to the user's request.
   * This function assumes that the user's request is completely subsumed by the
   * read data. This always creates a new buffer pointing to the same underlying
   * data but with its own mark and position fields such that reading one buffer
   * can't effect other's mark and position.
   * @param readData the buffer with the readData
   * @param readOffset the offset in the file for the readData
   * @param request the user's request
   * @return the readData buffer that is sliced to the user's request
   */
  public static ByteBuffer sliceTo(ByteBuffer readData, long readOffset,
                                   FileRange request) {
    int offsetChange = (int) (request.getOffset() - readOffset);
    int requestLength = request.getLength();
    // Create a new buffer that is backed by the original contents
    // The buffer will have position 0 and the same limit as the original one
    readData = readData.slice();
    // Change the offset and the limit of the buffer as the reader wants to see
    // only relevant data
    readData.position(offsetChange);
    readData.limit(offsetChange + requestLength);
    // Create a new buffer after the limit change so that only that portion of the data is
    // returned to the reader.
    readData = readData.slice();
    return readData;
  }

  /**
   * Implement {@link ByteBufferPositionedReadable#readFully(long, ByteBuffer)} by
   * converting to a vector read.
   * @param stream stream
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @throws IOException if there is some error performing the read
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   */
  public static void vectorizeByteBufferPositionedReadableReadFully(
      final PositionedReadable stream,
      final long position,
      final ByteBuffer buf) throws IOException {
    final int length = buf.remaining();
    if (length == 0) {
      // exit fast on zero-length read
      return;
    }
    final FileRange range = FileRange.createFileRange(position, length);
    // read with an allocator which returns the buffer passed in
    // rejecting a second attempt or any attempt to read a different length.
    readVectored(
        stream,
        Collections.singletonList(range),
        allocateExistingBuffer(buf));
    FutureIO.awaitFuture(range.getData());
  }

  /**
   * Special allocator which always returns the same buffer.
   * It will reject any request for a buffer of a different size,
   * or any attempt to issue a second buffer.
   * @param buf buffer
   * @return an allocator which serves up the buffer, once.
   */
  private static IntFunction<ByteBuffer> allocateExistingBuffer(final ByteBuffer buf) {
    final AtomicBoolean issued = new AtomicBoolean(false);
    final int length = buf.remaining();
    return len -> {
      checkArgument(len == length,
          "Read length must match buffer length: %s != %s",
          length, len);
      checkArgument(!issued.getAndSet(true),
          "buffer already issued");
      return buf;
    };
  }

  /**
   * Implement {@link ByteBufferPositionedReadable#read(long, ByteBuffer)} by
   * converting to a vector read.
   * This attempts to read the whole buffer and fails on partial reads,
   * returning -1.
   * @param stream stream
   * @param fileLength known file length
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @return the number of bytes read, possibly zero, or -1 if reached
   * end-of-stream
   * @throws IOException if there is some error performing the read
   */
  public static int vectorizeByteBufferPositionedReadableRead(
      final PositionedReadable stream,
      final long fileLength,
      final long position,
      final ByteBuffer buf) throws IOException {
    final int remaining = buf.remaining();
    if (remaining == 0) {
      // exit fast on zero-length read
      return 0;
    }
    if (position >= fileLength) {
      // EOF
      return -1;
    }

    long toEOF = fileLength - position;
    int length = (int)(Math.min(remaining, toEOF));
    try {
      vectorizeByteBufferPositionedReadableReadFully(stream, position, buf);
      return length;
    } catch (EOFException e) {
      final int bytesRead = length - buf.remaining();
      LOG.debug("EOF reading from {} at position {} bytesRead {}",
          stream, position, bytesRead, e);
      return bytesRead;
    }
  }


  /**
   * private constructor.
   */
  private VectoredReadUtils() {
    throw new UnsupportedOperationException();
  }
}
