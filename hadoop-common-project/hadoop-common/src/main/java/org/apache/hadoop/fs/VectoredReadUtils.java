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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.functional.Function4RaisingIOE;

/**
 * Utility class which implements helper methods used
 * in vectored IO implementation.
 */
public final class VectoredReadUtils {

  private static final int TMP_BUFFER_MAX_SIZE = 64 * 1024;

  /**
   * Validate a single range.
   * @param range file range.
   * @throws EOFException any EOF Exception.
   */
  public static void validateRangeRequest(FileRange range)
          throws EOFException {

    Preconditions.checkArgument(range.getLength() >= 0, "length is negative");
    if (range.getOffset() < 0) {
      throw new EOFException("position is negative");
    }
  }

  /**
   * Validate a list of vectored read ranges.
   * @param ranges list of ranges.
   * @throws EOFException any EOF exception.
   */
  public static void validateVectoredReadRanges(List<? extends FileRange> ranges)
          throws EOFException {
    for (FileRange range : ranges) {
      validateRangeRequest(range);
    }
  }



  /**
   * This is the default implementation which iterates through the ranges
   * to read each synchronously, but the intent is that subclasses
   * can make more efficient readers.
   * The data or exceptions are pushed into {@link FileRange#getData()}.
   * @param stream the stream to read the data from
   * @param ranges the byte ranges to read
   * @param allocate the byte buffer allocation
   */
  public static void readVectored(PositionedReadable stream,
                                  List<? extends FileRange> ranges,
                                  IntFunction<ByteBuffer> allocate) {
    for (FileRange range: ranges) {
      range.setData(readRangeFrom(stream, range, allocate));
    }
  }

  /**
   * Synchronously reads a range from the stream dealing with the combinations
   * of ByteBuffers buffers and PositionedReadable streams.
   * @param stream the stream to read from
   * @param range the range to read
   * @param allocate the function to allocate ByteBuffers
   * @return the CompletableFuture that contains the read data
   */
  public static CompletableFuture<ByteBuffer> readRangeFrom(PositionedReadable stream,
                                                            FileRange range,
                                                            IntFunction<ByteBuffer> allocate) {
    CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
    try {
      ByteBuffer buffer = allocate.apply(range.getLength());
      if (stream instanceof ByteBufferPositionedReadable) {
        ((ByteBufferPositionedReadable) stream).readFully(range.getOffset(),
            buffer);
        buffer.flip();
      } else {
        readNonByteBufferPositionedReadable(stream, range, buffer);
      }
      result.complete(buffer);
    } catch (IOException ioe) {
      result.completeExceptionally(ioe);
    }
    return result;
  }

  private static void readNonByteBufferPositionedReadable(PositionedReadable stream,
                                                          FileRange range,
                                                          ByteBuffer buffer) throws IOException {
    if (buffer.isDirect()) {
      readInDirectBuffer(range.getLength(),
          buffer,
          (position, buffer1, offset, length) -> {
            stream.readFully(position, buffer1, offset, length);
            return null;
          });
      buffer.flip();
    } else {
      stream.readFully(range.getOffset(), buffer.array(),
              buffer.arrayOffset(), range.getLength());
    }
  }

  /**
   * Read bytes from stream into a byte buffer using an
   * intermediate byte array.
   * @param length number of bytes to read.
   * @param buffer buffer to fill.
   * @param operation operation to use for reading data.
   * @throws IOException any IOE.
   */
  public static void readInDirectBuffer(int length,
                                        ByteBuffer buffer,
                                        Function4RaisingIOE<Integer, byte[], Integer,
                                                Integer, Void> operation) throws IOException {
    if (length == 0) {
      return;
    }
    int readBytes = 0;
    int position = 0;
    int tmpBufferMaxSize = Math.min(TMP_BUFFER_MAX_SIZE, length);
    byte[] tmp = new byte[tmpBufferMaxSize];
    while (readBytes < length) {
      int currentLength = (readBytes + tmpBufferMaxSize) < length ?
              tmpBufferMaxSize
              : (length - readBytes);
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
   * Calculates the ceil value of offset based on chunk size.
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
   * Check if the input ranges are overlapping in nature.
   * We call two ranges to be overlapping when start offset
   * of second is less than the end offset of first.
   * End offset is calculated as start offset + length.
   * @param input list if input ranges.
   * @return true/false based on logic explained above.
   */
  public static List<? extends FileRange> validateNonOverlappingAndReturnSortedRanges(
          List<? extends FileRange> input) {

    if (input.size() <= 1) {
      return input;
    }
    FileRange[] sortedRanges = sortRanges(input);
    FileRange prev = sortedRanges[0];
    for (int i=1; i<sortedRanges.length; i++) {
      if (sortedRanges[i].getOffset() < prev.getOffset() + prev.getLength()) {
        throw new UnsupportedOperationException("Overlapping ranges are not supported");
      }
      prev = sortedRanges[i];
    }
    return Arrays.asList(sortedRanges);
  }

  /**
   * Sort the input ranges by offset.
   * @param input input ranges.
   * @return sorted ranges.
   */
  public static FileRange[] sortRanges(List<? extends FileRange> input) {
    FileRange[] sortedRanges = input.toArray(new FileRange[0]);
    Arrays.sort(sortedRanges, Comparator.comparingLong(FileRange::getOffset));
    return sortedRanges;
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
   * private constructor.
   */
  private VectoredReadUtils() {
    throw new UnsupportedOperationException();
  }
}
