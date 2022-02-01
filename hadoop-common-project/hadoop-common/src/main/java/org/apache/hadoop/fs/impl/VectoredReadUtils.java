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

package org.apache.hadoop.fs.impl;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.util.Preconditions;

/**
 * Utility class which implements helper methods used
 * in vectored IO implementation.
 */
public final class VectoredReadUtils {

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
   * Read fully a list of file ranges asynchronously from this file.
   * The default iterates through the ranges to read each synchronously, but
   * the intent is that subclasses can make more efficient readers.
   * The data or exceptions are pushed into {@link FileRange#getData()}.
   * @param stream the stream to read the data from
   * @param ranges the byte ranges to read
   * @param allocate the byte buffer allocation
   * @param minimumSeek the minimum number of bytes to seek over
   * @param maximumRead the largest number of bytes to combine into a single read
   */
  public static void readVectored(PositionedReadable stream,
                                  List<? extends FileRange> ranges,
                                  IntFunction<ByteBuffer> allocate,
                                  int minimumSeek,
                                  int maximumRead) {
    if (isOrderedDisjoint(ranges, 1, minimumSeek)) {
      for(FileRange range: ranges) {
        range.setData(readRangeFrom(stream, range, allocate));
      }
    } else {
      for(CombinedFileRange range: sortAndMergeRanges(ranges, 1, minimumSeek,
          maximumRead)) {
        CompletableFuture<ByteBuffer> read =
            readRangeFrom(stream, range, allocate);
        for(FileRange child: range.getUnderlying()) {
          child.setData(read.thenApply(
              (b) -> sliceTo(b, range.getOffset(), child)));
        }
      }
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
      buffer.put(readInDirectBuffer(stream, range));
      buffer.flip();
    } else {
      stream.readFully(range.getOffset(), buffer.array(),
              buffer.arrayOffset(), range.getLength());
    }
  }

  private static byte[] readInDirectBuffer(PositionedReadable stream,
                                           FileRange range) throws IOException {
    // if we need to read data from a direct buffer and the stream doesn't
    // support it, we allocate a byte array to use.
    byte[] tmp = new byte[range.getLength()];
    stream.readFully(range.getOffset(), tmp, 0, tmp.length);
    return tmp;
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
    for(FileRange range: input) {
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
   * Sort and merge ranges to optimize the access from the underlying file
   * system.
   * The motivations are that:
   * <ul>
   *   <li>Upper layers want to pass down logical file ranges.</li>
   *   <li>Fewer reads have better performance.</li>
   *   <li>Applications want callbacks as ranges are read.</li>
   *   <li>Some file systems want to round ranges to be at checksum boundaries.</li>
   * </ul>
   *
   * @param input the list of input ranges
   * @param chunkSize round the start and end points to multiples of chunkSize
   * @param minimumSeek the smallest gap that we should seek over in bytes
   * @param maxSize the largest combined file range in bytes
   * @return the list of sorted CombinedFileRanges that cover the input
   */
  public static List<CombinedFileRange> sortAndMergeRanges(List<? extends FileRange> input,
                                                           int chunkSize,
                                                           int minimumSeek,
                                                           int maxSize) {
    // sort the ranges by offset
    FileRange[] ranges = input.toArray(new FileRange[0]);
    Arrays.sort(ranges, Comparator.comparingLong(FileRange::getOffset));
    CombinedFileRange current = null;
    List<CombinedFileRange> result = new ArrayList<>(ranges.length);

    // now merge together the ones that merge
    for(FileRange range: ranges) {
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
    readData = readData.slice();
    readData.position(offsetChange);
    readData.limit(offsetChange + requestLength);
    return readData;
  }

  /**
   * private constructor.
   */
  private VectoredReadUtils() {
    throw new UnsupportedOperationException();
  }
}
