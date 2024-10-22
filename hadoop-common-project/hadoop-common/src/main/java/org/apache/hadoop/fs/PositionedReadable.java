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
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;

/**
 * Stream that permits positional reading.
 *
 * Implementations are required to implement thread-safe operations; this may
 * be supported by concurrent access to the data, or by using a synchronization
 * mechanism to serialize access.
 *
 * Not all implementations meet this requirement. Those that do not cannot
 * be used as a backing store for some applications, such as Apache HBase.
 *
 * Independent of whether or not they are thread safe, some implementations
 * may make the intermediate state of the system, specifically the position
 * obtained in {@code Seekable.getPos()} visible.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PositionedReadable {
  /**
   * Read up to the specified number of bytes, from a given
   * position within a file, and return the number of bytes read. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @return actual number of bytes read; -1 means "none"
   * @throws IOException IO problems.
   */
  int read(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * Read the specified number of bytes, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @param offset offset in the buffer
   * @param length number of bytes to read
   * @throws IOException IO problems.
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   */
  void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * Read number of bytes equal to the length of the buffer, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   * @param position position within file
   * @param buffer destination buffer
   * @throws IOException IO problems.
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   */
  void readFully(long position, byte[] buffer) throws IOException;

  /**
   * What is the smallest reasonable seek?
   * @return the minimum number of bytes
   */
  default int minSeekForVectorReads() {
    return 4 * 1024;
  }

  /**
   * What is the largest size that we should group ranges together as?
   * @return the number of bytes to read at once
   */
  default int maxReadSizeForVectorReads() {
    return 1024 * 1024;
  }

  /**
   * Read fully a list of file ranges asynchronously from this file.
   * The default iterates through the ranges to read each synchronously, but
   * the intent is that FSDataInputStream subclasses can make more efficient
   * readers.
   * As a result of the call, each range will have FileRange.setData(CompletableFuture)
   * called with a future that when complete will have a ByteBuffer with the
   * data from the file's range.
   * <p>
   *   The position returned by getPos() after readVectored() is undefined.
   * </p>
   * <p>
   *   If a file is changed while the readVectored() operation is in progress, the output is
   *   undefined. Some ranges may have old data, some may have new and some may have both.
   * </p>
   * <p>
   *   While a readVectored() operation is in progress, normal read api calls may block.
   * </p>
   * @param ranges the byte ranges to read
   * @param allocate the function to allocate ByteBuffer
   * @throws IOException any IOE.
   * @throws IllegalArgumentException if the any of ranges are invalid, or they overlap.
   */
  default void readVectored(List<? extends FileRange> ranges,
                            IntFunction<ByteBuffer> allocate) throws IOException {
    VectoredReadUtils.readVectored(this, ranges, allocate);
  }

  /**
   * Variant of {@link #readVectored(List, IntFunction)} where a release() function
   * may be invoked if problems surface during reads -this method is called to
   * try to return any allocated buffer which has not been read yet.
   * Buffers which have successfully been read and returned to the caller do not
   * get released: this is for failures only.
   * <p>
   * The default implementation calls readVectored/2 so as to ensure that
   * if an existing stream implementation does not implement this method
   * all is good.
   * <p>
   * Implementations SHOULD override this method if they can release buffers as
   * part of their error handling.
   * @param ranges the byte ranges to read
   * @param allocate the function to allocate ByteBuffer
   * @param release the function to release a ByteBuffer.
   * @throws IOException any IOE.
   * @throws IllegalArgumentException if the any of ranges are invalid, or they overlap.
   */
  default void readVectored(List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocate,
      Consumer<ByteBuffer> release) throws IOException {
    readVectored(ranges, allocate);
  }

}
