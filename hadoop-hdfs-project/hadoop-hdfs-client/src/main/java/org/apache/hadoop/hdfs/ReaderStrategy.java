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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Wraps different possible read implementations so that callers can be
 * strategy-agnostic.
 */
interface ReaderStrategy {
  /**
   * Read from a block using the blockReader.
   * @param blockReader
   * @return number of bytes read
   * @throws IOException
   */
  int readFromBlock(BlockReader blockReader) throws IOException;

  /**
   * Read from a block using the blockReader with desired length to read.
   * @param blockReader
   * @param length number of bytes desired to read, not ensured
   * @return number of bytes read
   * @throws IOException
   */
  int readFromBlock(BlockReader blockReader, int length) throws IOException;

  /**
   * Read or copy from a src buffer.
   * @param src
   * @return number of bytes copied
   * Note: the position of the src buffer is not changed after the call
   */
  int readFromBuffer(ByteBuffer src);

  /**
   * Read or copy length of data bytes from a src buffer with desired length.
   * @param src
   * @return number of bytes copied
   * Note: the position of the src buffer is not changed after the call
   */
  int readFromBuffer(ByteBuffer src, int length);

  /**
   * @return the target read buffer that reads data into.
   */
  ByteBuffer getReadBuffer();

  /**
   * @return the target length to read.
   */
  int getTargetLength();
}

/**
 * Used to read bytes into a byte array buffer. Note it's not thread-safe
 * and the behavior is not defined if concurrently operated.
 */
class ByteArrayStrategy implements ReaderStrategy {
  private final DFSClient dfsClient;
  private final ReadStatistics readStatistics;
  private final byte[] readBuf;
  private int offset;
  private final int targetLength;

  /**
   * The constructor.
   * @param readBuf target buffer to read into
   * @param offset offset into the buffer
   * @param targetLength target length of data
   * @param readStatistics statistics counter
   */
  public ByteArrayStrategy(byte[] readBuf, int offset, int targetLength,
                           ReadStatistics readStatistics,
                           DFSClient dfsClient) {
    this.readBuf = readBuf;
    this.offset = offset;
    this.targetLength = targetLength;
    this.readStatistics = readStatistics;
    this.dfsClient = dfsClient;
  }

  @Override
  public ByteBuffer getReadBuffer() {
    return ByteBuffer.wrap(readBuf, offset, targetLength);
  }

  @Override
  public int getTargetLength() {
    return targetLength;
  }

  @Override
  public int readFromBlock(BlockReader blockReader) throws IOException {
    return readFromBlock(blockReader, targetLength);
  }

  @Override
  public int readFromBlock(BlockReader blockReader,
                           int length) throws IOException {
    int nRead = blockReader.read(readBuf, offset, length);
    if (nRead > 0) {
      offset += nRead;
    }
    return nRead;
  }

  @Override
  public int readFromBuffer(ByteBuffer src) {
    return readFromBuffer(src, src.remaining());
  }

  @Override
  public int readFromBuffer(ByteBuffer src, int length) {
    ByteBuffer dup = src.duplicate();
    dup.get(readBuf, offset, length);
    offset += length;
    return length;
  }
}

/**
 * Used to read bytes into a user-supplied ByteBuffer. Note it's not thread-safe
 * and the behavior is not defined if concurrently operated. When read operation
 * is performed, the position of the underlying byte buffer will move forward as
 * stated in ByteBufferReadable#read(ByteBuffer buf) method.
 */
class ByteBufferStrategy implements ReaderStrategy {
  private final DFSClient dfsClient;
  private final ReadStatistics readStatistics;
  private final ByteBuffer readBuf;
  private final int targetLength;

  /**
   * The constructor.
   * @param readBuf target buffer to read into
   * @param readStatistics statistics counter
   */
  ByteBufferStrategy(ByteBuffer readBuf,
                     ReadStatistics readStatistics,
                     DFSClient dfsClient) {
    this.readBuf = readBuf;
    this.targetLength = readBuf.remaining();
    this.readStatistics = readStatistics;
    this.dfsClient = dfsClient;
  }

  @Override
  public ByteBuffer getReadBuffer() {
    return readBuf;
  }

  @Override
  public int readFromBlock(BlockReader blockReader) throws IOException {
    return readFromBlock(blockReader, readBuf.remaining());
  }

  @Override
  public int readFromBlock(BlockReader blockReader,
                           int length) throws IOException {
    ByteBuffer tmpBuf = readBuf.duplicate();
    tmpBuf.limit(tmpBuf.position() + length);
    int nRead = blockReader.read(tmpBuf);
    // Only when data are read, update the position
    if (nRead > 0) {
      readBuf.position(readBuf.position() + nRead);
    }

    return nRead;
  }

  @Override
  public int getTargetLength() {
    return targetLength;
  }

  @Override
  public int readFromBuffer(ByteBuffer src) {
    return readFromBuffer(src, src.remaining());
  }

  @Override
  public int readFromBuffer(ByteBuffer src, int length) {
    ByteBuffer dup = src.duplicate();
    int newLen = Math.min(readBuf.remaining(), dup.remaining());
    newLen = Math.min(newLen, length);
    dup.limit(dup.position() + newLen);
    readBuf.put(dup);
    return newLen;
  }
}
