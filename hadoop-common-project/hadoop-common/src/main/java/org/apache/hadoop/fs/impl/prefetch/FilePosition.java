/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.nio.ByteBuffer;

import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNegative;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNull;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkPositiveInteger;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkState;
import static org.apache.hadoop.fs.impl.prefetch.Validate.checkWithinRange;

/**
 * Provides functionality related to tracking the position within a file.
 *
 * The file is accessed through an in memory buffer. The absolute position within
 * the file is the sum of start offset of the buffer within the file and the relative
 * offset of the current access location within the buffer.
 *
 * A file is made up of equal sized blocks. The last block may be of a smaller size.
 * The size of a buffer associated with this file is typically the same as block size.
 */
public final class FilePosition {

  /**
   * Holds block based information about a file.
   */
  private BlockData blockData;

  /**
   * Information about the buffer in use.
   */
  private BufferData data;

  /**
   * Provides access to the underlying file.
   */
  private ByteBuffer buffer;

  /**
   * Start offset of the buffer relative to the start of a file.
   */
  private long bufferStartOffset;

  /**
   * Offset where reading starts relative to the start of a file.
   */
  private long readStartOffset;

  // Read stats after a seek (mostly for debugging use).
  private int numSingleByteReads;

  private int numBytesRead;

  private int numBufferReads;

  /**
   * Constructs an instance of {@link FilePosition}.
   *
   * @param fileSize size of the associated file.
   * @param blockSize size of each block within the file.
   *
   * @throws IllegalArgumentException if fileSize is negative.
   * @throws IllegalArgumentException if blockSize is zero or negative.
   */
  public FilePosition(long fileSize, int blockSize) {
    checkNotNegative(fileSize, "fileSize");
    if (fileSize == 0) {
      checkNotNegative(blockSize, "blockSize");
    } else {
      checkPositiveInteger(blockSize, "blockSize");
    }

    this.blockData = new BlockData(fileSize, blockSize);

    // The position is valid only when a valid buffer is associated with this file.
    this.invalidate();
  }

  /**
   * Associates a buffer with this file.
   *
   * @param bufferData the buffer associated with this file.
   * @param startOffset Start offset of the buffer relative to the start of a file.
   * @param readOffset Offset where reading starts relative to the start of a file.
   *
   * @throws IllegalArgumentException if bufferData is null.
   * @throws IllegalArgumentException if startOffset is negative.
   * @throws IllegalArgumentException if readOffset is negative.
   * @throws IllegalArgumentException if readOffset is outside the range [startOffset, buffer end].
   */
  public void setData(BufferData bufferData,
      long startOffset,
      long readOffset) {
    checkNotNull(bufferData, "bufferData");
    checkNotNegative(startOffset, "startOffset");
    checkNotNegative(readOffset, "readOffset");
    checkWithinRange(
        readOffset,
        "readOffset",
        startOffset,
        startOffset + bufferData.getBuffer().limit());

    data = bufferData;
    buffer = bufferData.getBuffer().duplicate();
    bufferStartOffset = startOffset;
    readStartOffset = readOffset;
    setAbsolute(readOffset);

    resetReadStats();
  }

  public ByteBuffer buffer() {
    throwIfInvalidBuffer();
    return buffer;
  }

  public BufferData data() {
    throwIfInvalidBuffer();
    return data;
  }

  /**
   * Gets the current absolute position within this file.
   *
   * @return the current absolute position within this file.
   */
  public long absolute() {
    throwIfInvalidBuffer();
    return bufferStartOffset + relative();
  }

  /**
   * If the given {@code pos} lies within the current buffer, updates the current position to
   * the specified value and returns true; otherwise returns false without changing the position.
   *
   * @param pos the absolute position to change the current position to if possible.
   * @return true if the given current position was updated, false otherwise.
   */
  public boolean setAbsolute(long pos) {
    if (isValid() && isWithinCurrentBuffer(pos)) {
      int relativePos = (int) (pos - bufferStartOffset);
      buffer.position(relativePos);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Gets the current position within this file relative to the start of the associated buffer.
   *
   * @return the current position within this file relative to the start of the associated buffer.
   */
  public int relative() {
    throwIfInvalidBuffer();
    return buffer.position();
  }

  /**
   * Determines whether the given absolute position lies within the current buffer.
   *
   * @param pos the position to check.
   * @return true if the given absolute position lies within the current buffer, false otherwise.
   */
  public boolean isWithinCurrentBuffer(long pos) {
    throwIfInvalidBuffer();
    long bufferEndOffset = bufferStartOffset + buffer.limit();
    return (pos >= bufferStartOffset) && (pos <= bufferEndOffset);
  }

  /**
   * Gets the id of the current block.
   *
   * @return the id of the current block.
   */
  public int blockNumber() {
    throwIfInvalidBuffer();
    return blockData.getBlockNumber(bufferStartOffset);
  }

  /**
   * Determines whether the current block is the last block in this file.
   *
   * @return true if the current block is the last block in this file, false otherwise.
   */
  public boolean isLastBlock() {
    return blockData.isLastBlock(blockNumber());
  }

  /**
   * Determines if the current position is valid.
   *
   * @return true if the current position is valid, false otherwise.
   */
  public boolean isValid() {
    return buffer != null;
  }

  /**
   * Marks the current position as invalid.
   */
  public void invalidate() {
    buffer = null;
    bufferStartOffset = -1;
    data = null;
  }

  /**
   * Gets the start of the current block's absolute offset.
   *
   * @return the start of the current block's absolute offset.
   */
  public long bufferStartOffset() {
    throwIfInvalidBuffer();
    return bufferStartOffset;
  }

  /**
   * Determines whether the current buffer has been fully read.
   *
   * @return true if the current buffer has been fully read, false otherwise.
   */
  public boolean bufferFullyRead() {
    throwIfInvalidBuffer();
    return (bufferStartOffset == readStartOffset)
        && (relative() == buffer.limit())
        && (numBytesRead == buffer.limit());
  }

  public void incrementBytesRead(int n) {
    numBytesRead += n;
    if (n == 1) {
      numSingleByteReads++;
    } else {
      numBufferReads++;
    }
  }

  public int numBytesRead() {
    return numBytesRead;
  }

  public int numSingleByteReads() {
    return numSingleByteReads;
  }

  public int numBufferReads() {
    return numBufferReads;
  }

  private void resetReadStats() {
    numBytesRead = 0;
    numSingleByteReads = 0;
    numBufferReads = 0;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (buffer == null) {
      sb.append("currentBuffer = null");
    } else {
      int pos = buffer.position();
      int val;
      if (pos >= buffer.limit()) {
        val = -1;
      } else {
        val = buffer.get(pos);
      }
      String currentBufferState =
          String.format("%d at pos: %d, lim: %d", val, pos, buffer.limit());
      sb.append(String.format(
          "block: %d, pos: %d (CBuf: %s)%n",
          blockNumber(), absolute(),
          currentBufferState));
      sb.append("\n");
    }
    return sb.toString();
  }

  private void throwIfInvalidBuffer() {
    checkState(buffer != null, "'buffer' must not be null");
  }
}
