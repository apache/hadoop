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

package org.apache.hadoop.io.compress.lzo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

public class LzoCompressor implements Compressor {
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  private int bufferSize;
  private byte[] input;
  private int inputOffset;
  private int inputLength;

  private byte[] inputBuffer;
  private int inputBufferLen;
  private int inputBufferOffset;
  private int inputBufferMaxLen;

  private boolean finish;
  // If `finished` is true, meaning all inputs are consumed and no input in buffer.
  private boolean finished;

  private byte[] outputBuffer;
  private int outputBufferLen;
  private int outputBufferOffset;
  private int outputBufferMaxLen;

  private long bytesRead;
  private long bytesWritten;

  private io.airlift.compress.lzo.LzoCompressor compressor;

  /**
   * Creates a new compressor.
   */
  public LzoCompressor(int bufferSize) {
    this.bufferSize = bufferSize;
    compressor = new io.airlift.compress.lzo.LzoCompressor();

    reset();
  }

  /**
   * Creates a new compressor with the default buffer size.
   */
  public LzoCompressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Sets input data for compression.
   * This should be called whenever #needsInput() returns
   * <code>true</code> indicating that more input data is required.
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check if we can buffer current input.
    if (len > inputBufferMaxLen - inputBufferOffset) {
      input = b;
      inputOffset = off;
      inputLength = len;
    } else {
      // We can buffer current input.
      System.arraycopy(inputBuffer, inputBufferOffset, b, off, len);
      inputBufferOffset += len;
      inputBufferLen += len;
    }

    bytesRead += len;
  }

  /**
   * Does nothing.
   */
  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * #setInput() should be called to provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         #setInput() should be called in order to provide more input.
   */
  @Override
  public boolean needsInput() {
    // We do not need input if:
    // 1. there still are compressed output in buffer, or
    // 2. input buffer is full, or
    // 3. there is direct input (the input cannot put into input buffer).
    // But if `finish()` is called, this compressor does not need input after all.
    return !((outputBufferLen - outputBufferOffset > 0) ||
            (inputBufferMaxLen - inputBufferOffset == 0) || inputLength > 0) && !finish;
  }

  /**
   * When called, indicates that compression should end
   * with the current contents of the input buffer.
   */
  @Override
  public void finish() {
    finish = true;
  }

  /**
   * Returns true if the end of the compressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the compressed
   *         data output stream has been reached.
   */
  @Override
  public boolean finished() {
    // This compressor is in finished status if:
    // 1. `finish()` is called, and
    // 2. all input are consumed, and
    // 3. no compressed data is in buffer.
    return finish && finished && (outputBufferLen - outputBufferOffset == 0);
  }

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public int compress(byte[] b, int off, int len)
          throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check compressed data in output buffer.
    if (outputBufferLen > 0) {
      int outputSize = Math.min(outputBufferLen, len);
      System.arraycopy(outputBuffer, outputBufferOffset, b, off, outputSize);
      outputBufferOffset += outputSize;
      outputBufferLen -= outputSize;
      bytesWritten += outputSize;
      return outputSize;
    }

    outputBufferOffset = outputBufferLen = 0;

    int inputSize = 0;
    // If no input data in input buffer.
    if (inputBufferLen == 0) {
      // Copy direct input.
      inputBufferOffset = 0;
      inputSize = copyIntoInputBuffer();
      if (inputSize == 0) {
        finished = true;
        return 0;
      }
    } else {
      inputSize = inputBufferLen;
    }

    System.out.println("inputBufferOffset: " + inputBufferOffset + " inputSize: " + inputSize);
    // Compress input and write to output buffer.
    int compressedSize = compressor.compress(inputBuffer, inputBufferOffset, inputSize,
            outputBuffer, outputBufferOffset, outputBufferMaxLen);
    inputBufferOffset += inputSize;
    inputBufferLen -= inputSize;

    outputBufferLen += compressedSize;
    System.out.println("compressedSize: " + compressedSize + " outputBufferLen: " + outputBufferLen);

    if (inputLength == 0) {
      finished = true;
    }

    // Copy from compressed data buffer to user buffer.
    int copiedSize = Math.min(compressedSize, len);
    bytesWritten += copiedSize;
    System.arraycopy(outputBuffer, outputBufferOffset, b, off, copiedSize);
    outputBufferOffset += copiedSize;
    outputBufferLen -= copiedSize;
    System.out.println("copiedSize: " + copiedSize + " outputBufferOffset: " + outputBufferOffset + " outputBufferLen: " + outputBufferLen);

    return copiedSize;
  }

  /**
   * Copies the some input data from user input to input buffer.
   */
  int copyIntoInputBuffer() {
    if (inputLength == 0) {
      return 0;
    }

    int inputSize = Math.min(inputLength,  inputBufferMaxLen);
    System.arraycopy(input, inputOffset, inputBuffer, inputBufferOffset, inputSize);

    inputBufferOffset += inputSize;
    inputBufferLen += inputSize;

    inputOffset += inputSize;
    inputLength -= inputSize;

    return inputSize;
  }

  /**
   * Resets compressor so that a new set of input data can be processed.
   */
  @Override
  public void reset() {
    input = null;
    inputOffset = inputLength = 0;
    finish = finished = false;

    inputBufferMaxLen = bufferSize;
    outputBufferMaxLen = compressor.maxCompressedLength(inputBufferMaxLen);

    inputBuffer = new byte[inputBufferMaxLen];
    outputBuffer = new byte[outputBufferMaxLen];

    inputBufferLen = inputBufferOffset = outputBufferLen = outputBufferOffset = 0;
    bytesRead = bytesWritten = 0;
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration
   *
   * @param conf Configuration from which new setting are fetched
   */
  @Override
  public void reinit(Configuration conf) {
    reset();
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Closes the compressor and discards any unprocessed input.
   */
  @Override
  public void end() {
  }
}
