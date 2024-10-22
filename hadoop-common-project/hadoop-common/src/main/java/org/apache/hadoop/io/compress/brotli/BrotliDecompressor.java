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

package org.apache.hadoop.io.compress.brotli;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BrotliDecompressor implements Decompressor {

  private static final Logger LOG =
          LoggerFactory.getLogger(BrotliDecompressor.class);

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final ByteBuffer outBuffer;
  private final StackTraceElement[] stack;

  private ByteBuffer inBuffer = EMPTY_BUFFER;
  private long totalBytesIn = 0;
  private long totalBytesOut = 0;

  public BrotliDecompressor() {
    this.outBuffer = ByteBuffer.allocateDirect(16384);
    outBuffer.limit(0); // must be empty
    this.stack = Thread.currentThread().getStackTrace();
  }

  @Override
  public void setInput(byte[] inBytes, int off, int len) {
    Preconditions.checkState(isInputBufferEmpty(),
             "[BUG] Cannot call setInput with existing unconsumed input.");
    // this must use a ByteBuffer because not all of the bytes must be consumed
    this.inBuffer = ByteBuffer.wrap(inBytes, off, len);
    getMoreOutput();
    totalBytesIn += len;
  }

  private void getMoreOutput() {
    Preconditions.checkState(isOutputBufferEmpty(),
              "[BUG] Cannot call getMoreOutput without consuming all output.");
    outBuffer.clear();
    try {
      new BrotliDirectDecompressor().decompress(inBuffer, outBuffer);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  @Override
  public boolean needsInput() {
    return isInputBufferEmpty() && !hasMoreOutput();
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException(
            "Brotli decompression does not support dictionaries");
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean finished() {
    return isInputBufferEmpty() && !hasMoreOutput();
  }

  @Override
  public int decompress(byte[] out, int off, int len) {
    int bytesCopied = 0;
    int currentOffset = off;

    if (isOutputBufferEmpty() && (hasMoreInput())) {
      getMoreOutput();
    }

    while (bytesCopied < len && hasMoreOutput()) {
      int bytesToCopy = Math.min(len - bytesCopied, outBuffer.position());
      outBuffer.flip();
      outBuffer.get(out, currentOffset, bytesToCopy);
      currentOffset += bytesToCopy;

      if (isOutputBufferEmpty() && (hasMoreInput())) {
        getMoreOutput();
      }

      bytesCopied += bytesToCopy;
    }

    totalBytesOut += bytesCopied;

    return bytesCopied;
  }

  @Override
  public int getRemaining() {
    int available = outBuffer.remaining();
    if (available > 0) {
      return available;
    }
    return 0;
  }

  @Override
  public void reset() {
    Preconditions.checkState(isOutputBufferEmpty(),
              "Reused without consuming all output");
    end();
    outBuffer.limit(0);
    this.inBuffer = EMPTY_BUFFER;
    this.totalBytesIn = 0;
    this.totalBytesOut = 0;
  }

  @Override
  public void end() {
    if (!isOutputBufferEmpty()) {
      LOG.warn("Closed without consuming all output");
    }
  }

  public long getTotalBytesIn() {
    return totalBytesIn;
  }

  public long getTotalBytesOut() {
    return totalBytesOut;
  }

  private boolean hasMoreOutput() {
    return outBuffer.hasRemaining();
  }

  private boolean hasMoreInput() {
    return inBuffer.hasRemaining();
  }

  private boolean isOutputBufferEmpty() {
    return outBuffer.position() == 0;
  }

  private boolean isInputBufferEmpty() {
    return inBuffer.position() == 0;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();

    String message = "A decompressor is being GC-ed without consuming all its ";
    if (hasMoreInput()) {
      message += "input";

      if (hasMoreOutput()) {
        message += " and output";
      }
    } else if (hasMoreOutput()) {
      message += "output";
    }

    if (message.endsWith("put")) {
      String trace = Joiner
              .on("\n\t")
              .join(Arrays.copyOfRange(stack, 1, stack.length));
      LOG.warn("{}. Created at:\n\t{}", message, trace);
    }
  }
}
