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

import com.aayushatharva.brotli4j.encoder.Encoder;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.hadoop.io.compress.BrotliCodec.DEFAULT_LZ_WINDOW_SIZE;
import static org.apache.hadoop.io.compress.BrotliCodec.DEFAULT_MODE;
import static org.apache.hadoop.io.compress.BrotliCodec.DEFAULT_QUALITY;
import static org.apache.hadoop.io.compress.BrotliCodec.LZ_WINDOW_SIZE_PROP;
import static org.apache.hadoop.io.compress.BrotliCodec.MODE_PROP;
import static org.apache.hadoop.io.compress.BrotliCodec.QUALITY_LEVEL_PROP;

public class BrotliCompressor implements Compressor {

  private static final Logger LOG =
          LoggerFactory.getLogger(BrotliCompressor.class);

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

  private final StackTraceElement[] stack;

  private final Encoder.Parameters parameter = new Encoder.Parameters();

  // Using a direct byte buffer as input prevents a JNI-side copy
  private ByteBuffer inBuffer = ByteBuffer.allocateDirect(16384);
  private ByteBuffer outBuffer = EMPTY_BUFFER;
  private boolean compressing = false;
  private boolean shouldFinish = false;
  private boolean flushed = false;
  private int totalBytesIn = 0;
  private int totalBytesOut = 0;

  public BrotliCompressor(Configuration conf) {
    reinit(conf);
    this.stack = Thread.currentThread().getStackTrace();
  }

  private boolean isOutputBufferEmpty() {
    return outBuffer.position() == 0;
  }

  private boolean hasMoreOutput() {
    return outBuffer.hasRemaining();
  }

  @Override
  public void setInput(byte[] inBytes, int off, int len) {
    Preconditions.checkState(isOutputBufferEmpty(),
        "[BUG] setInput called with non-empty output buffer");
    Preconditions.checkState(!compressing,
        "[BUG] setInput called while compressing the input buffer");
    Preconditions.checkState(inBuffer.remaining() > (inBuffer.capacity() >> 1),
        "[BUG] setInput called with a full input buffer");
    ensureCapacity(len);

    // copy as much of the input as possible
    int bytesToCopy = Math.min(len, inBuffer.remaining());
    Preconditions.checkState(bytesToCopy == len,
        "[BUG] Cannot copy the entire input");
    inBuffer.put(inBytes, off, bytesToCopy);

    // if the buffer is full, compress it and copy any remaining input
    if (shouldFinish || inBuffer.remaining() <= (inBuffer.capacity() >> 1)) {
      compress(shouldFinish);
    }

    this.totalBytesIn += len;
  }

  /**
   * This checks that the given size is less than the current input buffer's
   * capacity. If not, the capacity of the input buffer is increased. This
   * ensures that an incoming buffer of size bytes can be handled by setInput,
   * even if the input buffer is almost full.
   */
  private void ensureCapacity(int size) {
    int targetCapacity = inBuffer.capacity() / 2;
    if (targetCapacity > size) {
      return;
    }

    // find a capacity that works for the current request
    while (targetCapacity < size) {
      targetCapacity *= 2;
    }

    // increase the input buffer size
    ByteBuffer oldBuffer = inBuffer;
    this.inBuffer = ByteBuffer.allocateDirect(targetCapacity * 2);
    oldBuffer.flip(); // prepare for reading
    inBuffer.put(oldBuffer);
  }

  /**
   * Compresses and clears the input buffer. After this method is called, the
   * output buffer must be read entirely before calling setInput again.
   */
  private void compress(boolean flush) {
    if (!compressing) {
      this.compressing = true;
      inBuffer.flip(); // prepare for reading
    }

    ByteBuffer toCompress = inBuffer.duplicate();
    int toRead = toCompress.remaining();

    final byte[] compressed;
    try {
      byte[] dst = new byte[toRead];
      toCompress.get(dst, 0, toRead);
      compressed = Encoder.compress(dst, parameter);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    this.outBuffer.put(compressed);

    inBuffer.position(toCompress.position());
    if (!inBuffer.hasRemaining()) {
      this.flushed = flush;
      inBuffer.clear(); // prepare for writing
      this.compressing = false;
    }
  }

  @Override
  public boolean needsInput() {
    return !compressing && isOutputBufferEmpty();
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  @Override
  public long getBytesRead() {
    return totalBytesOut;
  }

  @Override
  public long getBytesWritten() {
    return totalBytesIn;
  }

  @Override
  public void finish() {
    this.shouldFinish = true;
  }

  @Override
  public boolean finished() {
    return shouldFinish && flushed && isOutputBufferEmpty() && !compressing;
  }

  @Override
  public int compress(byte[] out, int off, int len) throws IOException {
    int bytesCopied = 0;

    if (isOutputBufferEmpty()) {
      if (compressing) {
        compress(shouldFinish);
      } else if (shouldFinish && !flushed) {
        compress(true);
      }
    }

    if (hasMoreOutput()) {
      int bytesToCopy = Math.min(len, outBuffer.position());
      outBuffer.flip();
      outBuffer.get(out, off, bytesToCopy);
      bytesCopied += bytesToCopy;
    }

    totalBytesOut += bytesCopied;

    return bytesCopied;
  }

  @Override
  public void reset() {
    end();
    Preconditions.checkState(totalBytesIn == 0 ||
        (flushed && !compressing && inBuffer.position() == 0),
        "Reused without consuming all input");
    Preconditions.checkState(isOutputBufferEmpty(),
        "Reused without consuming all output");

    this.inBuffer.clear();
    this.outBuffer = EMPTY_BUFFER;
    this.compressing = false;
    this.shouldFinish = false;
    this.flushed = false;
    this.totalBytesIn = 0;
    this.totalBytesOut = 0;
  }

  @Override
  public void end() {
    if (compressing || inBuffer.position() > 0) {
      LOG.warn("Closed without consuming all input");
    } else if (hasMoreOutput()) {
      LOG.warn("Closed without consuming all output");
    }
  }

  @Override
  public void reinit(Configuration conf) {
    this.parameter
            .setMode(DEFAULT_MODE)
            .setQuality(DEFAULT_QUALITY)
            .setWindow(DEFAULT_LZ_WINDOW_SIZE);

    if (conf != null) {
      this.parameter
          .setMode(Encoder.Mode.of(conf.get(MODE_PROP, DEFAULT_MODE.name())))
          .setQuality(conf.getInt(QUALITY_LEVEL_PROP, DEFAULT_QUALITY))
          .setWindow(conf.getInt(LZ_WINDOW_SIZE_PROP, DEFAULT_LZ_WINDOW_SIZE));
    }
    this.outBuffer = ByteBuffer.allocateDirect(16384);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();

    if (hasMoreOutput()) {
      String trace = Joiner
              .on("\n\t")
              .join(Arrays.copyOfRange(stack, 1, stack.length));
      LOG.warn("A compressor is being GC-ed without consuming all its output. "
               + "Created at:\n\t{}", trace);
    }
  }
}
