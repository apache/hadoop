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
package org.apache.hadoop.io.compress.snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyException;

public class SnappyCompressor implements Compressor {
  private static final Log logger = LogFactory.getLog(SnappyCompressor.class
      .getName());

  private boolean finish, finished;
  private ByteBuffer outBuf;
  private ByteBuffer compressedBuf;

  private long bytesRead = 0L;
  private long bytesWritten = 0L;

  public SnappyCompressor(int bufferSize) {
    outBuf = ByteBuffer.allocateDirect(bufferSize);
    compressedBuf = ByteBuffer.allocateDirect(Snappy
        .maxCompressedLength(bufferSize));

    reset();
  }

  public synchronized void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    finished = false;

    outBuf.put(b, off, len);

    bytesRead += len;
  }

  public synchronized void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  public synchronized boolean needsInput() {
    // needs input if compressed data was consumed
    if (compressedBuf.position() > 0
        && compressedBuf.limit() > compressedBuf.position())
      return false;

    return true;
  }

  public synchronized void finish() {
    finish = true;
  }

  public synchronized boolean finished() {
    // Check if all compressed data has been consumed
    return (finish && finished);
  }

  public synchronized int compress(byte[] b, int off, int len)
      throws IOException {

    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    if (finished || outBuf.position() == 0) {
      finished = true;
      return 0;
    }

    // Only need todo this once
    if (compressedBuf.position() == 0) {
      try {
        outBuf.limit(outBuf.position());
        outBuf.rewind();

        int lim = Snappy.compress(outBuf, compressedBuf);

        compressedBuf.limit(lim);
        compressedBuf.rewind();
      } catch (SnappyException e) {
        throw new IOException(e);
      }
    }

    int n = (compressedBuf.limit() - compressedBuf.position()) > len ? len
        : (compressedBuf.limit() - compressedBuf.position());

    if (n == 0) {
      finished = true;
      return 0;
    }

    compressedBuf.get(b, off, n);

    bytesWritten += n;

    // Set 'finished' if snappy has consumed all user-data
    if (compressedBuf.position() == compressedBuf.limit()) {
      finished = true;

      outBuf.limit(outBuf.capacity());
      outBuf.rewind();

      compressedBuf.limit(compressedBuf.capacity());
      compressedBuf.rewind();

    }

    return n;
  }

  public synchronized void reset() {
    finish = false;
    finished = false;

    outBuf.limit(outBuf.capacity());
    outBuf.rewind();

    compressedBuf.limit(compressedBuf.capacity());
    compressedBuf.rewind();

    bytesRead = bytesWritten = 0L;
  }

  public synchronized void reinit(Configuration conf) {
    reset();
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  public synchronized long getBytesRead() {
    return bytesRead;
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  public synchronized long getBytesWritten() {
    return bytesWritten;
  }

  public synchronized void end() {
  }

}
