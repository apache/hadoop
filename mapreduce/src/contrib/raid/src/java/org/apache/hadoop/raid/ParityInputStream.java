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

package org.apache.hadoop.raid;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.util.Progressable;

/**
 * Wraps over multiple input streams and provides an input stream that is
 * an XOR of the streams.
 */
class ParityInputStream extends InputStream {
  private static final int DEFAULT_BUFSIZE = 5*1024*1024;
  private InputStream[] streams;
  private byte[] xor;
  private byte[] buf;
  private int bufSize;
  private long remaining;
  private int available = 0;
  private int readPos = 0;

  public ParityInputStream(
      InputStream[] streams, long parityBlockSize, byte[] buf, byte[] xor) {
    assert buf.length == xor.length;
    bufSize = buf.length;
    this.streams = streams;
    remaining = parityBlockSize;
    this.buf = buf;
    this.xor = xor;
  }
  
  @Override
  public int read() throws IOException {
    makeAvailable();
    if (available == 0) {
      return -1;
    }
    int ret = xor[readPos];
    readPos++;
    available--;
    return ret;
  }
  
  @Override
  public int read(byte b[], int off, int len) throws IOException {
    makeAvailable();
    if (available == 0) {
      return -1;
    }
    int ret = Math.min(len, available);
    for (int i = 0; i < ret; ++i) {
      b[off+i] = xor[readPos+i];
    }
    readPos += ret;
    available -= ret;
    return ret;
  }

  public void close() throws IOException {
    for (InputStream i: streams) {
      i.close();
    }
  }
  
  /**
   * Send the contents of the stream to the sink.
   * @param sink
   * @param reporter
   * @throws IOException
   */
  public void drain(OutputStream sink, Progressable reporter)
          throws IOException {
    
    while (true) {
      makeAvailable();
      if (available == 0) {
        break;
      }
      sink.write(xor, readPos, available);
      available = 0;
      if (reporter != null) {
        reporter.progress();
      }
    }
  }

  /**
   * Make some bytes available for reading in the internal buffer.
   * @throws IOException
   */
  private void makeAvailable() throws IOException {
    if (available > 0 || remaining <= 0) {
      return;
    }
    // Read some bytes from the first stream.
    int xorlen = (int)Math.min(remaining, bufSize);
    readExact(streams[0], xor, xorlen);

    // Read bytes from all the other streams and xor them.
    for (int i = 1; i < streams.length; i++) {
      readExact(streams[i], buf, xorlen);

      for (int j = 0; j < xorlen; j++) {
        xor[j] ^= buf[j];
      }
    }
    
    remaining -= xorlen;
    available = xorlen;
    readPos = 0;
    readPos = 0;
  }

  private static void readExact(InputStream in, byte[] bufs, int toRead)
  throws IOException {
    int tread = 0;
    while (tread < toRead) {
      int read = in.read(bufs, tread, toRead - tread);
      if (read == -1) {
        // If the stream ends, fill in zeros.
        Arrays.fill(bufs, tread, toRead, (byte)0);
        tread = toRead;
      } else {
        tread += read;
      }
    }
    assert tread == toRead;
  }

}

