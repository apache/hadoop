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

package org.apache.hadoop.io.compress;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.compress.Decompressor;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DecompressorStream extends CompressionInputStream {
  protected Decompressor decompressor = null;
  protected byte[] buffer;
  protected boolean eof = false;
  protected boolean closed = false;
  private int lastBytesSent = 0;

  public DecompressorStream(InputStream in, Decompressor decompressor,
                            int bufferSize)
  throws IOException {
    super(in);

    if (decompressor == null) {
      throw new NullPointerException();
    } else if (bufferSize <= 0) {
      throw new IllegalArgumentException("Illegal bufferSize");
    }

    this.decompressor = decompressor;
    buffer = new byte[bufferSize];
  }

  public DecompressorStream(InputStream in, Decompressor decompressor)
  throws IOException {
    this(in, decompressor, 512);
  }

  /**
   * Allow derived classes to directly set the underlying stream.
   * 
   * @param in Underlying input stream.
   * @throws IOException
   */
  protected DecompressorStream(InputStream in) throws IOException {
    super(in);
  }
  
  private byte[] oneByte = new byte[1];
  @Override
  public int read() throws IOException {
    checkStream();
    return (read(oneByte, 0, oneByte.length) == -1) ? -1 : (oneByte[0] & 0xff);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkStream();
    
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    return decompress(b, off, len);
  }

  protected int decompress(byte[] b, int off, int len) throws IOException {
    int n = 0;

    while ((n = decompressor.decompress(b, off, len)) == 0) {
      if (decompressor.needsDictionary()) {
        eof = true;
        return -1;
      }

      if (decompressor.finished()) {
        // First see if there was any leftover buffered input from previous
        // stream; if not, attempt to refill buffer.  If refill -> EOF, we're
        // all done; else reset, fix up input buffer, and get ready for next
        // concatenated substream/"member".
        int nRemaining = decompressor.getRemaining();
        if (nRemaining == 0) {
          int m = getCompressedData();
          if (m == -1) {
            // apparently the previous end-of-stream was also end-of-file:
            // return success, as if we had never called getCompressedData()
            eof = true;
            return -1;
          }
          decompressor.reset();
          decompressor.setInput(buffer, 0, m);
          lastBytesSent = m;
        } else {
          // looks like it's a concatenated stream:  reset low-level zlib (or
          // other engine) and buffers, then "resend" remaining input data
          decompressor.reset();
          int leftoverOffset = lastBytesSent - nRemaining;
          assert (leftoverOffset >= 0);
          // this recopies userBuf -> direct buffer if using native libraries:
          decompressor.setInput(buffer, leftoverOffset, nRemaining);
          // NOTE:  this is the one place we do NOT want to save the number
          // of bytes sent (nRemaining here) into lastBytesSent:  since we
          // are resending what we've already sent before, offset is nonzero
          // in general (only way it could be zero is if it already equals
          // nRemaining), which would then screw up the offset calculation
          // _next_ time around.  IOW, getRemaining() is in terms of the
          // original, zero-offset bufferload, so lastBytesSent must be as
          // well.  Cheesy ASCII art:
          //
          //          <------------ m, lastBytesSent ----------->
          //          +===============================================+
          // buffer:  |1111111111|22222222222222222|333333333333|     |
          //          +===============================================+
          //     #1:  <-- off -->|<-------- nRemaining --------->
          //     #2:  <----------- off ----------->|<-- nRem. -->
          //     #3:  (final substream:  nRemaining == 0; eof = true)
          //
          // If lastBytesSent is anything other than m, as shown, then "off"
          // will be calculated incorrectly.
        }
      } else if (decompressor.needsInput()) {
        int m = getCompressedData();
        if (m == -1) {
          throw new EOFException("Unexpected end of input stream");
        }
        decompressor.setInput(buffer, 0, m);
        lastBytesSent = m;
      }
    }

    return n;
  }

  protected int getCompressedData() throws IOException {
    checkStream();
  
    // note that the _caller_ is now required to call setInput() or throw
    return in.read(buffer, 0, buffer.length);
  }

  protected void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }
  
  @Override
  public void resetState() throws IOException {
    decompressor.reset();
  }

  private byte[] skipBytes = new byte[512];
  @Override
  public long skip(long n) throws IOException {
    // Sanity checks
    if (n < 0) {
      throw new IllegalArgumentException("negative skip length");
    }
    checkStream();
    
    // Read 'n' bytes
    int skipped = 0;
    while (skipped < n) {
      int len = Math.min(((int)n - skipped), skipBytes.length);
      len = read(skipBytes, 0, len);
      if (len == -1) {
        eof = true;
        break;
      }
      skipped += len;
    }
    return skipped;
  }

  @Override
  public int available() throws IOException {
    checkStream();
    return (eof) ? 0 : 1;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      in.close();
      closed = true;
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void mark(int readlimit) {
  }

  @Override
  public synchronized void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }

}
