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
package org.apache.hadoop.hdfs.util;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

/**
 * An InputStream implementations which reads from some other InputStream
 * but expects an exact number of bytes. Any attempts to read past the
 * specified number of bytes will return as if the end of the stream
 * was reached. If the end of the underlying stream is reached prior to
 * the specified number of bytes, an EOFException is thrown.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExactSizeInputStream extends FilterInputStream {
  private int remaining;

  /**
   * Construct an input stream that will read no more than
   * 'numBytes' bytes.
   * 
   * If an EOF occurs on the underlying stream before numBytes
   * bytes have been read, an EOFException will be thrown.
   * 
   * @param in the inputstream to wrap
   * @param numBytes the number of bytes to read
   */
  public ExactSizeInputStream(InputStream in, int numBytes) {
    super(in);
    Preconditions.checkArgument(numBytes >= 0,
        "Negative expected bytes: ", numBytes);
    this.remaining = numBytes;
  }

  @Override
  public int available() throws IOException {
    return Math.min(super.available(), remaining);
  }

  @Override
  public int read() throws IOException {
    // EOF if we reached our limit
    if (remaining <= 0) {
      return -1;
    }
    final int result = super.read();
    if (result >= 0) {
      --remaining;
    } else if (remaining > 0) {
      // Underlying stream reached EOF but we haven't read the expected
      // number of bytes.
      throw new EOFException(
          "Premature EOF. Expected " + remaining + "more bytes");
    }
    return result;
  }

  @Override
  public int read(final byte[] b, final int off, int len)
                  throws IOException {
    if (remaining <= 0) {
      return -1;
    }
    len = Math.min(len, remaining);
    final int result = super.read(b, off, len);
    if (result >= 0) {
      remaining -= result;
    } else if (remaining > 0) {
      // Underlying stream reached EOF but we haven't read the expected
      // number of bytes.
      throw new EOFException(
          "Premature EOF. Expected " + remaining + "more bytes");
    }
    return result;
  }

  @Override
  public long skip(final long n) throws IOException {
    final long result = super.skip(Math.min(n, remaining));
    if (result > 0) {
      remaining -= result;
    } else if (remaining > 0) {
      // Underlying stream reached EOF but we haven't read the expected
      // number of bytes.
      throw new EOFException(
          "Premature EOF. Expected " + remaining + "more bytes");
    }
    return result;
  }
  
  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }
  
}