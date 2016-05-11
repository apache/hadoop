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

package org.apache.hadoop.io;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

/** A reusable {@link java.io.DataInput} implementation
 * that reads from an in-memory buffer.
 *
 * <p>This saves memory over creating a new DataInputStream and
 * ByteArrayInputStream each time data is read.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * DataInputBuffer buffer = new DataInputBuffer();
 * while (... loop condition ...) {
 *   byte[] data = ... get data ...;
 *   int dataLength = ... get data length ...;
 *   buffer.reset(data, dataLength);
 *   ... read buffer using DataInput methods ...
 * }
 * </pre>
 *  
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DataInputBuffer extends DataInputStream {
  private static class Buffer extends ByteArrayInputStream {
    public Buffer() {
      super(new byte[] {});
    }

    public void reset(byte[] input, int start, int length) {
      this.buf = input;
      this.count = start+length;
      this.mark = start;
      this.pos = start;
    }

    public byte[] getData() {
      return buf;
    }

    public int getPosition() {
      return pos;
    }

    public int getLength() {
      return count;
    }

    /* functions below comes verbatim from
     hive.common.io.NonSyncByteArrayInputStream */

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() {
      return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(byte[] b, int off, int len) {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      }
      if (pos >= count) {
        return -1;
      }
      if (pos + len > count) {
        len = count - pos;
      }
      if (len <= 0) {
        return 0;
      }
      System.arraycopy(buf, pos, b, off, len);
      pos += len;
      return len;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long skip(long n) {
      if (pos + n > count) {
        n = count - pos;
      }
      if (n < 0) {
        return 0;
      }
      pos += n;
      return n;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int available() {
      return count - pos;
    }
  }

  private Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public DataInputBuffer() {
    this(new Buffer());
  }

  private DataInputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Resets the data that the buffer reads. */
  public void reset(byte[] input, int length) {
    buffer.reset(input, 0, length);
  }

  /** Resets the data that the buffer reads. */
  public void reset(byte[] input, int start, int length) {
    buffer.reset(input, start, length);
  }
  
  public byte[] getData() {
    return buffer.getData();
  }

  /** Returns the current position in the input. */
  public int getPosition() { return buffer.getPosition(); }

  /**
   * Returns the index one greater than the last valid character in the input
   * stream buffer.
   */
  public int getLength() { return buffer.getLength(); }

}
