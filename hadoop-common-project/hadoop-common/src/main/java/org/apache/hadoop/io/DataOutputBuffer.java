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

import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/** A reusable {@link DataOutput} implementation that writes to an in-memory
 * buffer.
 *
 * <p>This saves memory over creating a new DataOutputStream and
 * ByteArrayOutputStream each time data is written.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * DataOutputBuffer buffer = new DataOutputBuffer();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write buffer using DataOutput methods ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 *  
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DataOutputBuffer extends DataOutputStream {

  private static class Buffer extends ByteArrayOutputStream {
    public byte[] getData() { return buf; }
    public int getLength() { return count; }

    public Buffer() {
      super();
    }
    
    public Buffer(int size) {
      super(size);
    }
    
    public void write(DataInput in, int len) throws IOException {
      int newcount = count + len;
      if (newcount > buf.length) {
        byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      in.readFully(buf, count, len);
      count = newcount;
    }

    /**
     * Set the count for the current buf.
     * @param newCount the new count to set
     * @return the original count
     */
    private int setCount(int newCount) {
      Preconditions.checkArgument(newCount >= 0 && newCount <= buf.length);
      int oldCount = count;
      count = newCount;
      return oldCount;
    }
  }

  private Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public DataOutputBuffer() {
    this(new Buffer());
  }
  
  public DataOutputBuffer(int size) {
    this(new Buffer(size));
  }
  
  private DataOutputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Returns the current contents of the buffer.
   *  Data is only valid to {@link #getLength()}.
   */
  public byte[] getData() { return buffer.getData(); }

  /** Returns the length of the valid data currently in the buffer. */
  public int getLength() { return buffer.getLength(); }

  /** Resets the buffer to empty. */
  public DataOutputBuffer reset() {
    this.written = 0;
    buffer.reset();
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. */
  public void write(DataInput in, int length) throws IOException {
    buffer.write(in, length);
  }

  /** Write to a file stream */
  public void writeTo(OutputStream out) throws IOException {
    buffer.writeTo(out);
  }

  /**
   * Overwrite an integer into the internal buffer. Note that this call can only
   * be used to overwrite existing data in the buffer, i.e., buffer#count cannot
   * be increased, and DataOutputStream#written cannot be increased.
   */
  public void writeInt(int v, int offset) throws IOException {
    Preconditions.checkState(offset + 4 <= buffer.getLength());
    byte[] b = new byte[4];
    b[0] = (byte) ((v >>> 24) & 0xFF);
    b[1] = (byte) ((v >>> 16) & 0xFF);
    b[2] = (byte) ((v >>> 8) & 0xFF);
    b[3] = (byte) ((v >>> 0) & 0xFF);
    int oldCount = buffer.setCount(offset);
    buffer.write(b);
    buffer.setCount(oldCount);
  }
}
