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

package org.apache.hadoop.hbase.io;

import java.io.*;

/** A reusable {@link DataOutput} implementation that writes to an in-memory
 * buffer.
 * 
 * <p>This is copy of Hadoop SequenceFile brought local so we can fix bugs;
 * e.g. hbase-1097</p>
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
public class DataOutputBuffer extends DataOutputStream {

  private static class Buffer extends ByteArrayOutputStream {
    public byte[] getData() { return buf; }
    public int getLength() { return count; }
    // Keep the initial buffer around so can put it back in place on reset.
    private final byte [] initialBuffer;

    public Buffer() {
      super();
      this.initialBuffer = this.buf;
    }
    
    public Buffer(int size) {
      super(size);
      this.initialBuffer = this.buf;
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
    
    @Override
    public synchronized void reset() {
      // Rest the buffer so we don't keep around the shape of the biggest
      // value ever read.
      this.buf = this.initialBuffer;
      super.reset();
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
   * @return byte[] 
   */
  public byte[] getData() { return buffer.getData(); }

  /** Returns the length of the valid data currently in the buffer. 
   * @return int
   */
  public int getLength() { return buffer.getLength(); }

  /** Resets the buffer to empty. 
   * @return DataOutputBuffer
   */
  public DataOutputBuffer reset() {
    this.written = 0;
    buffer.reset();
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. 
   * @param in 
   * @param length 
   * @throws IOException
   */
  public void write(DataInput in, int length) throws IOException {
    buffer.write(in, length);
  }

  /** Write to a file stream 
   * @param out 
   * @throws IOException
   */
  public void writeTo(OutputStream out) throws IOException {
    buffer.writeTo(out);
  }
}