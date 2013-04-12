/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A byte array backed output stream with a limit. The limit should be smaller
 * than the buffer capacity. The object can be reused through <code>reset</code>
 * API and choose different limits in each round.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class BoundedByteArrayOutputStream extends OutputStream {
  private byte[] buffer;
  private int startOffset;
  private int limit;
  private int currentPointer;

  /**
   * Create a BoundedByteArrayOutputStream with the specified
   * capacity
   * @param capacity The capacity of the underlying byte array
   */
  public BoundedByteArrayOutputStream(int capacity) {
    this(capacity, capacity);
  }

  /**
   * Create a BoundedByteArrayOutputStream with the specified
   * capacity and limit.
   * @param capacity The capacity of the underlying byte array
   * @param limit The maximum limit upto which data can be written
   */
  public BoundedByteArrayOutputStream(int capacity, int limit) {
    this(new byte[capacity], 0, limit);
  }

  protected BoundedByteArrayOutputStream(byte[] buf, int offset, int limit) {
    resetBuffer(buf, offset, limit);
  }
  
  protected void resetBuffer(byte[] buf, int offset, int limit) {
    int capacity = buf.length - offset;
    if ((capacity < limit) || (capacity | limit) < 0) {
      throw new IllegalArgumentException("Invalid capacity/limit");
    }
    this.buffer = buf;
    this.startOffset = offset;
    this.currentPointer = offset;
    this.limit = offset + limit;
  }
  
  @Override
  public void write(int b) throws IOException {
    if (currentPointer >= limit) {
      throw new EOFException("Reaching the limit of the buffer.");
    }
    buffer[currentPointer++] = (byte) b;
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (currentPointer + len > limit) {
      throw new EOFException("Reach the limit of the buffer");
    }

    System.arraycopy(b, off, buffer, currentPointer, len);
    currentPointer += len;
  }

  /**
   * Reset the limit 
   * @param newlim New Limit
   */
  public void reset(int newlim) {
    if (newlim > (buffer.length - startOffset)) {
      throw new IndexOutOfBoundsException("Limit exceeds buffer size");
    }
    this.limit = newlim;
    this.currentPointer = startOffset;
  }

  /** Reset the buffer */
  public void reset() {
    this.limit = buffer.length - startOffset;
    this.currentPointer = startOffset;
  }

  /** Return the current limit */
  public int getLimit() {
    return limit;
  }

  /** Returns the underlying buffer.
   *  Data is only valid to {@link #size()}.
   */
  public byte[] getBuffer() {
    return buffer;
  }

  /** Returns the length of the valid data 
   * currently in the buffer.
   */
  public int size() {
    return currentPointer - startOffset;
  }
  
  public int available() {
    return limit - currentPointer;
  }
}
