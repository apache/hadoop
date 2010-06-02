/**
 * Copyright 2009 The Apache Software Foundation
 *
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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A byte sequence that is usable as a key or value.  Based on
 * {@link org.apache.hadoop.io.BytesWritable} only this class is NOT resizable
 * and DOES NOT distinguish between the size of the seqeunce and the current
 * capacity as {@link org.apache.hadoop.io.BytesWritable} does. Hence its
 * comparatively 'immutable'. When creating a new instance of this class,
 * the underlying byte [] is not copied, just referenced.  The backing
 * buffer is accessed when we go to serialize.
 */
public class ImmutableBytesWritable
implements WritableComparable<ImmutableBytesWritable> {
  private byte[] bytes;
  private int offset;
  private int length;

  /**
   * Create a zero-size sequence.
   */
  public ImmutableBytesWritable() {
    super();
  }

  /**
   * Create a ImmutableBytesWritable using the byte array as the initial value.
   * @param bytes This array becomes the backing storage for the object.
   */
  public ImmutableBytesWritable(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  /**
   * Set the new ImmutableBytesWritable to the contents of the passed
   * <code>ibw</code>.
   * @param ibw the value to set this ImmutableBytesWritable to.
   */
  public ImmutableBytesWritable(final ImmutableBytesWritable ibw) {
    this(ibw.get(), 0, ibw.getSize());
  }

  /**
   * Set the value to a given byte range
   * @param bytes the new byte range to set to
   * @param offset the offset in newData to start at
   * @param length the number of bytes in the range
   */
  public ImmutableBytesWritable(final byte[] bytes, final int offset,
      final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Get the data from the BytesWritable.
   * @return The data is only valid between offset and offset+length.
   */
  public byte [] get() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
    }
    return this.bytes;
  }

  /**
   * @param b Use passed bytes as backing array for this instance.
   */
  public void set(final byte [] b) {
    set(b, 0, b.length);
  }

  /**
   * @param b Use passed bytes as backing array for this instance.
   * @param offset
   * @param length
   */
  public void set(final byte [] b, final int offset, final int length) {
    this.bytes = b;
    this.offset = offset;
    this.length = length;
  }

  /**
   * @return the number of valid bytes in the buffer
   */
  public int getSize() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
    }
    return this.length;
  }

  /**
   * @return the number of valid bytes in the buffer
   */
  //Should probably deprecate getSize() so that we keep the same calls for all
  //byte []
  public int getLength() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
    }
    return this.length;
  }

  /**
   * @return offset
   */
  public int getOffset(){
    return this.offset;
  }

  public void readFields(final DataInput in) throws IOException {
    this.length = in.readInt();
    this.bytes = new byte[this.length];
    in.readFully(this.bytes, 0, this.length);
    this.offset = 0;
  }

  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.length);
    out.write(this.bytes, this.offset, this.length);
  }

  // Below methods copied from BytesWritable
  @Override
  public int hashCode() {
    int hash = 1;
    for (int i = offset; i < offset + length; i++)
      hash = (31 * hash) + (int)bytes[i];
    return hash;
  }

  /**
   * Define the sort order of the BytesWritable.
   * @param that The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *         negative if left is smaller than right.
   */
  public int compareTo(ImmutableBytesWritable that) {
    return WritableComparator.compareBytes(
      this.bytes, this.offset, this.length,
      that.bytes, that.offset, that.length);
  }

  /**
   * Compares the bytes in this object to the specified byte array
   * @param that
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *         negative if left is smaller than right.
   */
  public int compareTo(final byte [] that) {
    return WritableComparator.compareBytes(
      this.bytes, this.offset, this.length,
      that, 0, that.length);
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object right_obj) {
    if (right_obj instanceof byte []) {
      return compareTo((byte [])right_obj) == 0;
    }
    if (right_obj instanceof ImmutableBytesWritable) {
      return compareTo((ImmutableBytesWritable)right_obj) == 0;
    }
    return false;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(3*this.bytes.length);
    for (int idx = offset; idx < offset + length; idx++) {
      // if not the first, put a blank separator in
      if (idx != offset) {
        sb.append(' ');
      }
      String num = Integer.toHexString(bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  /** A Comparator optimized for ImmutableBytesWritable.
   */
  public static class Comparator extends WritableComparator {
    private BytesWritable.Comparator comparator =
      new BytesWritable.Comparator();

    /** constructor */
    public Comparator() {
      super(ImmutableBytesWritable.class);
    }

    /**
     * @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return comparator.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  static { // register this comparator
    WritableComparator.define(ImmutableBytesWritable.class, new Comparator());
  }

  /**
   * @param array List of byte [].
   * @return Array of byte [].
   */
  public static byte [][] toArray(final List<byte []> array) {
    // List#toArray doesn't work on lists of byte [].
    byte[][] results = new byte[array.size()][];
    for (int i = 0; i < array.size(); i++) {
      results[i] = array.get(i);
    }
    return results;
  }

  /**
   * Returns a copy of the bytes referred to by this writable
   */
  public byte[] copyBytes() {
    return Arrays.copyOfRange(bytes, offset, offset+length);
  }
}
