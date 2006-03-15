/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/** 
 * A byte sequence that is usable as a key or value.
 * It is resizable and distinguishes between the size of the seqeunce and
 * the current capacity. The hash function is the front of the md5 of the 
 * buffer. The sort order is the same as memcmp.
 * @author Doug Cutting
 */
public class BytesWritable implements WritableComparable {
  private int size;
  private byte[] bytes;
  
  /**
   * Create a zero-size sequence.
   */
  public BytesWritable() {
    size = 0;
    bytes = new byte[100];
  }
  
  /**
   * Create a BytesWritable using the byte array as the initial value.
   * @param bytes This array becomes the backing storage for the object.
   */
  public BytesWritable(byte[] bytes) {
    this.bytes = bytes;
    this.size = bytes.length;
  }
  
  /**
   * Get the data from the BytesWritable.
   * @return The data is only valid between 0 and getSize() - 1.
   */
  public byte[] get() {
    return bytes;
  }
  
  /**
   * Get the current size of the buffer.
   * @return
   */
  public int getSize() {
    return size;
  }
  
  /**
   * Change the size of the buffer. The values in the old range are preserved
   * and any new values are undefined. The capacity is changed if it is 
   * necessary.
   * @param size The new number of bytes
   */
  public void setSize(int size) {
    if (size > getCapacity()) {
      setCapacity(size * 3 / 2);
    }
    this.size = size;
  }
  
  /**
   * Get the capacity, which is the maximum size that could handled without
   * resizing the backing storage.
   * @return The number of bytes
   */
  public int getCapacity() {
    return bytes.length;
  }
  
  /**
   * Change the capacity of the backing storage.
   * The data is preserved.
   * @param new_cap The new capacity in bytes.
   */
  public void setCapacity(int new_cap) {
    if (new_cap != getCapacity()) {
      byte[] new_data = new byte[new_cap];
      if (new_cap < size) {
        size = new_cap;
      }
      if (size != 0) {
        System.arraycopy(bytes, 0, new_data, 0, size);
      }
      bytes = new_data;
    }
  }
  
  // inherit javadoc
  public void readFields(DataInput in) throws IOException {
    setSize(0); // clear the old data
    setSize(in.readInt());
    in.readFully(bytes, 0, size);
  }
  
  // inherit javadoc
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    out.write(bytes, 0, size);
  }
  
  public int hashCode() {
    return WritableComparator.hashBytes(bytes, size);
  }
  
  /**
   * Define the sort order of the BytesWritable.
   * @param right_obj The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *         negative if left is smaller than right.
   */
  public int compareTo(Object right_obj) {
    BytesWritable right = ((BytesWritable) right_obj);
    return WritableComparator.compareBytes(bytes, 0, size, 
                                           right.bytes, 0, right.size);
  }
  
  /**
   * Are the two byte sequences equal?
   */
  public boolean equals(Object right_obj) {
    if (right_obj instanceof BytesWritable) {
      return compareTo(right_obj) == 0;
    }
    return false;
  }
  
  /** A Comparator optimized for BytesWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(BytesWritable.class);
    }
    
    /**
     * Compare the buffers in serialized form.
     */
    public int compare(byte[] b1, int s1, int l1,
        byte[] b2, int s2, int l2) {
      int size1 = readInt(b1, s1);
      int size2 = readInt(b2, s2);
      return compareBytes(b1,s1+4, size1, b2, s2+4, size2);
    }
  }
  
  static {                                        // register this comparator
    WritableComparator.define(BytesWritable.class, new Comparator());
  }
  
}
