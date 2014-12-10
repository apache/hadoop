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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A WritableComparable for longs. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongWritable implements WritableComparable<LongWritable> {
  private long value;

  public LongWritable() {}

  public LongWritable(long value) { set(value); }

  /** Set the value of this LongWritable. */
  public void set(long value) { this.value = value; }

  /** Return the value of this LongWritable. */
  public long get() { return value; }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(value);
  }

  /** Returns true iff <code>o</code> is a LongWritable with the same value. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof LongWritable))
      return false;
    LongWritable other = (LongWritable)o;
    return this.value == other.value;
  }

  @Override
  public int hashCode() {
    return (int)value;
  }

  /** Compares two LongWritables. */
  @Override
  public int compareTo(LongWritable o) {
    long thisValue = this.value;
    long thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }

  /** A Comparator optimized for LongWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(LongWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      long thatValue = readLong(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  /** A decreasing Comparator optimized for LongWritable. */ 
  public static class DecreasingComparator extends Comparator {
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return super.compare(b, a);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return super.compare(b2, s2, l2, b1, s1, l1);
    }
  }

  static {                                       // register default comparator
    WritableComparator.define(LongWritable.class, new Comparator());
  }

}

