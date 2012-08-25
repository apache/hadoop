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

/** A WritableComparable for ints. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class IntWritable implements WritableComparable<IntWritable> {
  private int value;

  public IntWritable() {}

  public IntWritable(int value) { set(value); }

  /** Set the value of this IntWritable. */
  public void set(int value) { this.value = value; }

  /** Return the value of this IntWritable. */
  public int get() { return value; }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(value);
  }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IntWritable))
      return false;
    IntWritable other = (IntWritable)o;
    return this.value == other.value;
  }

  @Override
  public int hashCode() {
    return value;
  }

  /** Compares two IntWritables. */
  @Override
  public int compareTo(IntWritable o) {
    int thisValue = this.value;
    int thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public String toString() {
    return Integer.toString(value);
  }

  /** A Comparator optimized for IntWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(IntWritable.class);
    }
    
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int thisValue = readInt(b1, s1);
      int thatValue = readInt(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(IntWritable.class, new Comparator());
  }
}

