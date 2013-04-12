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

/** A WritableComparable for shorts. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ShortWritable implements WritableComparable<ShortWritable> {
  private short value;

  public ShortWritable() {
  }

  public ShortWritable(short value) {
    set(value);
  }

  /** Set the value of this ShortWritable. */
  public void set(short value) {
    this.value = value;
  }

  /** Return the value of this ShortWritable. */
  public short get() {
    return value;
  }

  /** read the short value */
  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readShort();
  }

  /** write short value */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(value);
  }

  /** Returns true iff <code>o</code> is a ShortWritable with the same value. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ShortWritable))
      return false;
    ShortWritable other = (ShortWritable) o;
    return this.value == other.value;
  }

  /** hash code */
  @Override
  public int hashCode() {
    return value;
  }

  /** Compares two ShortWritable. */
  @Override
  public int compareTo(ShortWritable o) {
    short thisValue = this.value;
    short thatValue = (o).value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  /** Short values in string format */
  @Override
  public String toString() {
    return Short.toString(value);
  }

  /** A Comparator optimized for ShortWritable. */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(ShortWritable.class);
    }
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      short thisValue = (short) readUnsignedShort(b1, s1);
      short thatValue = (short) readUnsignedShort(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(ShortWritable.class, new Comparator());
  }

}
