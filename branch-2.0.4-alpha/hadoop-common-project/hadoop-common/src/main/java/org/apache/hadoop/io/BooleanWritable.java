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

/** 
 * A WritableComparable for booleans. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BooleanWritable implements WritableComparable<BooleanWritable> {
  private boolean value;

  /** 
   */
  public BooleanWritable() {};

  /** 
   */
  public BooleanWritable(boolean value) {
    set(value);
  }

  /** 
   * Set the value of the BooleanWritable
   */    
  public void set(boolean value) {
    this.value = value;
  }

  /**
   * Returns the value of the BooleanWritable
   */
  public boolean get() {
    return value;
  }

  /**
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readBoolean();
  }

  /**
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(value);
  }

  /**
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BooleanWritable)) {
      return false;
    }
    BooleanWritable other = (BooleanWritable) o;
    return this.value == other.value;
  }

  @Override
  public int hashCode() {
    return value ? 0 : 1;
  }



  /**
   */
  @Override
  public int compareTo(BooleanWritable o) {
    boolean a = this.value;
    boolean b = o.value;
    return ((a == b) ? 0 : (a == false) ? -1 : 1);
  }
  
  @Override
  public String toString() {
    return Boolean.toString(get());
  }

  /** 
   * A Comparator optimized for BooleanWritable. 
   */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(BooleanWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }


  static {
    WritableComparator.define(BooleanWritable.class, new Comparator());
  }
}
