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
import java.util.Arrays;
import java.util.Random;


public class RandomDatum implements WritableComparable<RandomDatum> {
  private int length;
  private byte[] data;

  public RandomDatum() {}

  public RandomDatum(Random random) {
    length = 10 + (int) Math.pow(10.0, random.nextFloat() * 3.0);
    data = new byte[length];
    random.nextBytes(data);
  }

  public int getLength() {
    return length;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(length);
    out.write(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    length = in.readInt();
    if (data == null || length > data.length)
      data = new byte[length];
    in.readFully(data, 0, length);
  }

  @Override
  public int compareTo(RandomDatum o) {
    return WritableComparator.compareBytes(this.data, 0, this.length,
                                           o.data, 0, o.length);
  }

  @Override
  public boolean equals(Object o) {
    return compareTo((RandomDatum)o) == 0;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.data);
  }
  
  private static final char[] HEX_DIGITS =
  {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /** Returns a string representation of this object. */
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder(length*2);
    for (int i = 0; i < length; i++) {
      int b = data[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();
  }

  public static class Generator {
    Random random;

    private RandomDatum key;
    private RandomDatum value;
    
    public Generator() { random = new Random(); }
    public Generator(int seed) { random = new Random(seed); }

    public RandomDatum getKey() { return key; }
    public RandomDatum getValue() { return value; }

    public void next() {
      key = new RandomDatum(random);
      value = new RandomDatum(random);
    }
  }

  /** A WritableComparator optimized for RandomDatum. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(RandomDatum.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int n1 = readInt(b1, s1);
      int n2 = readInt(b2, s2);
      return compareBytes(b1, s1+4, n1, b2, s2+4, n2);
    }
  }

}
