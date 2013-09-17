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
package org.apache.hadoop.examples.pi.math;

import java.math.BigInteger;

/** Support 124-bit integer arithmetic. */
class LongLong {
  static final int BITS_PER_LONG = 62;
  static final int MID = BITS_PER_LONG >> 1;
  static final int SIZE = BITS_PER_LONG << 1;

  static final long FULL_MASK = (1L << BITS_PER_LONG) - 1;
  static final long LOWER_MASK = FULL_MASK >>> MID;
  static final long UPPER_MASK = LOWER_MASK << MID;

  private long d0;
  private long d1;

  /** Set the values. */
  LongLong set(long d0, long d1) {
    this.d0 = d0;
    this.d1 = d1;
    return this;
  }

  /** And operation (&). */
  long and(long mask) {
    return d0 & mask;
  }

  /** Shift right operation (<<). */
  long shiftRight(int n) {
    return (d1 << (BITS_PER_LONG - n)) + (d0 >>> n);
  }

  /** Plus equal operation (+=). */
  LongLong plusEqual(LongLong that) {
    this.d0 += that.d0;
    this.d1 += that.d1;
    return this;
  }

  /** Convert this to a BigInteger. */
  BigInteger toBigInteger() {
    return BigInteger.valueOf(d1).shiftLeft(BITS_PER_LONG).add(BigInteger.valueOf(d0));
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final int remainder = BITS_PER_LONG % 4;
    return String.format("%x*2^%d + %016x", d1<<remainder, BITS_PER_LONG-remainder, d0);
  }

  /** Compute a*b and store the result to r.
   * @return r
   */
  static LongLong multiplication(final LongLong r, final long a, final long b) {
    /*
    final long x0 = a & LOWER_MASK;
    final long x1 = (a & UPPER_MASK) >> MID;

    final long y0 = b & LOWER_MASK;
    final long y1 = (b & UPPER_MASK) >> MID;

    final long t = (x0 + x1)*(y0 + y1);
    final long u = (x0 - x1)*(y0 - y1);
    final long v = x1*y1;

    final long tmp = (t - u)>>>1;
    result.d0 = ((t + u)>>>1) - v + ((tmp << MID) & FULL_MASK);;
    result.d1 = v + (tmp >> MID);
    return result;
    */
    final long a_lower = a & LOWER_MASK;
    final long a_upper = (a & UPPER_MASK) >> MID;

    final long b_lower = b & LOWER_MASK;
    final long b_upper = (b & UPPER_MASK) >> MID;

    final long tmp = a_lower*b_upper + a_upper*b_lower;
    r.d0 = a_lower*b_lower + ((tmp << MID) & FULL_MASK);
    r.d1 = a_upper*b_upper + (tmp >> MID);
    return r;
  }
}