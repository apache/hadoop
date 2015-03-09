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

/** Modular arithmetics */
public class Modular {
  static final long MAX_SQRT_LONG = (long)Math.sqrt(Long.MAX_VALUE);

  /** Compute 2^e mod n */
  public static long mod(long e, long n) {
    final int HALF = (63 - Long.numberOfLeadingZeros(n)) >> 1;
    final int FULL = HALF << 1;
    final long ONES = (1 << HALF) - 1; 

    long r = 2;
    for (long mask = Long.highestOneBit(e) >> 1; mask > 0; mask >>= 1) {
      if (r <= MAX_SQRT_LONG) {
        r *= r;
        if (r >= n) r %= n;
      } else {
        // r^2 will overflow
        final long high = r >>> HALF;
        final long low  = r &= ONES;
        
        r *= r;
        if (r >= n) r %= n;

        if (high != 0) {
          long s = high * high;
          if (s >= n) s %= n;
          for(int i = 0; i < FULL; i++)
            if ((s <<= 1) >= n) s -= n;
          
          if (low == 0)
            r = s;
          else {
            long t = high * low;
            if (t >= n) t %= n;
            for(int i = -1; i < HALF; i++)
              if ((t <<= 1) >= n) t -= n;
            
            r += s;
            if (r >= n) r -= n;
            r += t;
            if (r >= n) r -= n;
          }
        }
      }

      if ((e & mask) != 0) {
        r <<= 1;
        if (r >= n) r -= n;
      }
    }
    return r;
  }

  /** Given x in [0,1) and a in (-1,1),
   * return (x, a) mod 1.0. 
   */
  public static double addMod(double x, final double a) {
    x += a;
    return x >= 1? x - 1: x < 0? x + 1: x;
  }

  /** Given 0 &lt; x &lt; y,
   * return x^(-1) mod y.
   */
  public static long modInverse(final long x, final long y) {
    if (x == 1) return 1;

    long a = 1;
    long b = 0;
    long c = x;

    long u = 0;
    long v = 1;
    long w = y;
    
    for(;;) {
      {
        final long q = w/c;
        w -= q*c;
        u -= q*a;
        if (w == 1) return u > 0? u: u + y;
        v -= q*b;
      }
      {
        final long q = c/w;
        c -= q*w;
        a -= q*u;
        if (c == 1) return a > 0? a: a + y;
        b -= q*v;
      }
    }
  }
}
