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
import java.util.Random;

import org.apache.hadoop.examples.pi.Util.Timer;
import org.junit.Assert;
import org.junit.Test;

public class TestModular{
  private static final Random RANDOM = new Random();
  private static final BigInteger TWO = BigInteger.valueOf(2);


  static final int DIV_VALID_BIT = 32;
  static final long DIV_LIMIT = 1L << DIV_VALID_BIT;

  // return r/n for n > r > 0
  static long div(long sum, long r, long n) {
    long q = 0;
    int i = DIV_VALID_BIT - 1;
    for(r <<= 1; r < n; r <<= 1) i--;
//System.out.printf("  r=%d, n=%d, q=%d\n", r, n, q);

    for(; i >= 0 ;) {
      r -= n;
      q |= (1L << i);
      if (r <= 0) break;
      for(; r < n; r <<= 1) i--;
//System.out.printf("  r=%d, n=%d, q=%d\n", r, n, q);
    }

    sum += q;
    return sum < DIV_LIMIT? sum: sum - DIV_LIMIT;
  }

  @Test
  public void testDiv() {
    for(long n = 2; n < 100; n++)
      for(long r = 1; r < n; r++) {
        final long a = div(0, r, n);
        final long b = (long)((r*1.0/n) * (1L << DIV_VALID_BIT));
        final String s = String.format("r=%d, n=%d, a=%X, b=%X", r, n, a, b);
        Assert.assertEquals(s, b, a);
      }
  }

  static long[][][] generateRN(int nsize, int rsize) {
    final long[][][] rn = new long[nsize][][];

    for(int i = 0; i < rn.length; i++) {
      rn[i] = new long[rsize + 1][];
      long n = RANDOM.nextLong() & 0xFFFFFFFFFFFFFFFL;
      if (n <= 1) n = 0xFFFFFFFFFFFFFFFL - n;
      rn[i][0] = new long[]{n};
      final BigInteger N = BigInteger.valueOf(n);

      for(int j = 1; j < rn[i].length; j++) {
        long r = RANDOM.nextLong();
        if (r < 0) r = -r;
        if (r >= n) r %= n;
        final BigInteger R = BigInteger.valueOf(r);
        rn[i][j] = new long[]{r, R.multiply(R).mod(N).longValue()};
      }
    }
    return rn;
  }

  static long square_slow(long z, final long n) {
    long r = 0;
    for(long s = z; z > 0; z >>= 1) {
      if ((((int)z) & 1) == 1) {
        r += s;
        if (r >= n) r -= n;
      }

      s <<= 1;
      if (s >= n) s -= n;
    }
    return r;
  }

  //0 <= r < n < max/2
  static long square(long r, final long n, long r2p64) {
    if (r <= Modular.MAX_SQRT_LONG) {
      r *= r;
      if (r >= n) r %= n;
    } else {
      final int HALF = (63 - Long.numberOfLeadingZeros(n)) >> 1;
      final int FULL = HALF << 1;
      final long ONES = (1 << HALF) - 1;

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
    return r;
  }

  static void squareBenchmarks() {
    final Timer t = new Timer(false);
    t.tick("squareBenchmarks(), MAX_SQRT=" + Modular.MAX_SQRT_LONG);

    final long[][][] rn = generateRN(1000, 1000);
    t.tick("generateRN");

    for(int i = 0; i < rn.length; i++) {
      final long n = rn[i][0][0];
      for(int j = 1; j < rn[i].length; j++) {
        final long r = rn[i][j][0];
        final long answer = rn[i][j][1];
        final long s = square_slow(r, n);
        if (s != answer) {
          Assert.assertEquals(
              "r=" + r + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("square_slow");

    for(int i = 0; i < rn.length; i++) {
      final long n = rn[i][0][0];
      long r2p64 = (0x4000000000000000L % n) << 1;
      if (r2p64 >= n) r2p64 -= n;
      for(int j = 1; j < rn[i].length; j++) {
        final long r = rn[i][j][0];
        final long answer = rn[i][j][1];
        final long s = square(r, n, r2p64);
        if (s != answer) {
          Assert.assertEquals(
              "r=" + r + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("square");

    for(int i = 0; i < rn.length; i++) {
      final long n = rn[i][0][0];
      final BigInteger N = BigInteger.valueOf(n);
      for(int j = 1; j < rn[i].length; j++) {
        final long r = rn[i][j][0];
        final long answer = rn[i][j][1];
        final BigInteger R = BigInteger.valueOf(r);
        final long s = R.multiply(R).mod(N).longValue();
        if (s != answer) {
          Assert.assertEquals(
              "r=" + r + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("R.multiply(R).mod(N)");

    for(int i = 0; i < rn.length; i++) {
      final long n = rn[i][0][0];
      final BigInteger N = BigInteger.valueOf(n);
      for(int j = 1; j < rn[i].length; j++) {
        final long r = rn[i][j][0];
        final long answer = rn[i][j][1];
        final BigInteger R = BigInteger.valueOf(r);
        final long s = R.modPow(TWO, N).longValue();
        if (s != answer) {
          Assert.assertEquals(
              "r=" + r + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("R.modPow(TWO, N)");
  }

  static long[][][] generateEN(int nsize, int esize) {
    final long[][][] en = new long[nsize][][];

    for(int i = 0; i < en.length; i++) {
      en[i] = new long[esize + 1][];
      long n = (RANDOM.nextLong() & 0xFFFFFFFFFFFFFFFL) | 1L;
      if (n == 1) n = 3;
      en[i][0] = new long[]{n};
      final BigInteger N = BigInteger.valueOf(n);

      for(int j = 1; j < en[i].length; j++) {
        long e = RANDOM.nextLong();
        if (e < 0) e = -e;
        final BigInteger E = BigInteger.valueOf(e);
        en[i][j] = new long[]{e, TWO.modPow(E, N).longValue()};
      }
    }
    return en;
  }

  /** Compute $2^e \mod n$ for e > 0, n > 2 */
  static long modBigInteger(final long e, final long n) {
    long mask = (e & 0xFFFFFFFF00000000L) == 0 ? 0x00000000FFFFFFFFL
        : 0xFFFFFFFF00000000L;
    mask &= (e & 0xFFFF0000FFFF0000L & mask) == 0 ? 0x0000FFFF0000FFFFL
        : 0xFFFF0000FFFF0000L;
    mask &= (e & 0xFF00FF00FF00FF00L & mask) == 0 ? 0x00FF00FF00FF00FFL
        : 0xFF00FF00FF00FF00L;
    mask &= (e & 0xF0F0F0F0F0F0F0F0L & mask) == 0 ? 0x0F0F0F0F0F0F0F0FL
        : 0xF0F0F0F0F0F0F0F0L;
    mask &= (e & 0xCCCCCCCCCCCCCCCCL & mask) == 0 ? 0x3333333333333333L
        : 0xCCCCCCCCCCCCCCCCL;
    mask &= (e & 0xAAAAAAAAAAAAAAAAL & mask) == 0 ? 0x5555555555555555L
        : 0xAAAAAAAAAAAAAAAAL;

    final BigInteger N = BigInteger.valueOf(n);
    long r = 2;
    for (mask >>= 1; mask > 0; mask >>= 1) {
      if (r <= Modular.MAX_SQRT_LONG) {
        r *= r;
        if (r >= n) r %= n;
      } else {
        final BigInteger R = BigInteger.valueOf(r);
        r = R.multiply(R).mod(N).longValue();
      }

      if ((e & mask) != 0) {
        r <<= 1;
        if (r >= n) r -= n;
      }
    }
    return r;
  }

  static class Montgomery2 extends Montgomery {
    /** Compute 2^y mod N for N odd. */
    long mod2(final long y) {
      long r0 = R - N;
      long r1 = r0 << 1;
      if (r1 >= N) r1 -= N;

      for(long mask = Long.highestOneBit(y); mask > 0; mask >>>= 1) {
        if ((mask & y) == 0) {
          r1 = product.m(r0, r1);
          r0 = product.m(r0, r0);
        } else {
          r0 = product.m(r0, r1);
          r1 = product.m(r1, r1);
        }
      }
      return product.m(r0, 1);
    }
  }

  static void modBenchmarks() {
    final Timer t = new Timer(false);
    t.tick("modBenchmarks()");

    final long[][][] en = generateEN(10000, 10);
    t.tick("generateEN");

    for(int i = 0; i < en.length; i++) {
      final long n = en[i][0][0];
      for(int j = 1; j < en[i].length; j++) {
        final long e = en[i][j][0];
        final long answer = en[i][j][1];
        final long s = Modular.mod(e, n);
        if (s != answer) {
          Assert.assertEquals(
              "e=" + e + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("Modular.mod");

    final Montgomery2 m2 = new Montgomery2();
    for(int i = 0; i < en.length; i++) {
      final long n = en[i][0][0];
      m2.set(n);
      for(int j = 1; j < en[i].length; j++) {
        final long e = en[i][j][0];
        final long answer = en[i][j][1];
        final long s = m2.mod(e);
        if (s != answer) {
          Assert.assertEquals(
              "e=" + e + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("montgomery.mod");

    for(int i = 0; i < en.length; i++) {
      final long n = en[i][0][0];
      m2.set(n);
      for(int j = 1; j < en[i].length; j++) {
        final long e = en[i][j][0];
        final long answer = en[i][j][1];
        final long s = m2.mod2(e);
        if (s != answer) {
          Assert.assertEquals(
              "e=" + e + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("montgomery.mod2");

    for(int i = 0; i < en.length; i++) {
      final long n = en[i][0][0];
      final BigInteger N = BigInteger.valueOf(n);
      for(int j = 1; j < en[i].length; j++) {
        final long e = en[i][j][0];
        final long answer = en[i][j][1];
        final long s = TWO.modPow(BigInteger.valueOf(e), N).longValue();
        if (s != answer) {
          Assert.assertEquals(
              "e=" + e + ", n=" + n + ", answer=" + answer + " but s=" + s,
              answer, s);
        }
      }
    }
    t.tick("BigInteger.modPow(e, n)");
  }

  public static void main(String[] args) {
    squareBenchmarks();
    modBenchmarks();
  }
}