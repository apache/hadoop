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

/** Montgomery method.
 * 
 * References:
 * 
 * [1] Richard Crandall and Carl Pomerance.  Prime Numbers: A Computational 
 *     Perspective.  Springer-Verlag, 2001. 
 * 
 * [2] Peter Montgomery.  Modular multiplication without trial division.
 *     Math. Comp., 44:519-521, 1985.
 */
class Montgomery {
  protected final Product product = new Product();

  protected long N;
  protected long N_I;  // N'
  protected long R;
  protected long R_1;  // R - 1
  protected int s;

  /** Set the modular and initialize this object. */
  Montgomery set(long n) {
    if (n % 2 != 1)
      throw new IllegalArgumentException("n % 2 != 1, n=" + n);
    N = n;
    R = Long.highestOneBit(n) << 1;
    N_I = R - Modular.modInverse(N, R);
    R_1 = R - 1;
    s = Long.numberOfTrailingZeros(R);
    return this;
  }

  /** Compute 2^y mod N for N odd. */
  long mod(final long y) {
    long p = R - N; 
    long x = p << 1;
    if (x >= N) x -= N;
    
    for(long mask = Long.highestOneBit(y); mask > 0; mask >>>= 1) {
      p = product.m(p, p);
      if ((mask & y) != 0) p = product.m(p, x);
    }
    return product.m(p, 1);
  }

  class Product {
    private final LongLong x = new LongLong();
    private final LongLong xN_I = new LongLong();
    private final LongLong aN = new LongLong();

    long m(final long c, final long d) {
      LongLong.multiplication(x, c, d);
      // a = (x * N')&(R - 1) = ((x & R_1) * N') & R_1
      final long a = LongLong.multiplication(xN_I, x.and(R_1), N_I).and(R_1);
      LongLong.multiplication(aN, a, N);
      final long z = aN.plusEqual(x).shiftRight(s);
      return z < N? z: z - N;      
    }
  }
}