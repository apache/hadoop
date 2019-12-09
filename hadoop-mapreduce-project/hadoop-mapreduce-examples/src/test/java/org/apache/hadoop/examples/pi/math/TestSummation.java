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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.examples.pi.Container;
import org.apache.hadoop.examples.pi.Util;
import org.apache.hadoop.examples.pi.Util.Timer;
import org.apache.hadoop.examples.pi.math.TestModular.Montgomery2;
import org.junit.Test;
import org.junit.Assert;

public class TestSummation {
  static final Random RANDOM = new Random();
  static final BigInteger TWO = BigInteger.valueOf(2);
  private static final double DOUBLE_DELTA = 0.000000001f;

  private static Summation2 newSummation(final long base, final long range, final long delta) {
    final ArithmeticProgression N = new ArithmeticProgression('n', base + 3,
        delta, base + 3 + range);
    final ArithmeticProgression E = new ArithmeticProgression('e', base + range,
        -delta, base);
    return new Summation2(N, E);
  }

  private static void runTestSubtract(Summation sigma, List<Summation> diff) {
//    Util.out.println("diff=" + diff);
    List<Container<Summation>> tmp = new ArrayList<Container<Summation>>(diff.size());
    for(Summation s : diff)
      tmp.add(s);
    final List<Summation> a = sigma.remainingTerms(tmp);
//    Util.out.println("a   =" + a);

    a.addAll(diff);
    for(Summation s : a)
      s.compute();

    final List<Summation> combined = Util.combine(a);
//    Util.out.println("combined=" + combined);
    Assert.assertEquals(1, combined.size());
    Assert.assertEquals(sigma, combined.get(0));
  }

  @Test
  public void testSubtract() {
    final Summation sigma = newSummation(3, 10000, 20);
    final int size = 10;
    final List<Summation> parts = Arrays.asList(sigma.partition(size));
    Collections.sort(parts);

    runTestSubtract(sigma, new ArrayList<Summation>());
    runTestSubtract(sigma, parts);

    for(int n = 1; n < size; n++) {
      for(int j = 0; j < 10; j++) {
        final List<Summation> diff = new ArrayList<Summation>(parts);
        for(int i = 0; i < n; i++)
          diff.remove(RANDOM.nextInt(diff.size()));
///        Collections.sort(diff);
        runTestSubtract(sigma, diff);
      }
    }
  }
  
  static class Summation2 extends Summation {
    Summation2(ArithmeticProgression N, ArithmeticProgression E) {
      super(N, E);
    }

    
    final Montgomery2 m2 = new Montgomery2();
    double compute_montgomery2() {
      long e = E.value;
      long n = N.value;
      double s = 0;
      for(; e > E.limit; e += E.delta) {
        m2.set(n);
        s = Modular.addMod(s, m2.mod2(e)/(double)n);
        n += N.delta;
      }
      return s;
    }

    double compute_modBigInteger() {
      long e = E.value;
      long n = N.value;
      double s = 0;
      for(; e > E.limit; e += E.delta) {
        s = Modular.addMod(s, TestModular.modBigInteger(e, n)/(double)n);
        n += N.delta;
      }
      return s;
    }
  
    double compute_modPow() {
      long e = E.value;
      long n = N.value;
      double s = 0;
      for(; e > E.limit; e += E.delta) {
        s = Modular.addMod(s,
            TWO.modPow(BigInteger.valueOf(e), BigInteger.valueOf(n))
                .doubleValue() / n);
        n += N.delta;
      }
      return s;
    }
  }

  private static void computeBenchmarks(final Summation2 sigma) {
    final Timer t = new Timer(false);
    t.tick("sigma=" + sigma);
    final double value = sigma.compute();
    t.tick("compute=" + value);
    Assert.assertEquals(value, sigma.compute_modular(), DOUBLE_DELTA);
    t.tick("compute_modular");
    Assert.assertEquals(value, sigma.compute_montgomery(), DOUBLE_DELTA);
    t.tick("compute_montgomery");
    Assert.assertEquals(value, sigma.compute_montgomery2(), DOUBLE_DELTA);
    t.tick("compute_montgomery2");

    Assert.assertEquals(value, sigma.compute_modBigInteger(), DOUBLE_DELTA);
    t.tick("compute_modBigInteger");
    Assert.assertEquals(value, sigma.compute_modPow(), DOUBLE_DELTA);
    t.tick("compute_modPow");
  }

  /** Benchmarks */
  public static void main(String[] args) {
    final long delta = 1L << 4;
    final long range = 1L << 20;
    for(int i = 20; i < 40; i += 2) 
      computeBenchmarks(newSummation(1L << i, range, delta));
  }
}
