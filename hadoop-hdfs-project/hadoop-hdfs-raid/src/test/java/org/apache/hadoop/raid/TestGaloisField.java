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
package org.apache.hadoop.raid;

import java.util.Random;
import java.util.Set;
import java.util.HashSet;

import junit.framework.TestCase;

public class TestGaloisField extends TestCase {

  final int TEST_TIMES = 10000;
  final Random RAND = new Random();
  final static GaloisField GF = GaloisField.getInstance();

  private int randGF() {
    return 0x000000FF & RAND.nextInt(GF.getFieldSize());
  }
  private int[] randGFPoly(int len) {
    int[] result = new int[len];
    for (int i = 0; i < len; i++) {
      result[i] = randGF();
    }
    return result;
  }

  public void testGetInstance() {
    GaloisField gf1 = GaloisField.getInstance(256, 285);
    GaloisField gf2 = GaloisField.getInstance();
    GaloisField gf3 = GaloisField.getInstance(128, 137);
    GaloisField gf4 = GaloisField.getInstance(128, 137);
    GaloisField gf5 = GaloisField.getInstance(512, 529);
    GaloisField gf6 = GaloisField.getInstance(512, 529);
    assertTrue(gf1 == gf2);
    assertTrue(gf3 == gf4);
    assertTrue(gf5 == gf6);
  }

  public void testDistributivity() {
    for (int i = 0; i < TEST_TIMES; i++) {
      int a = RAND.nextInt(GF.getFieldSize());
      int b = RAND.nextInt(GF.getFieldSize());
      int c = RAND.nextInt(GF.getFieldSize());
      int result1 = GF.multiply(a, GF.add(b, c));
      int result2 = GF.add(GF.multiply(a, b), GF.multiply(a, c));
      assertTrue("Distributivity test #" + i + " failed: " + a + ", " + b + ", "
                 + c, result1 == result2);
    }
  }

  public void testDevision() {
    for (int i = 0; i < TEST_TIMES; i++) {
      int a = RAND.nextInt(GF.getFieldSize());
      int b = RAND.nextInt(GF.getFieldSize());
      if (b == 0) {
        continue;
      }
      int c = GF.divide(a, b);
      assertTrue("Division test #" + i + " failed: " + a + "/" + b + " = " + c,
                 a == GF.multiply(c, b));
    }
  }

  public void testPower() {
    for (int i = 0; i < TEST_TIMES; i++) {
      int a = randGF();
      int n = RAND.nextInt(10);
      int result1 = GF.power(a, n);
      int result2 = 1;
      for (int j = 0; j < n; j++) {
        result2 = GF.multiply(result2, a);
      }
      assert(result1 == result2);
    }
  }

  public void testPolynomialDistributivity() {
    final int TEST_LEN = 15;
    for (int i = 0; i < TEST_TIMES; i++) {
      int[] a = randGFPoly(RAND.nextInt(TEST_LEN - 1) + 1);
      int[] b = randGFPoly(RAND.nextInt(TEST_LEN - 1) + 1);
      int[] c = randGFPoly(RAND.nextInt(TEST_LEN - 1) + 1);
      int[] result1 = GF.multiply(a, GF.add(b, c));
      int[] result2 = GF.add(GF.multiply(a, b), GF.multiply(a, c));
      assertTrue("Distributivity test on polynomials failed",
                 java.util.Arrays.equals(result1, result2));
    }
  }

  public void testSubstitute() {
    final int TEST_LEN = 15;
    for (int i = 0; i < TEST_TIMES; i++) {
      int[] a = randGFPoly(RAND.nextInt(TEST_LEN - 1) + 1);
      int[] b = randGFPoly(RAND.nextInt(TEST_LEN - 1) + 1);
      int[] c = randGFPoly(RAND.nextInt(TEST_LEN - 1) + 1);
      int x = randGF();
      // (a * b * c)(x)
      int result1 = GF.substitute(GF.multiply(GF.multiply(a, b), c), x);
      // a(x) * b(x) * c(x)
      int result2 =
        GF.multiply(GF.multiply(GF.substitute(a, x), GF.substitute(b, x)),
                    GF.substitute(c, x));
      assertTrue("Substitute test on polynomial failed",
                 result1 == result2);
    }
  }

  public void testSolveVandermondeSystem() {
    final int TEST_LEN = 15;
    for (int i = 0; i < TEST_TIMES; i++) {
      int[] z = randGFPoly(RAND.nextInt(TEST_LEN - 1) + 1);
      // generate distinct values for x
      int[] x = new int[z.length];
      Set<Integer> s = new HashSet<Integer>();
      while (s.size() != z.length) {
        s.add(randGF());
      }
      int t = 0;
      for (int v : s) {
        x[t++] = v;
      }
      // compute the output for the Vandermonde system
      int[] y = new int[x.length];
      for (int j = 0; j < x.length; j++) {
        y[j] = 0;
        for (int k = 0; k < x.length; k++) {
          //y[j] = y[j] + z[k] * pow(x[k], j);
          y[j] = GF.add(y[j], GF.multiply(GF.power(x[k], j), z[k]));
        }
      }

      GF.solveVandermondeSystem(x, y);
      assertTrue("Solving Vandermonde system failed",
                 java.util.Arrays.equals(y, z));
    }
  }

  public void testRemainder() {
    final int TEST_LEN = 15;
    for (int i = 0; i < TEST_TIMES; i++) {
      int[] quotient = null;
      int[] divisor = null;
      int[] remainder = null;
      int[] dividend = null;
      while (true) {
        quotient = randGFPoly(RAND.nextInt(TEST_LEN - 3) + 3);
        divisor = randGFPoly(RAND.nextInt(quotient.length - 2) + 2);
        remainder = randGFPoly(RAND.nextInt(divisor.length - 1) + 1);
        dividend = GF.add(remainder, GF.multiply(quotient, divisor));
        if (quotient[quotient.length - 1] != 0 &&
            divisor[divisor.length - 1] != 0 &&
            remainder[remainder.length - 1] != 0) {
          // make sure all the leading terms are not zero
          break;
        }
      }
      GF.remainder(dividend, divisor);
      for (int j = 0; j < remainder.length; j++) {
        assertTrue("Distributivity test on polynomials failed",
                   dividend[j] == remainder[j]);
      }
    }
  }
}
