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
import org.junit.Test;
import org.junit.Assert;

public class TestLongLong {

  static final Random RAN = new Random();
  static final long MASK = (1L << (LongLong.SIZE >> 1)) - 1;

  static long nextPositiveLong() {
    return RAN.nextLong() & MASK;
  }

  static void verifyMultiplication(long a, long b) {
    final LongLong ll = LongLong.multiplication(new LongLong(), a, b);
    final BigInteger bi = BigInteger.valueOf(a).multiply(BigInteger.valueOf(b));

    final String s = String.format(
        "\na = %x\nb = %x\nll= " + ll + "\nbi= " + bi.toString(16) + "\n", a,
        b);
    //System.out.println(s);
    Assert.assertEquals(s, bi, ll.toBigInteger());
  }

  @Test
  public void testMultiplication() {
    for(int i = 0; i < 100; i++) {
      final long a = nextPositiveLong();
      final long b = nextPositiveLong();
      verifyMultiplication(a, b);
    }
    final long max = Long.MAX_VALUE & MASK;
    verifyMultiplication(max, max);
  }

  @Test
  public void testRightShift() {
    for(int i = 0; i < 1000; i++) {
      final long a = nextPositiveLong();
      final long b = nextPositiveLong();
      verifyRightShift(a, b);
    }
  }

  private static void verifyRightShift(long a, long b) {
    final LongLong ll = new LongLong().set(a, b);
    final BigInteger bi = ll.toBigInteger();

    final String s = String.format(
        "\na = %x\nb = %x\nll= " + ll + "\nbi= " + bi.toString(16) + "\n", a,
        b);
    Assert.assertEquals(s, bi, ll.toBigInteger());

    for (int i = 0; i < LongLong.SIZE >> 1; i++) {
      final long result = ll.shiftRight(i) & MASK;
      final long expected = bi.shiftRight(i).longValue() & MASK;
      Assert.assertEquals(s, expected, result);
    }
  }
}
