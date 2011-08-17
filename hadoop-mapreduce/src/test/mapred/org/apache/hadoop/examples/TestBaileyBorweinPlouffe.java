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
package org.apache.hadoop.examples;

import java.math.BigInteger;

/** Tests for BaileyBorweinPlouffe */
public class TestBaileyBorweinPlouffe extends junit.framework.TestCase {

  public void testMod() {
    final BigInteger TWO = BigInteger.ONE.add(BigInteger.ONE);
    for(long n = 3; n < 100; n++) {
      for (long e = 1; e < 100; e++) {
        final long r = TWO.modPow(
            BigInteger.valueOf(e), BigInteger.valueOf(n)).longValue();
        assertEquals("e=" + e + ", n=" + n, r, BaileyBorweinPlouffe.mod(e, n));
      }
    }
  }

  public void testHexDigit() {
    final long[] answers = {0x43F6, 0xA308, 0x29B7, 0x49F1, 0x8AC8, 0x35EA};
    long d = 1;
    for(int i = 0; i < answers.length; i++) {
      assertEquals("d=" + d, answers[i], BaileyBorweinPlouffe.hexDigits(d));
      d *= 10;
    }

    assertEquals(0x243FL, BaileyBorweinPlouffe.hexDigits(0));
 }
}