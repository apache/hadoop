/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

public class TestBytes extends TestCase {
  public void testNullHashCode() {
    byte [] b = null;
    Exception ee = null;
    try {
      Bytes.hashCode(b);
    } catch (Exception e) {
      ee = e;
    }
    assertNotNull(ee);
  }

  public void testSplit() throws Exception {
    byte [] lowest = Bytes.toBytes("AAA");
    byte [] middle = Bytes.toBytes("CCC");
    byte [] highest = Bytes.toBytes("EEE");
    byte [][] parts = Bytes.split(lowest, highest, 1);
    for (int i = 0; i < parts.length; i++) {
      System.out.println(Bytes.toString(parts[i]));
    }
    assertEquals(3, parts.length);
    assertTrue(Bytes.equals(parts[1], middle));
    // Now divide into three parts.  Change highest so split is even.
    highest = Bytes.toBytes("DDD");
    parts = Bytes.split(lowest, highest, 2);
    for (int i = 0; i < parts.length; i++) {
      System.out.println(Bytes.toString(parts[i]));
    }
    assertEquals(4, parts.length);
    // Assert that 3rd part is 'CCC'.
    assertTrue(Bytes.equals(parts[2], middle));
  }

  public void testSplit2() throws Exception {
    // More split tests.
    byte [] lowest = Bytes.toBytes("http://A");
    byte [] highest = Bytes.toBytes("http://z");
    byte [] middle = Bytes.toBytes("http://]");
    byte [][] parts = Bytes.split(lowest, highest, 1);
    for (int i = 0; i < parts.length; i++) {
      System.out.println(Bytes.toString(parts[i]));
    }
    assertEquals(3, parts.length);
    assertTrue(Bytes.equals(parts[1], middle));
  }

  public void testSplit3() throws Exception {
    // Test invalid split cases
    byte [] low = { 1, 1, 1 };
    byte [] high = { 1, 1, 3 };

    // If swapped, should throw IAE
    try {
      Bytes.split(high, low, 1);
      assertTrue("Should not be able to split if low > high", false);
    } catch(IllegalArgumentException iae) {
      // Correct
    }

    // Single split should work
    byte [][] parts = Bytes.split(low, high, 1);
    for (int i = 0; i < parts.length; i++) {
      System.out.println("" + i + " -> " + Bytes.toStringBinary(parts[i]));
    }
    assertTrue("Returned split should have 3 parts but has " + parts.length, parts.length == 3);

    // If split more than once, this should fail
    parts = Bytes.split(low, high, 2);
    assertTrue("Returned split but should have failed", parts == null);
  }

  public void testToLong() throws Exception {
    long [] longs = {-1l, 123l, 122232323232l};
    for (int i = 0; i < longs.length; i++) {
      byte [] b = Bytes.toBytes(longs[i]);
      assertEquals(longs[i], Bytes.toLong(b));
    }
  }

  public void testToFloat() throws Exception {
    float [] floats = {-1f, 123.123f, Float.MAX_VALUE};
    for (int i = 0; i < floats.length; i++) {
      byte [] b = Bytes.toBytes(floats[i]);
      assertEquals(floats[i], Bytes.toFloat(b));
    }
  }

  public void testToDouble() throws Exception {
    double [] doubles = {Double.MIN_VALUE, Double.MAX_VALUE};
    for (int i = 0; i < doubles.length; i++) {
      byte [] b = Bytes.toBytes(doubles[i]);
      assertEquals(doubles[i], Bytes.toDouble(b));
    }
  }

  public void testBinarySearch() throws Exception {
    byte [][] arr = {
        {1},
        {3},
        {5},
        {7},
        {9},
        {11},
        {13},
        {15},
    };
    byte [] key1 = {3,1};
    byte [] key2 = {4,9};
    byte [] key2_2 = {4};
    byte [] key3 = {5,11};

    assertEquals(1, Bytes.binarySearch(arr, key1, 0, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(0, Bytes.binarySearch(arr, key1, 1, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(-(2+1), Arrays.binarySearch(arr, key2_2,
      Bytes.BYTES_COMPARATOR));
    assertEquals(-(2+1), Bytes.binarySearch(arr, key2, 0, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(4, Bytes.binarySearch(arr, key2, 1, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(2, Bytes.binarySearch(arr, key3, 0, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(5, Bytes.binarySearch(arr, key3, 1, 1,
      Bytes.BYTES_RAWCOMPARATOR));
  }
  
  public void testStartsWith() {
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("h")));
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("")));
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("hello")));
    assertFalse(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("helloworld")));
    assertFalse(Bytes.startsWith(Bytes.toBytes(""), Bytes.toBytes("hello")));
  }

  public void testIncrementBytes() throws IOException {

    assertTrue(checkTestIncrementBytes(10, 1));
    assertTrue(checkTestIncrementBytes(12, 123435445));
    assertTrue(checkTestIncrementBytes(124634654, 1));
    assertTrue(checkTestIncrementBytes(10005460, 5005645));
    assertTrue(checkTestIncrementBytes(1, -1));
    assertTrue(checkTestIncrementBytes(10, -1));
    assertTrue(checkTestIncrementBytes(10, -5));
    assertTrue(checkTestIncrementBytes(1005435000, -5));
    assertTrue(checkTestIncrementBytes(10, -43657655));
    assertTrue(checkTestIncrementBytes(-1, 1));
    assertTrue(checkTestIncrementBytes(-26, 5034520));
    assertTrue(checkTestIncrementBytes(-10657200, 5));
    assertTrue(checkTestIncrementBytes(-12343250, 45376475));
    assertTrue(checkTestIncrementBytes(-10, -5));
    assertTrue(checkTestIncrementBytes(-12343250, -5));
    assertTrue(checkTestIncrementBytes(-12, -34565445));
    assertTrue(checkTestIncrementBytes(-1546543452, -34565445));
  }

  private static boolean checkTestIncrementBytes(long val, long amount)
  throws IOException {
    byte[] value = Bytes.toBytes(val);
    byte [] testValue = {-1, -1, -1, -1, -1, -1, -1, -1};
    if (value[0] > 0) {
      testValue = new byte[Bytes.SIZEOF_LONG];
    }
    System.arraycopy(value, 0, testValue, testValue.length - value.length,
        value.length);

    long incrementResult = Bytes.toLong(Bytes.incrementBytes(value, amount));

    return (Bytes.toLong(testValue) + amount) == incrementResult;
  }
}
