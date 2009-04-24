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

import java.util.Arrays;

import junit.framework.TestCase;

public class TestBytes extends TestCase {
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
}