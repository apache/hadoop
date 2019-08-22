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

package org.apache.hadoop.yarn.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class to handle all test cases needed to verify basic unit conversion
 * scenarios.
 */
class TestUnitsConversionUtil {

  @Test
  void testUnitsConversion() {
    int value = 5;
    String fromUnit = "";
    long test = value;
    assertEquals(value * 1000L * 1000L * 1000L * 1000L,
        UnitsConversionUtil.convert(fromUnit, "p", test),
        "pico test failed");
    assertEquals(value * 1000L * 1000L * 1000L,
        UnitsConversionUtil.convert(fromUnit, "n", test),
        "nano test failed");
    assertEquals(value * 1000L * 1000L,
        UnitsConversionUtil.convert(fromUnit, "u", test),
        "micro test failed");
    assertEquals(value * 1000L,
        UnitsConversionUtil.convert(fromUnit, "m", test),
        "milli test failed");

    test = value * 1000L * 1000L * 1000L * 1000L * 1000L;
    fromUnit = "";
    assertEquals(test / 1000L,
        UnitsConversionUtil.convert(fromUnit, "k", test),
        "kilo test failed");

    assertEquals(test / (1000L * 1000L),
        UnitsConversionUtil.convert(fromUnit, "M", test),
        "mega test failed");
    assertEquals(test / (1000L * 1000L * 1000L),
        UnitsConversionUtil.convert(fromUnit, "G", test),
        "giga test failed");
    assertEquals(test / (1000L * 1000L * 1000L * 1000L),
        UnitsConversionUtil.convert(fromUnit, "T", test),
        "tera test failed");
    assertEquals(test / (1000L * 1000L * 1000L * 1000L * 1000L),
        UnitsConversionUtil.convert(fromUnit, "P", test),
        "peta test failed");

    assertEquals(value * 1000L,
        UnitsConversionUtil.convert("n", "p", value),
        "nano to pico test failed");

    assertEquals(value,
        UnitsConversionUtil.convert("M", "G", value * 1000L),
        "mega to giga test failed");

    assertEquals(value,
        UnitsConversionUtil.convert("Mi", "Gi", value * 1024L),
        "Mi to Gi test failed");

    assertEquals(value * 1024,
        UnitsConversionUtil.convert("Mi", "Ki", value),
        "Mi to Ki test failed");

    assertEquals(5 * 1024,
        UnitsConversionUtil.convert("Ki", "", 5),
        "Ki to base units test failed");

    assertEquals(1073741,
        UnitsConversionUtil.convert("Mi", "k", 1024),
        "Mi to k test failed");

    assertEquals(953,
        UnitsConversionUtil.convert("M", "Mi", 1000),
        "M to Mi test failed");
  }

  @Test
  void testOverflow() {
    long test = 5 * 1000L * 1000L * 1000L * 1000L * 1000L;
    try {
      UnitsConversionUtil.convert("P", "p", test);
      fail("this operation should result in an overflow");
    } catch (IllegalArgumentException ie) {
      // do nothing
    }
    try {
      UnitsConversionUtil.convert("m", "p", Long.MAX_VALUE - 1);
      fail("this operation should result in an overflow");
    } catch (IllegalArgumentException ie) {
      // do nothing
    }
  }

  @Test
  void testCompare() {
    String unitA = "P";
    long valueA = 1;
    String unitB = "p";
    long valueB = 2;
    assertEquals(1,
        UnitsConversionUtil.compare(unitA, valueA, unitB, valueB));
    assertEquals(-1,
        UnitsConversionUtil.compare(unitB, valueB, unitA, valueA));
    assertEquals(0,
        UnitsConversionUtil.compare(unitA, valueA, unitA, valueA));
    assertEquals(-1,
        UnitsConversionUtil.compare(unitA, valueA, unitA, valueB));
    assertEquals(1,
        UnitsConversionUtil.compare(unitA, valueB, unitA, valueA));

    unitB = "T";
    assertEquals(1,
        UnitsConversionUtil.compare(unitA, valueA, unitB, valueB));
    assertEquals(-1,
        UnitsConversionUtil.compare(unitB, valueB, unitA, valueA));
    assertEquals(0,
        UnitsConversionUtil.compare(unitA, valueA, unitB, 1000L));

    unitA = "p";
    unitB = "n";
    assertEquals(-1,
        UnitsConversionUtil.compare(unitA, valueA, unitB, valueB));
    assertEquals(1,
        UnitsConversionUtil.compare(unitB, valueB, unitA, valueA));
    assertEquals(0,
        UnitsConversionUtil.compare(unitA, 1000L, unitB, valueA));

  }
}
