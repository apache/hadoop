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

import org.junit.Assert;
import org.junit.Test;

public class TestUnitsConversionUtil {

  @Test
  public void testUnitsConversion() {
    int value = 5;
    String fromUnit = "";
    long test = value;
    Assert.assertEquals("pico test failed",
        value * 1000L * 1000L * 1000L * 1000L,
        UnitsConversionUtil.convert(fromUnit, "p", test));
    Assert.assertEquals("nano test failed",
        value * 1000L * 1000L * 1000L,
        UnitsConversionUtil.convert(fromUnit, "n", test));
    Assert
        .assertEquals("micro test failed", value * 1000L * 1000L,
            UnitsConversionUtil.convert(fromUnit, "u", test));
    Assert.assertEquals("milli test failed", value * 1000L,
        UnitsConversionUtil.convert(fromUnit, "m", test));

    test = value * 1000L * 1000L * 1000L * 1000L * 1000L;
    fromUnit = "";
    Assert.assertEquals("kilo test failed", test / 1000L,
        UnitsConversionUtil.convert(fromUnit, "k", test));

    Assert
        .assertEquals("mega test failed", test / (1000L * 1000L),
            UnitsConversionUtil.convert(fromUnit, "M", test));
    Assert.assertEquals("giga test failed",
        test / (1000L * 1000L * 1000L),
        UnitsConversionUtil.convert(fromUnit, "G", test));
    Assert.assertEquals("tera test failed",
        test / (1000L * 1000L * 1000L * 1000L),
        UnitsConversionUtil.convert(fromUnit, "T", test));
    Assert.assertEquals("peta test failed",
        test / (1000L * 1000L * 1000L * 1000L * 1000L),
        UnitsConversionUtil.convert(fromUnit, "P", test));

    Assert.assertEquals("nano to pico test failed", value * 1000L,
        UnitsConversionUtil.convert("n", "p", value));

    Assert.assertEquals("mega to giga test failed", value,
        UnitsConversionUtil.convert("M", "G", value * 1000L));

    Assert.assertEquals("Mi to Gi test failed", value,
        UnitsConversionUtil.convert("Mi", "Gi", value * 1024L));

    Assert.assertEquals("Mi to Ki test failed", value * 1024,
        UnitsConversionUtil.convert("Mi", "Ki", value));

    Assert.assertEquals("Ki to base units test failed", 5 * 1024,
        UnitsConversionUtil.convert("Ki", "", 5));

    Assert.assertEquals("Mi to k test failed", 1073741,
        UnitsConversionUtil.convert("Mi", "k", 1024));

    Assert.assertEquals("M to Mi test failed", 953,
        UnitsConversionUtil.convert("M", "Mi", 1000));
  }

  @Test
  public void testOverflow() {
    long test = 5 * 1000L * 1000L * 1000L * 1000L * 1000L;
    try {
      UnitsConversionUtil.convert("P", "p", test);
      Assert.fail("this operation should result in an overflow");
    } catch (IllegalArgumentException ie) {
      ; // do nothing
    }
    try {
      UnitsConversionUtil.convert("m", "p", Long.MAX_VALUE - 1);
      Assert.fail("this operation should result in an overflow");
    } catch (IllegalArgumentException ie) {
      ; // do nothing
    }
  }

  @Test
  public void testCompare() {
    String unitA = "P";
    long valueA = 1;
    String unitB = "p";
    long valueB = 2;
    Assert.assertEquals(1,
        UnitsConversionUtil.compare(unitA, valueA, unitB, valueB));
    Assert.assertEquals(-1,
        UnitsConversionUtil.compare(unitB, valueB, unitA, valueA));
    Assert.assertEquals(0,
        UnitsConversionUtil.compare(unitA, valueA, unitA, valueA));
    Assert.assertEquals(-1,
        UnitsConversionUtil.compare(unitA, valueA, unitA, valueB));
    Assert.assertEquals(1,
        UnitsConversionUtil.compare(unitA, valueB, unitA, valueA));

    unitB = "T";
    Assert.assertEquals(1,
        UnitsConversionUtil.compare(unitA, valueA, unitB, valueB));
    Assert.assertEquals(-1,
        UnitsConversionUtil.compare(unitB, valueB, unitA, valueA));
    Assert.assertEquals(0,
        UnitsConversionUtil.compare(unitA, valueA, unitB, 1000L));

    unitA = "p";
    unitB = "n";
    Assert.assertEquals(-1,
        UnitsConversionUtil.compare(unitA, valueA, unitB, valueB));
    Assert.assertEquals(1,
        UnitsConversionUtil.compare(unitB, valueB, unitA, valueA));
    Assert.assertEquals(0,
        UnitsConversionUtil.compare(unitA, 1000L, unitB, valueA));

  }
}
