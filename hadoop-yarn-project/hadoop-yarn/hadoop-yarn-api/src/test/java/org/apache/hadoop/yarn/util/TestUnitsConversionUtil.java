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

import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestUnitsConversionUtil {

  @Test
  public void testUnitsConversion() {
    int value = 5;
    String fromUnit = "";
    Long test = Long.valueOf(value);
    Assert.assertEquals("pico test failed",
        Long.valueOf(value * 1000l * 1000l * 1000l * 1000l),
        UnitsConversionUtil.convert(fromUnit, "p", test));
    Assert.assertEquals("nano test failed",
        Long.valueOf(value * 1000l * 1000l * 1000l),
        UnitsConversionUtil.convert(fromUnit, "n", test));
    Assert
        .assertEquals("micro test failed", Long.valueOf(value * 1000l * 1000l),
            UnitsConversionUtil.convert(fromUnit, "u", test));
    Assert.assertEquals("milli test failed", Long.valueOf(value * 1000l),
        UnitsConversionUtil.convert(fromUnit, "m", test));

    test = Long.valueOf(value * 1000l * 1000l * 1000l * 1000l * 1000l);
    fromUnit = "";
    Assert.assertEquals("kilo test failed", Long.valueOf(test / 1000l),
        UnitsConversionUtil.convert(fromUnit, "k", test));
    Assert
        .assertEquals("mega test failed", Long.valueOf(test / (1000l * 1000l)),
            UnitsConversionUtil.convert(fromUnit, "M", test));
    Assert.assertEquals("giga test failed",
        Long.valueOf(test / (1000l * 1000l * 1000l)),
        UnitsConversionUtil.convert(fromUnit, "G", test));
    Assert.assertEquals("tera test failed",
        Long.valueOf(test / (1000l * 1000l * 1000l * 1000l)),
        UnitsConversionUtil.convert(fromUnit, "T", test));
    Assert.assertEquals("peta test failed",
        Long.valueOf(test / (1000l * 1000l * 1000l * 1000l * 1000l)),
        UnitsConversionUtil.convert(fromUnit, "P", test));

    Assert.assertEquals("nano to pico test failed", Long.valueOf(value * 1000l),
        UnitsConversionUtil.convert("n", "p", Long.valueOf(value)));

    Assert.assertEquals("mega to giga test failed", Long.valueOf(value),
        UnitsConversionUtil.convert("M", "G", Long.valueOf(value * 1000l)));
  }

  @Test
  public void testOverflow() {
    Long test = Long.valueOf(5 * 1000l * 1000l * 1000l * 1000l * 1000l);
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
    Long valueA = Long.valueOf(1);
    String unitB = "p";
    Long valueB = Long.valueOf(2);
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
        UnitsConversionUtil.compare(unitA, valueA, unitB, 1000l));

    unitA = "p";
    unitB = "n";
    Assert.assertEquals(-1,
        UnitsConversionUtil.compare(unitA, valueA, unitB, valueB));
    Assert.assertEquals(1,
        UnitsConversionUtil.compare(unitB, valueB, unitA, valueA));
    Assert.assertEquals(0,
        UnitsConversionUtil.compare(unitA, 1000l, unitB, valueA));

  }
}
