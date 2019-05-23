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

package org.apache.hadoop.yarn.submarine.common.resource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Almost the same logic as UnitsConversionUtil[YARN-4081]. If the dependencies
 * are upgraded to hadoop 3.*, this class can be replaced.
 */
public final class UnitsConversionUtil {

  private UnitsConversionUtil() {}

  /**
   * Helper class for encapsulating conversion values.
   */
  public static class Converter {
    private long numerator;
    private long denominator;

    Converter(long n, long d) {
      this.numerator = n;
      this.denominator = d;
    }
  }

  private static final String[] UNITS = {"p", "n", "u", "m", "", "k", "M", "G",
      "T", "P", "Ki", "Mi", "Gi", "Ti", "Pi"};
  private static final List<String> SORTED_UNITS = Arrays.asList(UNITS);
  public static final Set<String> KNOWN_UNITS = createKnownUnitsSet();
  private static final Converter PICO =
      new Converter(1L, 1000L * 1000L * 1000L * 1000L);
  private static final Converter NANO =
      new Converter(1L, 1000L * 1000L * 1000L);
  private static final Converter MICRO = new Converter(1L, 1000L * 1000L);
  private static final Converter MILLI = new Converter(1L, 1000L);
  private static final Converter BASE = new Converter(1L, 1L);
  private static final Converter KILO = new Converter(1000L, 1L);
  private static final Converter MEGA = new Converter(1000L * 1000L, 1L);
  private static final Converter GIGA =
      new Converter(1000L * 1000L * 1000L, 1L);
  private static final Converter TERA =
      new Converter(1000L * 1000L * 1000L * 1000L, 1L);
  private static final Converter PETA =
      new Converter(1000L * 1000L * 1000L * 1000L * 1000L, 1L);

  private static final Converter KILO_BINARY = new Converter(1024L, 1L);
  private static final Converter MEGA_BINARY = new Converter(1024L * 1024L, 1L);
  private static final Converter GIGA_BINARY =
      new Converter(1024L * 1024L * 1024L, 1L);
  private static final Converter TERA_BINARY =
      new Converter(1024L * 1024L * 1024L * 1024L, 1L);
  private static final Converter PETA_BINARY =
      new Converter(1024L * 1024L * 1024L * 1024L * 1024L, 1L);

  private static Set<String> createKnownUnitsSet() {
    Set<String> ret = new HashSet<>();
    ret.addAll(Arrays.asList(UNITS));
    return ret;
  }

  private static Converter getConverter(String unit) {
    switch (unit) {
    case "p":
      return PICO;
    case "n":
      return NANO;
    case "u":
      return MICRO;
    case "m":
      return MILLI;
    case "":
      return BASE;
    case "k":
      return KILO;
    case "M":
      return MEGA;
    case "G":
      return GIGA;
    case "T":
      return TERA;
    case "P":
      return PETA;
    case "Ki":
      return KILO_BINARY;
    case "Mi":
      return MEGA_BINARY;
    case "Gi":
      return GIGA_BINARY;
    case "Ti":
      return TERA_BINARY;
    case "Pi":
      return PETA_BINARY;
    default:
      throw new IllegalArgumentException(
          "Unknown unit '" + unit + "'. Known units are " + KNOWN_UNITS);
    }
  }

  /**
   * Converts a value from one unit to another. Supported units can be obtained
   * by inspecting the KNOWN_UNITS set.
   *
   * @param fromUnit  the unit of the from value
   * @param toUnit    the target unit
   * @param fromValue the value you wish to convert
   * @return the value in toUnit
   */
  public static long convert(String fromUnit, String toUnit, long fromValue) {
    if (toUnit == null || fromUnit == null) {
      throw new IllegalArgumentException("One or more arguments are null");
    }

    if (fromUnit.equals(toUnit)) {
      return fromValue;
    }
    Converter fc = getConverter(fromUnit);
    Converter tc = getConverter(toUnit);
    long numerator = fc.numerator * tc.denominator;
    long denominator = fc.denominator * tc.numerator;
    long numeratorMultiplierLimit = Long.MAX_VALUE / numerator;
    if (numerator < denominator) {
      if (numeratorMultiplierLimit < fromValue) {
        String overflowMsg =
            "Converting " + fromValue + " from '" + fromUnit + "' to '" + toUnit
                + "' will result in an overflow of Long";
        throw new IllegalArgumentException(overflowMsg);
      }
      return (fromValue * numerator) / denominator;
    }
    if (numeratorMultiplierLimit > fromValue) {
      return (numerator * fromValue) / denominator;
    }
    long tmp = numerator / denominator;
    if ((Long.MAX_VALUE / tmp) < fromValue) {
      String overflowMsg =
          "Converting " + fromValue + " from '" + fromUnit + "' to '" + toUnit
              + "' will result in an overflow of Long";
      throw new IllegalArgumentException(overflowMsg);
    }
    return fromValue * tmp;
  }

}
