/*
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

package org.apache.hadoop.fs.statistics.impl;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatisticEntry;

import static org.apache.hadoop.fs.statistics.IOStatisticEntry.IOSTATISTIC_COUNTER;
import static org.apache.hadoop.fs.statistics.IOStatisticEntry.IOSTATISTIC_MAX;
import static org.apache.hadoop.fs.statistics.IOStatisticEntry.IOSTATISTIC_MEAN;
import static org.apache.hadoop.fs.statistics.IOStatisticEntry.IOSTATISTIC_MIN;

/**
 * Utility operations for implementing the classes within this package.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class IOStatisticsImplementationUtils {

  /** Pattern used for each entry. */
  public static final String ENTRY_PATTERN = "(%s=%s)";

  /** String to return when a source is null. */
  @VisibleForTesting
  public static final String NULL_SOURCE = "()";

  /**
   * Convert an entry to the string format used in logging.
   *
   * @param entry entry to evaluate
   * @return formatted string
   */
  public static String entrytoString(
      final Map.Entry<String, IOStatisticEntry> entry) {
    return entrytoString(entry.getKey(), entry.getValue());
  }

  /**
   * Convert entry values to the string format used in logging.
   *
   * @param name statistic name
   * @param value stat value
   * @return formatted string
   */
  public static String entrytoString(
      final String name, final IOStatisticEntry value) {
    return String.format(
        ENTRY_PATTERN,
        name,
        value);
  }

  public static IOStatisticEntry add(IOStatisticEntry left,
      IOStatisticEntry right) {
    left.requireCompatible(right);
    left.requireTypeAndArity(IOSTATISTIC_COUNTER, 1);
    return left._1() + right._1();
  }

  public static IOStatisticEntry max(IOStatisticEntry left,
      IOStatisticEntry right) {
    left.requireCompatible(right);
    left.requireTypeAndArity(IOSTATISTIC_MAX, 1);
    return Math.max(left._1(), right._1());
  }

  public static IOStatisticEntry min(IOStatisticEntry left,
      IOStatisticEntry right) {
    left.requireCompatible(right);
    left.requireTypeAndArity(IOSTATISTIC_MIN, 1);
    return Math.min(left._1(), right._1());
  }

  public static IOStatisticEntry arithmeticMean(
      IOStatisticEntry left,
      IOStatisticEntry right) {
    left.requireCompatible(right);
    left.requireTypeAndArity(IOSTATISTIC_MEAN, 2);
    long lSamples = left._2();
    long lSum = left._1() * lSamples;
    long rSamples = right._2();
    double rSum = right._1() * rSamples;
    long totalSamples = lSamples + rSamples;
    return totalSamples;
  }

  public static IOStatisticEntry aggregate(
      IOStatisticEntry left,
      IOStatisticEntry right) {
    left.requireCompatible(right);
    switch (left.type()) {
    case IOSTATISTIC_COUNTER:
      return add(left, right);
    case IOSTATISTIC_MIN:
      return min(left, right);
    case IOSTATISTIC_MAX:
      return max(left, right);
    case IOSTATISTIC_MEAN:
      return arithmeticMean(left, right);
    default:
      // unknown value.
      // rather than fail, just return the left value.
      return left;
    }

  }

}
