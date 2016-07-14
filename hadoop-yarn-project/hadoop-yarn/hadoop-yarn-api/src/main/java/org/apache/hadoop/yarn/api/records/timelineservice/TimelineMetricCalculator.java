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
package org.apache.hadoop.yarn.api.records.timelineservice;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * A calculator for timeline metrics.
 */
public final class TimelineMetricCalculator {

  private TimelineMetricCalculator() {
    // do nothing.
  }

  /**
   * Compare two not-null numbers.
   * @param n1 Number n1
   * @param n2 Number n2
   * @return 0 if n1 equals n2, a negative int if n1 is less than n2, a
   * positive int otherwise.
   */
  public static int compare(Number n1, Number n2) {
    if (n1 == null || n2 == null) {
      throw new YarnRuntimeException(
          "Number to be compared shouldn't be null.");
    }

    if (n1 instanceof Integer || n1 instanceof Long) {
      if (n1.longValue() == n2.longValue()) {
        return 0;
      } else {
        return (n1.longValue() < n2.longValue()) ? -1 : 1;
      }
    }

    if (n1 instanceof Float || n1 instanceof Double) {
      if (n1.doubleValue() == n2.doubleValue()) {
        return 0;
      } else {
        return (n1.doubleValue() < n2.doubleValue()) ? -1 : 1;
      }
    }

    // TODO throw warnings/exceptions for other types of number.
    throw new YarnRuntimeException("Unsupported types for number comparison: "
        + n1.getClass().getName() + ", " + n2.getClass().getName());
  }

  /**
   * Subtract operation between two Numbers.
   * @param n1 Number n1
   * @param n2 Number n2
   * @return Number represent to (n1 - n2).
   */
  public static Number sub(Number n1, Number n2) {
    if (n1 == null) {
      throw new YarnRuntimeException(
          "Number to be subtracted shouldn't be null.");
    } else if (n2 == null) {
      return n1;
    }

    if (n1 instanceof Integer || n1 instanceof Long) {
      return n1.longValue() - n2.longValue();
    }

    if (n1 instanceof Float || n1 instanceof Double) {
      return n1.doubleValue() - n2.doubleValue();
    }

    // TODO throw warnings/exceptions for other types of number.
    return null;
  }

  /**
   * Sum up two Numbers.
   * @param n1 Number n1
   * @param n2 Number n2
   * @return Number represent to (n1 + n2).
   */
  public static Number sum(Number n1, Number n2) {
    if (n1 == null) {
      return n2;
    } else if (n2 == null) {
      return n1;
    }

    if (n1 instanceof Integer || n1 instanceof Long) {
      return n1.longValue() + n2.longValue();
    }

    if (n1 instanceof Float || n1 instanceof Double) {
      return n1.doubleValue() + n2.doubleValue();
    }

    // TODO throw warnings/exceptions for other types of number.
    return null;
  }
}
