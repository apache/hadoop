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

package org.apache.hadoop.fs.statistics;

import org.apache.hadoop.classification.InterfaceStability;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Assertions and any other support for IOStatistics testing.
 * If used downstream, know it is unstable.
 * There's some oddness here related to AssertJ's handling of iterables;
 * we need to explicitly cast it to call methods on the interface
 * other than iterator().
 */
@InterfaceStability.Unstable
public final class IOStatisticAssertions {

  private IOStatisticAssertions() {
  }

  /**
   * Assert that a statistics instance has an attribute.
   * Note: some type inference in Assertions causes confusion
   * with the .matches predicate; it needs to be cast down to its type
   * again.
   * @param stats statistics source
   * @param attr attribute to probe for
   */
  public static void assertHasAttribute(
      final IOStatistics stats,
      final IOStatistics.Attributes attr) {
    assertThat(stats)
        .describedAs("Statistics %s and attribute %s", stats, attr)
        .isNotNull()
        .matches(s -> ((IOStatistics) s).hasAttribute(attr),
            "Does not have attribute " + attr);
  }

  /**
   * Assert that a statistics instance has an attribute.
   * Note: some type inference in Assertions causes confusion
   * with the .matches predicate; it needs to be cast down to its type
   * again.
   * @param stats statistics source
   * @param attr attribute to probe for
   */
  public static void assertDoesNotHaveAttribute(
      final IOStatistics stats,
      final IOStatistics.Attributes attr) {
    assertThat(stats)
        .describedAs("Statistics %s and attribute %s", stats, attr)
        .isNotNull()
        .matches(s -> !((IOStatistics) s).hasAttribute(attr),
            "Should not have attribute " + attr);
  }

  /**
   * Assert that a given statistic has an expected value.
   * @param stats statistics source
   * @param key statistic key
   * @param value expected value.
   */
  public static void assertStatisticHasValue(
      final IOStatistics stats,
      final String key,
      final long value) {
    assertThat(stats)
        .describedAs("Statistics %s and key %s with expected value %s", stats,
            key, value)
        .isNotNull()
        .extracting(s -> ((IOStatistics) s).getStatistic(key))
        .isNotNull()
        .isEqualTo(value);
  }

  /**
   * Assert that a given statistic is unknown.
   * @param stats statistics source
   * @param key statistic key
   */
  public static void assertStatisticUnknown(
      final IOStatistics stats,
      final String key) {
    assertThat(stats.getStatistic(key))
        .describedAs("Statistics %s and key %s", stats,
            key)
        .isNull();
  }

  /**
   * Assert that a given statistic is tracked.
   * @param stats statistics source
   * @param key statistic key
   */
  public static void assertStatisticTracked(
      final IOStatistics stats,
      final String key) {
    assertThat(stats.isTracked(key))
        .describedAs("Statistic %s is not tracked in %s", key, stats)
        .isTrue();
  }

  /**
   * Assert that a given statistic is untracked.
   * @param stats statistics source
   * @param key statistic key
   */
  public static void assertStatisticUntracked(
      final IOStatistics stats,
      final String key) {
    assertThat(stats.isTracked(key))
        .describedAs("Statistic %s is tracked in %s", key, stats)
        .isFalse();
  }
}
