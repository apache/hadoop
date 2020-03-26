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

import org.assertj.core.api.Assertions;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Assertions and other helper classes for IOStatistics testing.
 * If used downstream, know it is unstable.
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
   * @param stats statistics
   * @param attr attribute to probe for
   */
  static void assertHasAttribute(IOStatistics stats,
      IOStatistics.Attributes attr) {
    Assertions.assertThat(stats)
        .describedAs("Statistics %s and attribute %s", stats, attr)
        .isNotNull()
        .matches(s -> ((IOStatistics)s).hasAttribute(attr),
            "Does not have attribute " + attr);
  }

  /**
   * Assert that a statistics instance has an attribute.
   * Note: some type inference in Assertions causes confusion
   * with the .matches predicate; it needs to be cast down to its type
   * again.
   * @param stats statistics
   * @param attr attribute to probe for
   */
  static void assertDoesNotHaveAttribute(IOStatistics stats,
      IOStatistics.Attributes attr) {
    Assertions.assertThat(stats)
        .describedAs("Statistics %s and attribute %s", stats, attr)
        .isNotNull()
        .matches(s -> !((IOStatistics)s).hasAttribute(attr),
            "Should not have attribute " +   attr);
  }

  static void assertStatisticsValue(IOStatistics stats,
      String key, long value) {
    Assertions.assertThat(stats)
        .describedAs("Statistics %s and key %s with expected value %s", stats,
            key, value)
        .isNotNull()
        .extracting(s -> ((IOStatistics) s).getStatistic(key))
        .isNotNull()
        .isEqualTo(value);
  }

  static void assertStatisticUnknown(IOStatistics stats,
      String key) {
    Assertions.assertThat(stats.getStatistic(key))
        .describedAs("Statistics %s and key %s", stats,
            key)
        .isNull();
  }

  static void assertStatisticTracked(IOStatistics stats,
      String key) {
    Assertions.assertThat(stats.isTracked(key))
        .describedAs("Statistic %s is not tracked in %s", key, stats)
        .isTrue();
  }

  static void assertStatisticUntracked(IOStatistics stats,
      String key) {
    Assertions.assertThat(stats.isTracked(key))
        .describedAs("Statistic %s is tracked in %s", key, stats)
        .isFalse();
  }
}
