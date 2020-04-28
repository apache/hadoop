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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceStability;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Assertions and any other support for IOStatistics testing.
 * If used downstream: know it is unstable.
 * There's some oddness here related to AssertJ's handling of iterables;
 * we need to explicitly cast it to call methods on the interface
 * other than iterator().
 */
@InterfaceStability.Unstable
public final class IOStatisticAssertions {

  private IOStatisticAssertions() {
  }

  /**
   * Assert that a given statistic has an expected value.
   * @param stats statistics source
   * @param key statistic key
   * @param value expected value.
   * @return the value (which always equals the expected value)
   */
  public static long verifyStatisticValue(
      final IOStatistics stats,
      final String key,
      final long value) {
    final Long statistic = stats.getStatistic(key);
    assertThat(statistic)
        .describedAs("Statistics %s and key %s with expected value %s", stats,
            key, value)
        .isNotNull()
        .isEqualTo(value);
    return statistic;
  }

  /**
   * Assert that a given statistic is unknown.
   * @param stats statistics source
   * @param key statistic key
   */
  public static void assertStatisticIsUnknown(
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
  public static void assertStatisticIsTracked(
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
  public static void assertStatisticIsUntracked(
      final IOStatistics stats,
      final String key) {
    assertThat(stats.isTracked(key))
        .describedAs("Statistic %s is tracked in %s", key, stats)
        .isFalse();
  }

  /**
   * Assert that an object is a statistics source and that the
   * statistics is not null.
   * @param source source object.
   */
  public static void assertIsStatisticsSource(Object source) {
    assertThat(source)
        .describedAs("Object %s", source )
        .isInstanceOf(IOStatisticsSource.class)
        .extracting(o -> ((IOStatisticsSource)o).getIOStatistics())
        .isNotNull();
  }

  /**
   * query the source for the statistics; fails if the statistics
   * returned are null.
   * @param source source object.
   * @return the statistics it provides.
   */
  public static IOStatistics extractStatistics(Object source) {
    assertThat(source)
        .describedAs("Object %s", source)
        .isInstanceOf(IOStatisticsSource.class);
    IOStatistics statistics = ((IOStatisticsSource) source).getIOStatistics();
    assertThat(statistics)
        .describedAs("Statistics from %s", source)
        .isNotNull();
    return statistics;
  }

  /**
   * Perform a serialization round trip on a statistics instance.
   * @param stat statistic
   * @return the deserialized version.
   */
  public static IOStatistics roundTrip(final IOStatistics stat)
      throws IOException, ClassNotFoundException {
    assertThat(stat).isInstanceOf(Serializable.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(stat);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    IOStatistics deser;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deser = (IOStatistics) ois.readObject();
    }
    return deser;
  }
}
