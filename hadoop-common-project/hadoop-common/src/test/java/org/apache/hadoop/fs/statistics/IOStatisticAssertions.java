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
import java.util.Map;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.ObjectAssert;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Assertions and any other support for IOStatistics testing.
 * <p></p>
 * If used downstream: know it is unstable.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class IOStatisticAssertions {

  private static final String COUNTER = "Counter";

  private static final String GAUGE = "Gauge";

  private static final String MINIMUM = "Minimum";

  private static final String MAXIMUM = "Maxiumum";

  private static final String MEAN = "Mean";

  private IOStatisticAssertions() {
  }

  /**
   * Get a required counter statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return the value
   */
  public static long lookupCounterStatistic(
      final IOStatistics stats,
      final String key) {
    return lookupStatistic(COUNTER, key,
        verifyStatisticsNotNull(stats).counters());
  }

  /**
   * Given an IOStatistics instance, verify it is not null,
   * and return the value for continued use in a test.
   * @param stats statistics source.
   * @param <T> type of statistics
   * @return the value passed in.
   */
  public static <T extends IOStatistics> T
      verifyStatisticsNotNull(final T stats) {
    assertThat(stats)
        .describedAs("IO Statistics reference")
        .isNotNull();
    return stats;
  }

  /**
   * Get a required gauge statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return the value
   */
  public static long lookupGaugeStatistic(
      final IOStatistics stats,
      final String key) {
    return lookupStatistic(GAUGE, key,
        verifyStatisticsNotNull(stats).gauges());
  }

  /**
   * Get a required maximum statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return the value
   */
  public static long lookupMaximumStatistic(
      final IOStatistics stats,
      final String key) {
    return lookupStatistic(MAXIMUM, key,
        verifyStatisticsNotNull(stats).maximums());
  }

  /**
   * Get a required minimum statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return the value
   */
  public static long lookupMinimumStatistic(
      final IOStatistics stats,
      final String key) {
    return lookupStatistic(MINIMUM, key,
        verifyStatisticsNotNull(stats).minimums());
  }

  /**
   * Get a required mean statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return the value
   */
  public static MeanStatistic lookupMeanStatistic(
      final IOStatistics stats,
      final String key) {
    return lookupStatistic(MEAN, key,
        verifyStatisticsNotNull(stats).meanStatistics());
  }

  /**
   * Get a required counter statistic.
   * @param <E> type of map element
   * @param type type for error text
   * @param key statistic key
   * @param map map to probe
   * @return the value
   */
  private static <E> E lookupStatistic(
      final String type,
      final String key,
      final Map<String, E> map) {
    final E statistic = map.get(key);
    assertThat(statistic)
        .describedAs("%s named %s", type, key)
        .isNotNull();
    return statistic;
  }

  /**
   * Assert that a counter has an expected value.
   * @param stats statistics source
   * @param key statistic key
   * @param value expected value.
   * @return the value (which always equals the expected value)
   */
  public static long verifyCounterStatisticValue(
      final IOStatistics stats,
      final String key,
      final long value) {
    return verifyStatisticValue(COUNTER, key,
        verifyStatisticsNotNull(stats).counters(), value);
  }

  /**
   * Assert that a gauge has an expected value.
   * @param stats statistics source
   * @param key statistic key
   * @param value expected value.
   * @return the value (which always equals the expected value)
   */
  public static long verifyGaugeStatisticValue(
      final IOStatistics stats,
      final String key,
      final long value) {
    return verifyStatisticValue(GAUGE, key,
        verifyStatisticsNotNull(stats).gauges(), value);
  }

  /**
   * Assert that a maximum has an expected value.
   * @param stats statistics source
   * @param key statistic key
   * @param value expected value.
   * @return the value (which always equals the expected value)
   */
  public static long verifyMaximumStatisticValue(
      final IOStatistics stats,
      final String key,
      final long value) {
    return verifyStatisticValue(MAXIMUM, key,
        verifyStatisticsNotNull(stats).maximums(), value);
  }

  /**
   * Assert that a minimum has an expected value.
   * @param stats statistics source
   * @param key statistic key
   * @param value expected value.
   * @return the value (which always equals the expected value)
   */
  public static long verifyMinimumStatisticValue(
      final IOStatistics stats,
      final String key,
      final long value) {
    return verifyStatisticValue(MINIMUM, key,
        verifyStatisticsNotNull(stats).minimums(), value);
  }

  /**
   * Assert that a mean has an expected value.
   * @param stats statistics source
   * @param key statistic key
   * @param value expected value.
   * @return the value (which always equals the expected value)
   */
  public static MeanStatistic verifyMeanStatisticValue(
      final IOStatistics stats,
      final String key,
      final MeanStatistic value) {
    return verifyStatisticValue(MEAN, key,
        verifyStatisticsNotNull(stats).meanStatistics(), value);
  }

  /**
   * Assert that a given statistic has an expected value.
   * @param type type for error text
   * @param key statistic key
   * @param map map to look up
   * @param value expected value.
   * @param <E> type of map element
   * @return the value (which always equals the expected value)
   */
  private static <E> E verifyStatisticValue(
      final String type,
      final String key,
      final Map<String, E> map,
      final E value) {
    final E statistic = lookupStatistic(type, key, map);
    assertThat(statistic)
        .describedAs("%s named %s with expected value %s", type,
            key, value)
        .isEqualTo(value);
    return statistic;
  }


  /**
   * Assert that a given statistic has an expected value.
   * @param <E> type of map element
   * @param type type for error text
   * @param key statistic key
   * @param map map to look up
   * @return an ongoing assertion
   */
  private static <E> ObjectAssert<E> assertThatStatistic(
      final String type,
      final String key,
      final Map<String, E> map) {
    final E statistic = lookupStatistic(type, key, map);
    return assertThat(statistic)
        .describedAs("%s named %s", type, key);
  }

  /**
   * Assert that a given statistic has an expected value.
   * @param <E> type of map element
   * @param type type for error text
   * @param key statistic key
   * @param map map to look up
   * @return an ongoing assertion
   */
  private static AbstractLongAssert<?> assertThatLongStatistic(
      final String type,
      final String key,
      final Map<String, Long> map) {
    final long statistic = lookupStatistic(type, key, map);
    return assertThat(statistic)
        .describedAs("%s named %s", type, key);
  }

  /**
   * Start an assertion chain on
   * a required counter statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return an ongoing assertion
   */
  public static AbstractLongAssert<?> assertThatCounterStatistic(
      final IOStatistics stats,
      final String key) {
    return assertThatLongStatistic(COUNTER, key,
        verifyStatisticsNotNull(stats).counters());
  }

  /**
   * Start an assertion chain on
   * a required gauge statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return an ongoing assertion
   */
  public static AbstractLongAssert<?> assertThatGaugeStatistic(
      final IOStatistics stats,
      final String key) {
    return assertThatLongStatistic(GAUGE, key,
        verifyStatisticsNotNull(stats).gauges());
  }

  /**
   * Start an assertion chain on
   * a required minimum statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return an ongoing assertion
   */
  public static AbstractLongAssert<?> assertThatMinimumStatistic(
      final IOStatistics stats,
      final String key) {
    return assertThatLongStatistic(MINIMUM, key,
        verifyStatisticsNotNull(stats).minimums());
  }

  /**
   * Start an assertion chain on
   * a required maximum statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return an ongoing assertion
   */
  public static AbstractLongAssert<?> assertThatMaximumStatistic(
      final IOStatistics stats,
      final String key) {
    return assertThatLongStatistic(MAXIMUM, key,
        verifyStatisticsNotNull(stats).maximums());
  }

  /**
   * Start an assertion chain on
   * a required mean statistic.
   * @param stats statistics source
   * @param key statistic key
   * @return an ongoing assertion
   */
  public static ObjectAssert<MeanStatistic> assertThatMeanStatistic(
      final IOStatistics stats,
      final String key) {
    return assertThatStatistic(MEAN, key,
        verifyStatisticsNotNull(stats).meanStatistics());
  }

  /**
   * Start an assertion chain on
   * a required mean statistic with the initial validation on the
   * sample count and sum.
   * @param stats statistics source
   * @param key statistic key
   * @return an ongoing assertion
   */
  public static ObjectAssert<MeanStatistic> assertThatMeanStatisticMatches(
      final IOStatistics stats,
      final String key,
      final long samples,
      final long sum) {
    return assertThatMeanStatistic(stats, key)
        .matches(p -> (p.getSamples() == samples),
            "samples == " + samples)
        .matches(p -> (p.getSum() == sum),
            "sum == " + sum);
  }

  /**
   * Assert that a given counter statistic is untracked.
   * @param stats statistics source
   * @param type type for error text
   * @param key statistic key
   * @param map map to probe
   */
  private static void assertUntracked(final IOStatistics stats,
      final String type,
      final String key,
      final Map<String, ?> map) {
    assertThat(map.containsKey(key))
        .describedAs("%s %s is tracked in %s", type, key, stats)
        .isFalse();
  }

  /**
   * Assert that a given counter statistic is untracked.
   * @param stats statistics source
   * @param type type for error text
   * @param key statistic key
   * @param map map to probe
   */
  private static void assertTracked(final IOStatistics stats,
      final String type,
      final String key,
      final Map<String, ?> map) {
    assertThat(map.containsKey(key))
        .describedAs("%s %s is not tracked in %s", type, key, stats)
        .isTrue();
  }

  /**
   * Assert that a given statistic is tracked.
   * @param stats statistics source
   * @param key statistic key
   */
  public static void assertCounterStatisticIsTracked(
      final IOStatistics stats,
      final String key) {
    assertTracked(stats, COUNTER, key,
        verifyStatisticsNotNull(stats).counters());
  }

  /**
   * Assert that a given counter statistic is untracked.
   * @param stats statistics source
   * @param key statistic key
   */
  public static void assertCounterStatisticIsUntracked(
      final IOStatistics stats,
      final String key) {
    assertUntracked(stats, COUNTER, key,
        verifyStatisticsNotNull(stats).counters());
  }

  /**
   * Assert that an object is a statistics source and that the
   * statistics is not null.
   * @param source source object.
   */
  public static void assertIsStatisticsSource(Object source) {
    assertThat(source)
        .describedAs("Object %s", source)
        .isInstanceOf(IOStatisticsSource.class)
        .extracting(o -> ((IOStatisticsSource) o).getIOStatistics())
        .isNotNull();
  }

  /**
   * Query the source for the statistics; fails if the statistics
   * returned are null or the class does not implement the API.
   * @param source source object.
   * @return the statistics it provides.
   */
  public static IOStatistics extractStatistics(Object source) {
    assertThat(source)
        .describedAs("Object %s", source)
        .isInstanceOf(IOStatisticsSource.class);
    IOStatisticsSource ios = (IOStatisticsSource) source;
    return extractStatistics(ios);
  }

  /**
   * Get the non-null statistics.
   * @param ioStatisticsSource source
   * @return the statistics, guaranteed to be non null
   */
  private static IOStatistics extractStatistics(
      final IOStatisticsSource ioStatisticsSource) {
    IOStatistics statistics = ioStatisticsSource.getIOStatistics();
    assertThat(statistics)
        .describedAs("Statistics from %s", ioStatisticsSource)
        .isNotNull();
    return statistics;
  }

  /**
   * Perform a serialization round trip on a statistics instance.
   * @param stat statistic
   * @return the deserialized version.
   */
  public static IOStatistics javaRoundTrip(final IOStatistics stat)
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
