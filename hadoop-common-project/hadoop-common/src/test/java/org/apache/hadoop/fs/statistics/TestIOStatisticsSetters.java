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

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.statistics.impl.ForwardingIOStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticGauge;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMaximum;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMean;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMinimum;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the {@link IOStatisticsSetters} interface implementations through
 * a parameterized run with each implementation.
 * For each of the setters, the value is set, verified,
 * updated, verified again.
 * An option known to be undefined in all created IOStatisticsStore instances
 * is set, to verify it is harmless.
 */

public class TestIOStatisticsSetters extends AbstractHadoopTestBase {

  public static final String COUNTER = "counter";

  public static final String GAUGE = "gauge";

  public static final String MAXIMUM = "max";

  public static final String MINIMUM = "min";

  public static final String MEAN = "mean";

  private IOStatisticsSetters ioStatistics;

  private boolean createsNewEntries;

  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"IOStatisticsSnapshot", new IOStatisticsSnapshot(), true},
        {"IOStatisticsStore", createTestStore(), false},
        {"ForwardingIOStatisticsStore", new ForwardingIOStatisticsStore(createTestStore()), false},
    });
  }

  /**
   * Create a test store with the stats used for testing set up.
   * @return a set up store
   */
  private static IOStatisticsStore createTestStore() {
    return iostatisticsStore()
        .withCounters(COUNTER)
        .withGauges(GAUGE)
        .withMaximums(MAXIMUM)
        .withMinimums(MINIMUM)
        .withMeanStatistics(MEAN)
        .build();
  }

  public void initTestIOStatisticsSetters(String source,
      IOStatisticsSetters pIoStatisticsSetters,
      boolean pCreatesNewEntries) {
    this.ioStatistics = pIoStatisticsSetters;
    this.createsNewEntries = pCreatesNewEntries;
  }

  @ParameterizedTest(name="{0}")
  @MethodSource("params")
  public void testCounter(String source,
      IOStatisticsSetters pIoStatisticsSetters, boolean pCreatesNewEntries)
      throws Throwable {
    initTestIOStatisticsSetters(source, pIoStatisticsSetters, pCreatesNewEntries);

    // write
    ioStatistics.setCounter(COUNTER, 1);
    assertThatStatisticCounter(ioStatistics, COUNTER)
        .isEqualTo(1);

    // update
    ioStatistics.setCounter(COUNTER, 2);
    assertThatStatisticCounter(ioStatistics, COUNTER)
        .isEqualTo(2);

    // unknown value
    final String unknown = "unknown";
    ioStatistics.setCounter(unknown, 3);
    if (createsNewEntries) {
      assertThatStatisticCounter(ioStatistics, unknown)
          .isEqualTo(3);
    } else {
      assertThat(ioStatistics.counters())
          .describedAs("Counter map in {}", ioStatistics)
          .doesNotContainKey(unknown);
    }
  }

  @ParameterizedTest(name="{0}")
  @MethodSource("params")
  public void testMaximum(String source,
      IOStatisticsSetters pIoStatisticsSetters, boolean pCreatesNewEntries) throws Throwable {
    initTestIOStatisticsSetters(source, pIoStatisticsSetters, pCreatesNewEntries);
    // write
    ioStatistics.setMaximum(MAXIMUM, 1);
    assertThatStatisticMaximum(ioStatistics, MAXIMUM)
        .isEqualTo(1);

    // update
    ioStatistics.setMaximum(MAXIMUM, 2);
    assertThatStatisticMaximum(ioStatistics, MAXIMUM)
        .isEqualTo(2);

    // unknown value
    ioStatistics.setMaximum("mm2", 3);
  }

  @ParameterizedTest(name="{0}")
  @MethodSource("params")
  public void testMinimum(String source,
      IOStatisticsSetters pIoStatisticsSetters, boolean pCreatesNewEntries) throws Throwable {
    initTestIOStatisticsSetters(source, pIoStatisticsSetters, pCreatesNewEntries);
    // write
    ioStatistics.setMinimum(MINIMUM, 1);
    assertThatStatisticMinimum(ioStatistics, MINIMUM)
        .isEqualTo(1);

    // update
    ioStatistics.setMinimum(MINIMUM, 2);
    assertThatStatisticMinimum(ioStatistics, MINIMUM)
        .isEqualTo(2);

    // unknown value
    ioStatistics.setMinimum("c2", 3);
  }

  @ParameterizedTest(name="{0}")
  @MethodSource("params")
  public void testGauge(String source,
      IOStatisticsSetters pIoStatisticsSetters, boolean pCreatesNewEntries) throws Throwable {
    initTestIOStatisticsSetters(source, pIoStatisticsSetters, pCreatesNewEntries);
    // write
    ioStatistics.setGauge(GAUGE, 1);
    assertThatStatisticGauge(ioStatistics, GAUGE)
        .isEqualTo(1);

    // update
    ioStatistics.setGauge(GAUGE, 2);
    assertThatStatisticGauge(ioStatistics, GAUGE)
        .isEqualTo(2);

    // unknown value
    ioStatistics.setGauge("g2", 3);
  }

  @ParameterizedTest(name="{0}")
  @MethodSource("params")
  public void testMean(String source,
      IOStatisticsSetters pIoStatisticsSetters, boolean pCreatesNewEntries) throws Throwable {
    initTestIOStatisticsSetters(source, pIoStatisticsSetters, pCreatesNewEntries);
    // write
    final MeanStatistic mean11 = new MeanStatistic(1, 1);
    ioStatistics.setMeanStatistic(MEAN, mean11);
    assertThatStatisticMean(ioStatistics, MEAN)
        .isEqualTo(mean11);

    // update
    final MeanStatistic mean22 = new MeanStatistic(2, 2);
    ioStatistics.setMeanStatistic(MEAN, mean22);
    assertThatStatisticMean(ioStatistics, MEAN)
        .isEqualTo(mean22);

    // unknown value
    ioStatistics.setMeanStatistic("m2", mean11);
  }
}
