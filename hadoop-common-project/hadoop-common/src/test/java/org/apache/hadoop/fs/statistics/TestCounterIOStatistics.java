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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.statistics.impl.CounterIOStatistics;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatMeanStatisticMatches;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyCounterStatisticValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyGaugeStatisticValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyMaximumStatisticValue;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyMinimumStatisticValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.counterIOStatistics;

public class TestCounterIOStatistics extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCounterIOStatistics.class);


  private static final String COUNT = "count";

  private static final String GAUGE = "gauge";

  private static final String MIN = "min";

  private static final String MAX = "max";

  private static final String MEAN = "mean";

  public static final String UNKNOWN = "unknown";

  private CounterIOStatistics stats;

  @Before
  public void setup() {
    stats = counterIOStatistics()
        .withCounters(COUNT)
        .withGauges(GAUGE)
        .withMinimums(MIN)
        .withMaximums(MAX)
        .withMeanStatistics(MEAN)
        .build();
  }

  @After
  public void teardown() {
    LOG.info("stats {}", stats);
  }

  /**
   * Gauges go up and down.
   */
  @Test
  public void testGauges() throws Throwable {
    stats.setGauge(GAUGE, 1);
    verifyGaugeStatisticValue(stats, GAUGE, 1);
    stats.incrementGauge(GAUGE, 1);
    verifyGaugeStatisticValue(stats, GAUGE, 2);
    stats.setGauge(GAUGE, -1);
    verifyGaugeStatisticValue(stats, GAUGE, -1);
    stats.incrementGauge(GAUGE, -1);
    verifyGaugeStatisticValue(stats, GAUGE, -2);
    Assertions.assertThat(stats.getGaugeReference(GAUGE).get())
        .isEqualTo(-2);
    stats.incrementGauge(UNKNOWN, 1);
    stats.setGauge(UNKNOWN, 1);
  }

  @Test
  public void testMinimums() throws Throwable {
    stats.setMinimum(MIN, 100);
    verifyMinimumStatisticValue(stats, MIN, 100);
    stats.setMinimum(MIN, 100);
    // will do nothing as it is higher
    stats.addMinimumSample(MIN, 200);
    verifyMinimumStatisticValue(stats, MIN, 100);
    stats.addMinimumSample(MIN, 10);
    verifyMinimumStatisticValue(stats, MIN, 10);
    stats.setMinimum(UNKNOWN, 100);
    stats.addMinimumSample(UNKNOWN, 200);
  }

  @Test
  public void testMaximums() throws Throwable {
    stats.setMaximum(MAX, 100);
    verifyMaximumStatisticValue(stats, MAX, 100);
    stats.setMaximum(MAX, 100);
    stats.addMaximumSample(MAX, 200);
    verifyMaximumStatisticValue(stats, MAX, 200);
    stats.addMaximumSample(MAX, 10);
    verifyMaximumStatisticValue(stats, MAX, 200);
    stats.setMaximum(UNKNOWN, 100);
    stats.addMaximumSample(UNKNOWN, 200);
  }

  @Test
  public void testMeans() throws Throwable {
    stats.setMeanStatistic(MEAN,
        new MeanStatistic(1, 1));

    assertThatMeanStatisticMatches(stats, MEAN, 1, 1)
        .matches(p -> p.mean() == 1, "mean");
    stats.addMeanStatisticSample(MEAN, 9);
    assertThatMeanStatisticMatches(stats, MEAN, 2, 10)
        .matches(p -> p.mean() == 5, "mean");

  }

  @Test
  public void testRoundTrip() throws Throwable {
    JsonSerialization<IOStatisticsSnapshot> serializer
        = IOStatisticsSnapshot.serializer();
    stats.incrementCounter(COUNT, 1);
    stats.setGauge(GAUGE, -1);
    stats.addMaximumSample(MAX, 200);
    stats.addMinimumSample(MIN, -100);
    stats.addMeanStatisticSample(MEAN, 1);
    stats.addMeanStatisticSample(MEAN, 9);

    String json = serializer.toJson(snapshotIOStatistics(stats));
    LOG.info("serialized form\n{}", json);
    IOStatisticsSnapshot deser = serializer.fromJson(json);
    LOG.info("deserialized {}", deser);
    verifyCounterStatisticValue(deser, COUNT, 1L);
    verifyGaugeStatisticValue(deser, GAUGE, -1);
    verifyMaximumStatisticValue(deser, MAX, 200);
    verifyMinimumStatisticValue(deser, MIN, -100);
    assertThatMeanStatisticMatches(deser, MEAN, 2, 10)
        .matches(p -> p.mean() == 5, "mean");

  }


}
