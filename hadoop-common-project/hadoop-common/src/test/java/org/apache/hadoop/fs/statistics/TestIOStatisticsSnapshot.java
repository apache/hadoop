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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.*;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test handling of the {@link IOStatisticsSnapshot} class.
 */
public class TestIOStatisticsSnapshot extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIOStatisticsSnapshot.class);

  /**
   * Simple snapshot built up in test setup.
   */
  private final IOStatisticsSnapshot snapshot = new IOStatisticsSnapshot();

  /** Saved to the snapshot as "mean01". */
  private MeanStatistic mean0;

  /** Saved to the snapshot as "mean1". */
  private MeanStatistic mean1;

  @Before
  public void setup() throws Exception {
    snapshot.counters().put("c1", 0L);
    snapshot.gauges().put("g1", 1L);
    snapshot.minimums().put("m1", -1L);
    mean1 = new MeanStatistic(1, 1);
    snapshot.meanStatistics().put("mean1",
        mean1);
    mean0 = new MeanStatistic(0, 1);
    snapshot.meanStatistics().put("mean0",
        mean0);
  }

  @Test
  public void testTrackedValues() throws Throwable {
    verifyStatisticCounterValue(snapshot, "c1", 0L);
    verifyStatisticGaugeValue(snapshot, "g1", 1L);
    verifyStatisticMinimumValue(snapshot, "m1", -1L);
    verifyStatisticMeanValue(snapshot, "mean0",
        new MeanStatistic(0, 1));
  }

  @Test
  public void testStatisticsValueAssertion() throws Throwable {
    // expect an exception to be raised when
    // an assertion is made about the value of an unknown statistics
    assertThatThrownBy(() ->
        verifyStatisticCounterValue(snapshot, "anything", 0))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void testStringification() throws Throwable {
    assertThat(ioStatisticsToString(snapshot))
        .isNotBlank();
  }

  @Test
  public void testStringification2() throws Throwable {

    String ss = snapshot.toString();
    LOG.info("original {}", ss);
    Assertions.assertThat(ss)
        .describedAs("snapshot toString()")
        .contains("c1=0")
        .contains("g1=1");
  }

  @Test
  public void testWrap() throws Throwable {
    IOStatisticsSource statisticsSource = IOStatisticsBinding.wrap(snapshot);
    assertThat(statisticsSource.getIOStatistics())
        .isSameAs(snapshot);
  }

  @Test
  public void testJsonRoundTrip() throws Throwable {
    JsonSerialization<IOStatisticsSnapshot> serializer
        = IOStatisticsSnapshot.serializer();

    String json = serializer.toJson(snapshot);
    LOG.info("serialized form\n{}", json);
    IOStatisticsSnapshot deser = serializer.fromJson(json);
    verifyDeserializedInstance(deser);
  }

  /**
   * Verify the deserialized instance's data
   * matches the expected values.
   * @param deser deserialized vlaue.
   */
  public void verifyDeserializedInstance(
      final IOStatistics deser) {
    LOG.info("deserialized {}", deser);
    verifyStatisticCounterValue(deser, "c1", 0L);
    verifyStatisticGaugeValue(deser, "g1", 1L);
    verifyStatisticMinimumValue(deser, "m1", -1L);
    verifyStatisticMeanValue(deser, "mean0",
        new MeanStatistic(0, 1));
    verifyStatisticMeanValue(deser, "mean1",
        snapshot.meanStatistics().get("mean1"));
  }

  @Test
  public void testJavaRoundTrip() throws Throwable {
    verifyDeserializedInstance(
        IOStatisticAssertions.statisticsJavaRoundTrip(
            snapshot));


  }

}
