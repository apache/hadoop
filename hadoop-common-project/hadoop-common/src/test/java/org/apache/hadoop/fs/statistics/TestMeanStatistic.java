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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.JsonSerialization;

/**
 * Test the {@link MeanStatistic} class.
 */
public class TestMeanStatistic extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMeanStatistic.class);

  private static final int TEN = 10;

  private static final double ZEROD = 0.0d;

  private static final double TEND = 10.0d;

  private final MeanStatistic empty = new MeanStatistic(0, 0);

  private final MeanStatistic tenFromOne = new MeanStatistic(1, TEN);

  private final MeanStatistic tenFromTen = new MeanStatistic(TEN, TEN);

  @Test
  public void testEmptiness() throws Throwable {
    Assertions.assertThat(empty)
        .matches(MeanStatistic::isEmpty, "is empty")
        .isEqualTo(new MeanStatistic(0, TEN))
        .isEqualTo(new MeanStatistic())
        .isNotEqualTo(tenFromOne);
    Assertions.assertThat(empty.mean())
        .isEqualTo(ZEROD);
    Assertions.assertThat(empty.toString())
        .contains("0.0");
  }

  @Test
  public void testTenFromOne() throws Throwable {
    Assertions.assertThat(tenFromOne)
        .matches(p -> !p.isEmpty(), "is not empty")
        .isEqualTo(tenFromOne)
        .isNotEqualTo(tenFromTen);
    Assertions.assertThat(tenFromOne.mean())
        .isEqualTo(TEND);
  }

  @Test
  public void testNegativeSamplesAreEmpty() throws Throwable {
    MeanStatistic stat = new MeanStatistic(-10, 1);
    Assertions.assertThat(stat)
        .describedAs("stat with negative samples")
        .matches(MeanStatistic::isEmpty, "is empty")
        .isEqualTo(empty)
        .extracting(MeanStatistic::mean)
        .isEqualTo(ZEROD);
    Assertions.assertThat(stat.toString())
        .contains("0.0");

  }

  @Test
  public void testCopyNonEmpty() throws Throwable {
    MeanStatistic stat = tenFromOne.copy();
    Assertions.assertThat(stat)
        .describedAs("copy of " + tenFromOne)
        .isEqualTo(tenFromOne)
        .isNotSameAs(tenFromOne);
  }

  @Test
  public void testCopyEmpty() throws Throwable {
    MeanStatistic stat = empty.copy();
    Assertions.assertThat(stat)
        .describedAs("copy of " + empty)
        .isEqualTo(empty)
        .isNotSameAs(empty);
  }

  @Test
  public void testDoubleSamples() throws Throwable {
    MeanStatistic stat = tenFromOne.copy();
    Assertions.assertThat(stat.add(tenFromOne))
        .isEqualTo(new MeanStatistic(2, 20))
        .extracting(MeanStatistic::mean)
        .isEqualTo(TEND);
  }

  @Test
  public void testAddEmptyR() throws Throwable {
    MeanStatistic stat = tenFromOne.copy();
    Assertions.assertThat(stat.add(empty))
        .isEqualTo(tenFromOne);
  }

  @Test
  public void testAddEmptyL() throws Throwable {
    MeanStatistic stat = empty.copy();
    Assertions.assertThat(stat.add(tenFromOne))
        .isEqualTo(tenFromOne);
  }

  @Test
  public void testAddEmptyLR() throws Throwable {
    MeanStatistic stat = empty.copy();
    Assertions.assertThat(stat.add(empty))
        .isEqualTo(empty);
  }

  @Test
  public void testAddSampleToEmpty() throws Throwable {
    MeanStatistic stat = empty.copy();
    stat.addSample(TEN);
    Assertions.assertThat(stat)
        .isEqualTo(tenFromOne);
  }

  @Test
  public void testAddZeroValueSamples() throws Throwable {
    MeanStatistic stat = tenFromOne.copy();
    for (int i = 0; i < 9; i++) {
      stat.addSample(0);
    }
    Assertions.assertThat(stat)
        .isEqualTo(tenFromTen);
  }

  @Test
  public void testSetSamples() throws Throwable {
    MeanStatistic stat = tenFromOne.copy();
    stat.setSamples(10);
    Assertions.assertThat(stat)
        .isEqualTo(tenFromTen);
  }

  @Test
  public void testSetSums() throws Throwable {
    MeanStatistic stat = tenFromOne.copy();
    stat.setSum(100);
    stat.setSamples(20);
    Assertions.assertThat(stat)
        .isEqualTo(new MeanStatistic(20, 100))
        .extracting(MeanStatistic::mean)
        .isEqualTo(5.0d);
  }

  @Test
  public void testSetNegativeSamplesMakesEmpty() throws Throwable {
    MeanStatistic stat = tenFromOne.copy();
    stat.setSamples(-3);
    Assertions.assertThat(stat)
        .isEqualTo(empty);
  }

  @Test
  public void testJsonRoundTrip() throws Throwable {
    JsonSerialization<MeanStatistic> serializer = serializer();

    String json = serializer.toJson(tenFromTen);
    LOG.info("serialized form\n{}", json);
    Assertions.assertThat(json)
        .describedAs("JSON form of %s", tenFromTen)
        .doesNotContain("empty")
        .doesNotContain("mean");

    MeanStatistic deser = serializer.fromJson(json);
    LOG.info("deserialized {}", deser);
    Assertions.assertThat(deser)
        .isEqualTo(tenFromTen);
  }

  /**
   * negative sample counts in the json convert the stat to being empty.
   */
  @Test
  public void testHandleMaliciousStat() throws Throwable {
    String json = "{\n"
        + "  \"sum\" : 10,\n"
        + "  \"samples\" : -10\n"
        + "}";
    JsonSerialization<MeanStatistic> serializer = serializer();
    MeanStatistic deser = serializer.fromJson(json);
    LOG.info("deserialized {}", deser);
    Assertions.assertThat(deser)
        .isEqualTo(empty);
  }

  /**
   * Get a JSON serializer.
   * @return a serializer.
   */
  public static JsonSerialization<MeanStatistic> serializer() {
    return new JsonSerialization<>(MeanStatistic.class, true, true);
  }
}
