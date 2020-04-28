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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Test;

import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsTracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsUnknown;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsUntracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.iostatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatistics;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test handling of the empty IO statistics class.
 */
public class TestEmptyIOStatistics extends AbstractHadoopTestBase {

  private final IOStatistics empty = emptyStatistics();

  @Test
  public void testIterator() throws Throwable {
    Iterator<Map.Entry<String, Long>> iterator = empty.iterator();

    assertThat(iterator.hasNext())
        .describedAs("iterator.hasNext()")
        .isFalse();
    intercept(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testUnknownStatistic() throws Throwable {
    assertStatisticIsUnknown(empty, "anything");
    assertStatisticIsUntracked(empty, "anything");
  }

  @Test
  public void testStatisticsTrackedAssertion() throws Throwable {
    // expect an exception to be raised when an assertion
    // is made that an unknown statistic is tracked,.
    assertThatThrownBy(() ->
        assertStatisticIsTracked(empty, "anything"))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void testStatisticsValueAssertion() throws Throwable {
    // expect an exception to be raised when
    // an assertion is made about the value of an unknown statistics
    assertThatThrownBy(() ->
        verifyStatisticValue(empty, "anything", 0))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void testEmptySnapshot() throws Throwable {
    final IOStatistics stat = IOStatisticsSupport.snapshot(empty);
    assertThat(stat.keys())
        .describedAs("keys of snapshot")
        .isEmpty();
    IOStatistics deser = IOStatisticAssertions.roundTrip(stat);
    assertThat(deser.keys())
        .describedAs("keys of deserialized snapshot")
        .isEmpty();
  }

  @Test
  public void testStringification() throws Throwable {
    assertThat(iostatisticsToString(empty))
        .isNotBlank();
  }

  @Test
  public void testWrap() throws Throwable {
    IOStatisticsSource statisticsSource = IOStatisticsBinding.wrap(empty);
    assertThat(statisticsSource.getIOStatistics())
        .isSameAs(empty);
  }

  @Test
  public void testStringifyNullSource() throws Throwable {
    assertThat(IOStatisticsLogging.sourceToString(null))
        .isEmpty();
  }

  @Test
  public void testStringifyNullStats() throws Throwable {
    assertThat(
        IOStatisticsLogging.sourceToString(
            IOStatisticsBinding.wrap(null)))
        .isEmpty();
  }

  @Test
  public void testStringificationNull() throws Throwable {
    assertThat(iostatisticsToString(null))
        .describedAs("Null statistics should stringify to \"\"")
        .isEmpty();
  }

}
