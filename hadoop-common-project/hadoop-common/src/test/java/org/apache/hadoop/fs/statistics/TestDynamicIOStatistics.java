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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.statistics.impl.SourceWrappedStatistics;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticCounterIsTracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticCounterIsUntracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatisticsSource;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.ENTRY_PATTERN;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.NULL_SOURCE;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatistics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * verify dynamic statistics are dynamic, except when you iterate through
 * them, along with other tests of the class's behavior.
 */
public class TestDynamicIOStatistics extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDynamicIOStatistics.class);

  private static final String ALONG = "along";

  private static final String AINT = "aint";

  private static final String COUNT = "count";

  private static final String EVAL = "eval";

  /**
   * The statistics.
   */
  private IOStatistics statistics = emptyStatistics();

  /**
   * A source of these statistics.
   */
  private IOStatisticsSource statsSource;

  private final AtomicLong aLong = new AtomicLong();

  private final AtomicInteger aInt = new AtomicInteger();

  private final MutableCounterLong counter = new MutableCounterLong(
      new Info("counter"), 0);

  private long evalLong;

  private static final String[] KEYS = new String[]{ALONG, AINT, COUNT, EVAL};

  @Before
  public void setUp() throws Exception {
    statistics = dynamicIOStatistics()
        .withAtomicLongCounter(ALONG, aLong)
        .withAtomicIntegerCounter(AINT, aInt)
        .withMutableCounter(COUNT, counter)
        .withLongFunctionCounter(EVAL, x -> evalLong)
        .build();
    statsSource = new SourceWrappedStatistics(statistics);
  }

  /**
   * The eval operation is foundational.
   */
  @Test
  public void testEval() throws Throwable {
    verifyStatisticCounterValue(statistics, EVAL, 0);
    evalLong = 10;
    verifyStatisticCounterValue(statistics, EVAL, 10);
  }

  /**
   * Atomic Long statistic.
   */
  @Test
  public void testAlong() throws Throwable {
    verifyStatisticCounterValue(statistics, ALONG, 0);
    aLong.addAndGet(1);
    verifyStatisticCounterValue(statistics, ALONG, 1);
  }

  /**
   * Atomic Int statistic.
   */
  @Test
  public void testAint() throws Throwable {
    verifyStatisticCounterValue(statistics, AINT, 0);
    aInt.addAndGet(1);
    verifyStatisticCounterValue(statistics, AINT, 1);
  }

  /**
   * Metrics2 counter.
   */
  @Test
  public void testCounter() throws Throwable {
    verifyStatisticCounterValue(statistics, COUNT, 0);
    counter.incr();
    verifyStatisticCounterValue(statistics, COUNT, 1);
  }

  /**
   * keys() returns all the keys.
   */
  @Test
  public void testKeys() throws Throwable {
    Assertions.assertThat(statistics.counters().keySet())
        .describedAs("statistic keys of %s", statistics)
        .containsExactlyInAnyOrder(KEYS);
  }

  @Test
  public void testIteratorHasAllKeys() throws Throwable {
    // go through the statistics iterator and assert that it contains exactly
    // the values.
    assertThat(statistics.counters().keySet())
        .containsExactlyInAnyOrder(KEYS);
  }

  /**
   * Verify that the iterator is taken from
   * a snapshot of the values.
   */
  @Test
  public void testIteratorIsSnapshot() throws Throwable {
    // set the counters all to 1
    incrementAllCounters();
    // take the snapshot
    final Iterator<Map.Entry<String, Long>> it =
        statistics.counters().entrySet().iterator();
    // increment the counters
    incrementAllCounters();
    // now assert that all the iterator values are of value 1
    while (it.hasNext()) {
      Map.Entry<String, Long> next = it.next();
      assertThat(next.getValue())
          .describedAs("Value of entry %s", next)
          .isEqualTo(1);
    }
  }

  @Test
  public void testUnknownStatistic() throws Throwable {
    assertStatisticCounterIsUntracked(statistics, "anything");
  }

  @Test
  public void testStatisticsTrackedAssertion() throws Throwable {
    // expect an exception to be raised when an assertion
    // is made that an unknown statistic is tracked,.
    assertThatThrownBy(() ->
        assertStatisticCounterIsTracked(statistics, "anything"))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void testStatisticsValueAssertion() throws Throwable {
    // expect an exception to be raised when
    // an assertion is made about the value of an unknown statistics
    assertThatThrownBy(() ->
        verifyStatisticCounterValue(statistics, "anything", 0))
        .isInstanceOf(AssertionError.class);
  }

  /**
   * Serialization round trip will preserve all the values.
   */
  @Test
  public void testSerDeser() throws Throwable {
    incrementAllCounters();
    IOStatistics stat = IOStatisticsSupport.snapshotIOStatistics(statistics);
    incrementAllCounters();
    IOStatistics deser = IOStatisticAssertions.statisticsJavaRoundTrip(stat);
    assertThat(deser.counters().keySet())
        .containsExactlyInAnyOrder(KEYS);
    for (Map.Entry<String, Long> e : deser.counters().entrySet()) {
      assertThat(e.getValue())
          .describedAs("Value of entry %s", e)
          .isEqualTo(1);
    }
  }

  @Test
  public void testStringification() throws Throwable {
    assertThat(ioStatisticsToString(statistics))
        .isNotBlank()
        .contains(KEYS);
  }

  @Test
  public void testDemandStringification() throws Throwable {
    String counterPattern = ENTRY_PATTERN;
    // this is not yet evaluated
    Object demand = demandStringifyIOStatistics(statistics);
    // nor is this.
    Object demandSource = demandStringifyIOStatisticsSource(statsSource);

    // show it evaluates
    String formatted1 = String.format(counterPattern, ALONG, aLong.get());
    assertThat(demand
        .toString())
        .contains(formatted1);
    assertThat(demandSource
        .toString())
        .contains(formatted1);

    // when the counters are incremented
    incrementAllCounters();
    incrementAllCounters();
    // there are new values to expect
    String formatted2 = String.format(counterPattern, ALONG, aLong.get());
    assertThat(demand
        .toString())
        .doesNotContain(formatted1)
        .contains(formatted2);
    assertThat(demandSource
        .toString())
        .doesNotContain(formatted1)
        .contains(formatted2);
  }

  @Test
  public void testNullSourceStringification() throws Throwable {
    assertThat(demandStringifyIOStatisticsSource((IOStatisticsSource) null)
        .toString())
        .isEqualTo(NULL_SOURCE);
  }

  @Test
  public void testNullStatStringification() throws Throwable {
    assertThat(demandStringifyIOStatistics((IOStatistics) null)
        .toString())
        .isEqualTo(NULL_SOURCE);
  }

  @Test
  public void testStringLogging() throws Throwable {
    LOG.info("Output {}", demandStringifyIOStatistics(statistics));
  }

  /**
   * Increment all the counters from their current value.
   */
  private void incrementAllCounters() {
    aLong.incrementAndGet();
    aInt.incrementAndGet();
    evalLong += 1;
    counter.incr();
  }

  /**
   * Needed to provide a metrics info instance for the counter
   * constructor.
   */
  private static final class Info implements MetricsInfo {

    private final String name;

    private Info(final String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return name;
    }
  }

}
