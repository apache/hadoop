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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;
import org.apache.hadoop.util.functional.FutureIO;

import static org.apache.hadoop.fs.statistics.DurationStatisticSummary.fetchDurationSummary;
import static org.apache.hadoop.fs.statistics.DurationStatisticSummary.fetchSuccessSummary;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.*;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.*;
import static org.apache.hadoop.fs.statistics.impl.StubDurationTrackerFactory.STUB_DURATION_TRACKER_FACTORY;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the IOStatistic DurationTracker logic.
 */
public class TestDurationTracking extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDurationTracking.class);

  private static final String REQUESTS = "requests";

  public static final String UNKNOWN = "unknown";

  private IOStatisticsStore stats;

  private final AtomicInteger invocationCounter = new AtomicInteger(0);

  @Before
  public void setup() {
    stats = iostatisticsStore()
        .withDurationTracking(REQUESTS)
        .build();
  }

  @After
  public void teardown() {
    LOG.info("stats {}", stats);
  }

  /**
   * Duration tracking.
   */
  @Test
  public void testDurationTryWithResources() throws Throwable {
    DurationTracker tracker =
        stats.trackDuration(REQUESTS);
    verifyStatisticCounterValue(stats, REQUESTS, 1L);
    sleep();
    tracker.close();
    try (DurationTracker ignored =
             stats.trackDuration(REQUESTS)) {
      sleep();
    }
    LOG.info("Statistics: {}", stats);
    DurationStatisticSummary summary = fetchSuccessSummary(stats, REQUESTS);
    assertSummaryValues(summary, 2, 1, 1);
    assertSummaryMean(summary, 2, 0);
  }

  /**
   * A little sleep method; exceptions are swallowed.
   * Increments {@link #invocationCounter}.
   * Increments {@inheritDoc #atomicCounter}.
   */
  public void sleep() {
    sleepf(10);
  }

  /**
   * A little sleep function; exceptions are swallowed.
   * Increments {@link #invocationCounter}.
   */
  protected int sleepf(final int millis) {
    invocationCounter.incrementAndGet();
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
    }
    return millis;
  }

  /**
   * Assert that the sleep counter has been invoked
   * the expected number of times.
   * @param expected expected value
   */
  private void assertCounterValue(final int expected) {
    assertThat(invocationCounter.get())
        .describedAs("Sleep invocation Counter")
        .isEqualTo(expected);
  }

  /**
   * Test that a function raising an IOE can be wrapped.
   */
  @Test
  public void testDurationFunctionIOE() throws Throwable {
    FunctionRaisingIOE<Integer, Integer> fn =
        trackFunctionDuration(stats, REQUESTS,
            (Integer x) -> invocationCounter.getAndSet(x));
    assertThat(fn.apply(1)).isEqualTo(0);
    assertCounterValue(1);
    assertSummaryValues(
        fetchSuccessSummary(stats, REQUESTS),
        1, 0, 0);
  }

  /**
   * Trigger a failure and verify its the failure statistics
   * which go up.
   */
  @Test
  public void testDurationFunctionIOEFailure() throws Throwable {
    FunctionRaisingIOE<Integer, Integer> fn =
        trackFunctionDuration(stats, REQUESTS,
            (Integer x) -> {
              sleep();
              return 100 / x;
            });
    intercept(ArithmeticException.class,
        () -> fn.apply(0));
    assertSummaryValues(
        fetchSuccessSummary(stats, REQUESTS),
        1, -1, -1);

    DurationStatisticSummary failures = fetchDurationSummary(stats, REQUESTS,
        false);
    assertSummaryValues(failures, 1, 0, 0);
    assertSummaryMean(failures, 1, 0);
  }

  /**
   * Trigger a failure and verify its the failure statistics
   * which go up.
   */
  @Test
  public void testDurationJavaFunctionFailure() throws Throwable {
    Function<Integer, Integer> fn =
        trackJavaFunctionDuration(stats, REQUESTS,
            (Integer x) -> {
              return 100 / x;
            });
    intercept(ArithmeticException.class,
        () -> fn.apply(0));
    assertSummaryValues(
        fetchSuccessSummary(stats, REQUESTS),
        1, -1, -1);

    DurationStatisticSummary failures = fetchDurationSummary(stats, REQUESTS,
        false);
    assertSummaryValues(failures, 1, 0, 0);
  }

  /**
   * Test trackDurationOfCallable.
   */
  @Test
  public void testCallableDuration() throws Throwable {
    // call the operation
    assertThat(
        trackDurationOfCallable(stats, REQUESTS, () -> sleepf(100)).call())
        .isEqualTo(100);
    DurationStatisticSummary summary = fetchSuccessSummary(stats, REQUESTS);
    assertSummaryValues(summary, 1, 0, 0);
    assertSummaryMean(summary, 1, 0);
  }

  /**
   * Callable raising an RTE after a sleep; failure
   * stats will be updated and the execution count will be
   * 1.
   */
  @Test
  public void testCallableFailureDuration() throws Throwable {

    intercept(RuntimeException.class,
        trackDurationOfCallable(stats, REQUESTS, () -> {
          sleepf(100);
          throw new RuntimeException("oops");
        }));
    assertCounterValue(1);
    assertSummaryValues(
        fetchSuccessSummary(stats, REQUESTS),
        1, -1, -1);

    assertSummaryValues(fetchDurationSummary(stats, REQUESTS, false),
        1, 0, 0);
  }

  /**
   * Duration of the successful execution of a InvocationRaisingIOE.
   */
  @Test
  public void testInvocationDuration() throws Throwable {
    // call the operation
    trackDurationOfInvocation(stats, REQUESTS, () -> {
      sleepf(100);
    });
    assertCounterValue(1);
    DurationStatisticSummary summary = fetchSuccessSummary(stats, REQUESTS);
    assertSummaryValues(summary, 1, 0, 0);
    assertSummaryMean(summary, 1, 0);
  }

  /**
   * Duration of the successful execution of a CallableRaisingIOE.
   */
  @Test
  public void testCallableIOEDuration() throws Throwable {
    // call the operation
    assertThat(
        trackDuration(stats, REQUESTS, () -> sleepf(100)))
        .isEqualTo(100);
    DurationStatisticSummary summary = fetchSuccessSummary(stats, REQUESTS);
    assertSummaryValues(summary, 1, 0, 0);
    assertSummaryMean(summary, 1, 0);
  }

  /**
   * Track the duration of an IOE raising callable which fails.
   */
  @Test
  public void testCallableIOEFailureDuration() throws Throwable {
    intercept(IOException.class,
        () ->
        trackDuration(stats, REQUESTS, () -> {
          sleepf(100);
          throw new IOException("oops");
        }));
    assertSummaryValues(
        fetchSuccessSummary(stats, REQUESTS),
        1, -1, -1);

    assertSummaryValues(fetchDurationSummary(stats, REQUESTS, false),
        1, 0, 0);
  }


  /**
   * Track the duration of an IOE raising callable which fails.
   */
  @Test
  public void testDurationThroughEval() throws Throwable {
    CompletableFuture<Object> eval = FutureIOSupport.eval(
        trackDurationOfOperation(stats, REQUESTS, () -> {
          sleepf(100);
          throw new FileNotFoundException("oops");
        }));
    intercept(FileNotFoundException.class, "oops", () ->
        FutureIO.awaitFuture(eval));
    assertSummaryValues(fetchDurationSummary(stats, REQUESTS, false),
        1, 0, 0);
  }

  /**
   * It's OK to track a duration against an unknown statistic.
   */
  @Test
  public void testUnknownDuration() throws Throwable {
    trackDurationOfCallable(stats, UNKNOWN, () -> sleepf(1)).call();
    DurationStatisticSummary summary = fetchSuccessSummary(stats, UNKNOWN);
    assertSummaryValues(summary, 0, -1, -1);
    assertThat(summary.getMean()).isNull();
  }

  /**
   * The stub duration tracker factory can be supplied as an input.
   */
  @Test
  public void testTrackDurationWithStubFactory() throws Throwable {
    trackDuration(STUB_DURATION_TRACKER_FACTORY, UNKNOWN, () -> sleepf(1));
  }

  /**
   * Make sure the tracker returned from the stub factory
   * follows the basic lifecycle.
   */
  @Test
  public void testStubDurationLifecycle() throws Throwable {
    DurationTracker tracker = STUB_DURATION_TRACKER_FACTORY
        .trackDuration("k", 1);
    tracker.failed();
    tracker.close();
    tracker.close();
  }

  /**
   * Assert that a statistics summary has the specific values.
   * @param summary summary data
   * @param count count -must match exactly.
   * @param minBase minimum value for the minimum field (inclusive)
   * @param maxBase minimum value for the maximum field (inclusive)
   */
  protected void assertSummaryValues(
      final DurationStatisticSummary summary,
      final int count,
      final int minBase,
      final int maxBase) {
    assertThat(summary)
        .matches(s -> s.getCount() == count, "Count value")
        .matches(s -> s.getMax() >= maxBase, "Max value")
        .matches(s -> s.getMin() >= minBase, "Min value");
  }

  /**
   * Assert that at a summary has a matching mean value.
   * @param summary summary data.
   * @param expectedSampleCount sample count -which must match
   * @param meanGreaterThan the mean must be greater than this value.
   */
  protected void assertSummaryMean(
      final DurationStatisticSummary summary,
      final int expectedSampleCount,
      final double meanGreaterThan) {
    String description = "mean of " + summary;
    assertThat(summary.getMean())
        .describedAs(description)
        .isNotNull();
    assertThat(summary.getMean().getSamples())
        .describedAs(description)
        .isEqualTo(expectedSampleCount);
    assertThat(summary.getMean().mean())
        .describedAs(description)
        .isGreaterThan(meanGreaterThan);
  }
}
