/**
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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for <code>AbfsClientThrottlingAnalyzer</code>.
 */
public class TestAbfsClientThrottlingAnalyzer {
  private static final int ANALYSIS_PERIOD = 1000;
  private static final int ANALYSIS_PERIOD_PLUS_10_PERCENT = ANALYSIS_PERIOD
      + ANALYSIS_PERIOD / 10;
  private static final long MEGABYTE = 1024 * 1024;
  private static final int MAX_ACCEPTABLE_PERCENT_DIFFERENCE = 20;

  private void sleep(long milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void fuzzyValidate(long expected, long actual, double percentage) {
    final double lowerBound = Math.max(expected - percentage / 100 * expected, 0);
    final double upperBound = expected + percentage / 100 * expected;

    assertTrue(
        String.format(
            "The actual value %1$d is not within the expected range: "
                + "[%2$.2f, %3$.2f].",
            actual,
            lowerBound,
            upperBound),
        actual >= lowerBound && actual <= upperBound);
  }

  private void validate(long expected, long actual) {
    assertEquals(
        String.format("The actual value %1$d is not the expected value %2$d.",
            actual,
            expected),
        expected, actual);
  }

  private void validateLessThanOrEqual(long maxExpected, long actual) {
    assertTrue(
        String.format(
            "The actual value %1$d is not less than or equal to the maximum"
                + " expected value %2$d.",
            actual,
            maxExpected),
        actual < maxExpected);
  }

  /**
   * Ensure that there is no waiting (sleepDuration = 0) if the metrics have
   * never been updated.  This validates proper initialization of
   * ClientThrottlingAnalyzer.
   */
  @Test
  public void testNoMetricUpdatesThenNoWaiting() {
    AbfsClientThrottlingAnalyzer analyzer = new AbfsClientThrottlingAnalyzer(
        "test",
        ANALYSIS_PERIOD);
    validate(0, analyzer.getSleepDuration());
    sleep(ANALYSIS_PERIOD_PLUS_10_PERCENT);
    validate(0, analyzer.getSleepDuration());
  }

  /**
   * Ensure that there is no waiting (sleepDuration = 0) if the metrics have
   * only been updated with successful requests.
   */
  @Test
  public void testOnlySuccessThenNoWaiting() {
    AbfsClientThrottlingAnalyzer analyzer = new AbfsClientThrottlingAnalyzer(
        "test",
        ANALYSIS_PERIOD);
    analyzer.addBytesTransferred(8 * MEGABYTE, false);
    validate(0, analyzer.getSleepDuration());
    sleep(ANALYSIS_PERIOD_PLUS_10_PERCENT);
    validate(0, analyzer.getSleepDuration());
  }

  /**
   * Ensure that there is waiting (sleepDuration != 0) if the metrics have
   * only been updated with failed requests.  Also ensure that the
   * sleepDuration decreases over time.
   */
  @Test
  public void testOnlyErrorsAndWaiting() {
    AbfsClientThrottlingAnalyzer analyzer = new AbfsClientThrottlingAnalyzer(
        "test",
        ANALYSIS_PERIOD);
    validate(0, analyzer.getSleepDuration());
    analyzer.addBytesTransferred(4 * MEGABYTE, true);
    sleep(ANALYSIS_PERIOD_PLUS_10_PERCENT);
    final int expectedSleepDuration1 = 1100;
    validateLessThanOrEqual(expectedSleepDuration1, analyzer.getSleepDuration());
    sleep(10 * ANALYSIS_PERIOD);
    final int expectedSleepDuration2 = 900;
    validateLessThanOrEqual(expectedSleepDuration2, analyzer.getSleepDuration());
  }

  /**
   * Ensure that there is waiting (sleepDuration != 0) if the metrics have
   * only been updated with both successful and failed requests.  Also ensure
   * that the sleepDuration decreases over time.
   */
  @Test
  public void testSuccessAndErrorsAndWaiting() {
    AbfsClientThrottlingAnalyzer analyzer = new AbfsClientThrottlingAnalyzer(
        "test",
        ANALYSIS_PERIOD);
    validate(0, analyzer.getSleepDuration());
    analyzer.addBytesTransferred(8 * MEGABYTE, false);
    analyzer.addBytesTransferred(2 * MEGABYTE, true);
    sleep(ANALYSIS_PERIOD_PLUS_10_PERCENT);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    analyzer.suspendIfNecessary();
    final int expectedElapsedTime = 126;
    fuzzyValidate(expectedElapsedTime,
        timer.elapsedTimeMs(),
        MAX_ACCEPTABLE_PERCENT_DIFFERENCE);
    sleep(10 * ANALYSIS_PERIOD);
    final int expectedSleepDuration = 110;
    validateLessThanOrEqual(expectedSleepDuration, analyzer.getSleepDuration());
  }

  /**
   * Ensure that there is waiting (sleepDuration != 0) if the metrics have
   * only been updated with many successful and failed requests.  Also ensure
   * that the sleepDuration decreases to zero over time.
   */
  @Test
  public void testManySuccessAndErrorsAndWaiting() {
    AbfsClientThrottlingAnalyzer analyzer = new AbfsClientThrottlingAnalyzer(
        "test",
        ANALYSIS_PERIOD);
    validate(0, analyzer.getSleepDuration());
    final int numberOfRequests = 20;
    for (int i = 0; i < numberOfRequests; i++) {
      analyzer.addBytesTransferred(8 * MEGABYTE, false);
      analyzer.addBytesTransferred(2 * MEGABYTE, true);
    }
    sleep(ANALYSIS_PERIOD_PLUS_10_PERCENT);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    analyzer.suspendIfNecessary();
    fuzzyValidate(7,
        timer.elapsedTimeMs(),
        MAX_ACCEPTABLE_PERCENT_DIFFERENCE);
    sleep(10 * ANALYSIS_PERIOD);
    validate(0, analyzer.getSleepDuration());
  }
}