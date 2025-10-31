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

import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO_D;
import static org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamTestUtils.HUNDRED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for SlidingWindowHdrHistogram
 */
public class TestSlidingWindowHdrHistogram {

  private static final int TEST_ANALYSIS_WINDOW_SIZE_MS = 100;
  private static final int TEST_ANALYSIS_WINDOW_GRANULARITY = 5;
  private static final int TEST_MINIMUM_SAMPLE_SIZE = 7;
  private static final int TEST_TAIL_LATENCY_PERCENTILE = 99;
  private static final int TEST_MIN_DEVIATION_LOW = 0;
  private static final int TEST_MIN_DEVIATION_HIGH = 100;
  private static final int TEST_MAXIMUM_VALUE_RECORDED = 100;
  private static final int TEST_SIG_FIG = 3;
  private static final int TEST_SLEEP_INTERVAL_MS = 200;

  /**
   * Test the SlidingWindowHdrHistogram functionality.
   * @throws Exception in case of any failure
   */
  @Test
  public void testSlidingWindowHdrHistogram() throws Exception {
    SlidingWindowHdrHistogram histogram = new SlidingWindowHdrHistogram(
        HUNDRED, // Analysis window size in ms
        TEST_ANALYSIS_WINDOW_GRANULARITY, // Number of histogram slots in the analysis window
        TEST_MINIMUM_SAMPLE_SIZE,
        TEST_TAIL_LATENCY_PERCENTILE,
        TEST_MIN_DEVIATION_LOW,
        TEST_MAXIMUM_VALUE_RECORDED, // Maximum expected value
        TEST_SIG_FIG, // Number of significant digits
        AbfsRestOperationType.GetPathStatus);

    // Verify that the histogram is created successfully with default values and
    // do not report any percentiles
    assertThat(histogram).isNotNull();
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(ZERO);
    assertThat(histogram.getCurrentIndex()).isEqualTo(ZERO);
    assertThat(histogram.getP50()).isEqualTo(ZERO_D);
    assertThat(histogram.getTailLatency()).isEqualTo(ZERO_D);

    // Verify that recording values works as expected
    addAndRotate(histogram, 10, 5); // Add 5 values of 10
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(5);

    // Verify that percentiles are not computed with insufficient samples
    assertThat(histogram.getP50()).isEqualTo(ZERO_D);
    assertThat(histogram.getTailLatency()).isEqualTo(ZERO_D);

    // Record more values to exceed the minimum sample size
    addAndRotate(histogram, 20, 5); // Add 5 values of 20

    // Verify that percentiles are now computed but tail Latency is still not reported
    assertThat(histogram.getP50()).isGreaterThan(ZERO_D);
    assertThat(histogram.getTailLatency()).isEqualTo(ZERO_D);

    // Record more values and rotate histogram to fill whole analysis window
    addAndRotate(histogram, 30, 5); // Add 5 values of 30
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(15);

    // Verify that analysis window is not full until full rotation.
    assertThat(histogram.isAnalysisWindowFilled()).isFalse();

    addAndRotate(histogram, 60, 5); // Add 5 values of 60
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(20);

    // Verify that analysis window is not full until full rotation.
    assertThat(histogram.isAnalysisWindowFilled()).isFalse();

    // Verify that rotation is skipped if nothing new recorded and hence window not filled
    addAndRotate(histogram, 100, 0); // No new values added
    assertThat(histogram.isAnalysisWindowFilled()).isFalse();

    // Verify that rotation does not happen if analysis window is not filled
    histogram.rotateIfNeeded();
    assertThat(histogram.isAnalysisWindowFilled()).isFalse();

    addAndRotate(histogram, 80, 5); // Add 5 values of 80
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(25);

    // Verify that analysis window is full after full rotation.
    assertThat(histogram.isAnalysisWindowFilled()).isTrue();

    // Verify that percentiles and tail latency are computed
    assertThat(histogram.getP50()).isGreaterThan(0.0);
    assertThat(histogram.getTailLatency()).isGreaterThan(0.0);

    // Verify that sliding window works. Old values should be evicted
    addAndRotate(histogram, 90, 3); // Add 3 values of 90
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(23);
    assertThat(histogram.isAnalysisWindowFilled()).isTrue();
  }

  /**
   * Test that percentiles are not reported if minimum deviation requirement is not met.
   * @throws Exception in case of any failure
   */
  @Test
  public void testMinDeviationRequirementNotMet() throws Exception {
    SlidingWindowHdrHistogram histogram = new SlidingWindowHdrHistogram(
        TEST_ANALYSIS_WINDOW_SIZE_MS,
        TEST_ANALYSIS_WINDOW_GRANULARITY,
        TEST_MINIMUM_SAMPLE_SIZE,
        TEST_TAIL_LATENCY_PERCENTILE,
        TEST_MIN_DEVIATION_HIGH,
        TEST_MAXIMUM_VALUE_RECORDED,
        TEST_SIG_FIG,
        AbfsRestOperationType.GetPathStatus);

    // Add values with low deviation
    addAndRotate(histogram, 50, 5); // Add 5 values of 50
    addAndRotate(histogram, 51, 5); // Add 5 values of 52
    addAndRotate(histogram, 52, 5); // Add 5 values of 51
    addAndRotate(histogram, 80, 5); // Add 5 values of 53
    addAndRotate(histogram, 90, 5); // Add 5 values of 50

    // Verify that analysis window is full after full rotation.
    assertThat(histogram.isAnalysisWindowFilled()).isTrue();

    // Verify that percentiles are not computed due to low deviation
    assertThat(histogram.getP50()).isGreaterThan(0.0);
    assertThat(histogram.getTailLatency()).isEqualTo(0.0);
  }

  /**
   * Test that percentiles are reported if minimum deviation requirement is met.
   * @throws Exception in case of any failure
   */
  @Test
  public void testMinDeviationRequirementMet() throws Exception {
    SlidingWindowHdrHistogram histogram = new SlidingWindowHdrHistogram(
        100,
        5,
        7,
        99,
        50,
        100,
        3,
        AbfsRestOperationType.GetPathStatus);

    // Add values with low deviation
    addAndRotate(histogram, 50, 5); // Add 5 values of 50
    addAndRotate(histogram, 51, 5); // Add 5 values of 52
    addAndRotate(histogram, 52, 5); // Add 5 values of 51
    addAndRotate(histogram, 80, 5); // Add 5 values of 53
    addAndRotate(histogram, 90, 5); // Add 5 values of 50

    // Verify that analysis window is full after full rotation.
    assertThat(histogram.isAnalysisWindowFilled()).isTrue();

    // Verify that percentiles are computed.
    assertThat(histogram.getP50()).isGreaterThan(0.0);
    assertThat(histogram.getTailLatency()).isGreaterThan(0.0);
  }

  private void addAndRotate(SlidingWindowHdrHistogram histogram, int value, int times)
      throws InterruptedException {
    for (int i = 0; i < times; i++) {
      histogram.recordValue(value);
    }
    Thread.sleep(TEST_SLEEP_INTERVAL_MS); // Sleep to allow rotation
    histogram.rotateIfNeeded();
    histogram.computeLatency();
  }
}
