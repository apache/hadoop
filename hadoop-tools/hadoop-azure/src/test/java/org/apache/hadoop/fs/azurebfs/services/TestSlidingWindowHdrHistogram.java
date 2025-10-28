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

import static org.assertj.core.api.Assertions.assertThat;

public class TestSlidingWindowHdrHistogram {

  @Test
  public void testSlidingWindowHdrHistogram() throws Exception {
    SlidingWindowHdrHistogram histogram = new SlidingWindowHdrHistogram(
        100, // Analysis window size in ms
        5, // Number of histogram slots in the analysis window
        7, // Minimum number of samples to compute percentiles
        99, // Percentile to compute
        0, // Minimum deviation percentage to report tail latency
        100, // Maximum expected value
        3, // Number of significant digits
        AbfsRestOperationType.GetPathStatus);

    // Verify that the histogram is created successfully with default values and
    // do not report any percentiles
    assertThat(histogram).isNotNull();
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(0);
    assertThat(histogram.getCurrentIndex()).isEqualTo(0);
    assertThat(histogram.getP50()).isEqualTo(0.0);
    assertThat(histogram.getTailLatency()).isEqualTo(0.0);

    // Verify that recording values works as expected
    addAndRotate(histogram, 10, 5); // Add 5 values of 10
    assertThat(histogram.getCurrentTotalCount()).isEqualTo(5);

    // Verify that percentiles are not computed with insufficient samples
    assertThat(histogram.getP50()).isEqualTo(0.0);
    assertThat(histogram.getTailLatency()).isEqualTo(0.0);

    // Record more values to exceed the minimum sample size
    addAndRotate(histogram, 20, 5); // Add 5 values of 20

    // Verify that percentiles are now computed but tail Latency is still not reported
    assertThat(histogram.getP50()).isGreaterThan(0.0);
    assertThat(histogram.getTailLatency()).isEqualTo(0.0);

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

  @Test
  public void testMinDeviationRequirementNotMet() throws Exception {
    SlidingWindowHdrHistogram histogram = new SlidingWindowHdrHistogram(
        100,
        5,
        7,
        99,
        100,
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

    // Verify that percentiles are not computed due to low deviation
    assertThat(histogram.getP50()).isGreaterThan(0.0);
    assertThat(histogram.getTailLatency()).isEqualTo(0.0);
  }

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
    Thread.sleep(200); // Sleep to allow rotation
    histogram.rotateIfNeeded();
    histogram.computeLatency();
  }
}
