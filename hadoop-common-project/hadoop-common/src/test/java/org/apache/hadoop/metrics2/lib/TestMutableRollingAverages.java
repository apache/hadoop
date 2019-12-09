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
package org.apache.hadoop.metrics2.lib;

import com.google.common.base.Supplier;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.metrics2.lib.Interns.info;
import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * This class tests various cases of the algorithms implemented in
 * {@link MutableRollingAverages}.
 */
public class TestMutableRollingAverages {

  /**
   * Tests if the results are correct if no samples are inserted, dry run of
   * empty roll over.
   */
  @Test(timeout = 30000)
  public void testRollingAveragesEmptyRollover() throws Exception {
    final MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    /* 5s interval and 2 windows */
    try (MutableRollingAverages rollingAverages =
             new MutableRollingAverages("Time")) {
      rollingAverages.replaceScheduledTask(2, 5, TimeUnit.SECONDS);
      /* Check it initially */
      rollingAverages.snapshot(rb, true);
      verify(rb, never()).addGauge(
          info("FooRollingAvgTime", "Rolling average time for foo"), (long) 0);
      verify(rb, never()).addGauge(
          info("BarAvgTime", "Rolling average time for bar"), (long) 0);

      /* sleep 6s longer than 5s interval to wait for rollover done */
      Thread.sleep(6000);
      rollingAverages.snapshot(rb, false);
      verify(rb, never()).addGauge(
          info("FooRollingAvgTime", "Rolling average time for foo"), (long) 0);
      verify(rb, never()).addGauge(
          info("BarAvgTime", "Rolling average time for bar"), (long) 0);
    }
  }

  /**
   * Tests the case:
   * <p>
   * 5s interval and 2 sliding windows
   * </p>
   * <p>
   * sample stream: 1000 times 1, 2, and 3, respectively, e.g. [1, 1...1], [2,
   * 2...2] and [3, 3...3]
   * </p>
   */
  @Test(timeout = 30000)
  public void testRollingAveragesRollover() throws Exception {
    final MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    final String name = "foo2";
    final int windowSizeMs = 5000; // 5s roll over interval
    final int numWindows = 2;
    final int numOpsPerIteration = 1000;
    try (MutableRollingAverages rollingAverages =
             new MutableRollingAverages("Time")) {
      rollingAverages.replaceScheduledTask(2, 5000, TimeUnit.MILLISECONDS);
      /* Push values for three intervals */
      final long start = Time.monotonicNow();
      for (int i = 1; i <= 3; i++) {
        /* insert value */
        for (long j = 1; j <= numOpsPerIteration; j++) {
          rollingAverages.add(name, i);
        }

        /**
         * Sleep until 1s after the next windowSize seconds interval, to let the
         * metrics roll over
         */
        final long sleep = (start + (windowSizeMs * i) + 1000)
            - Time.monotonicNow();
        Thread.sleep(sleep);

        /* Verify that the window reset, check it has the values we pushed in */
        rollingAverages.snapshot(rb, false);

        /*
         * #1 window with a series of 1 1000
         * times, e.g. [1, 1...1], similarly, #2 window, e.g. [2, 2...2],
         * #3 window, e.g. [3, 3...3]
         */
        final double rollingSum = numOpsPerIteration * (i > 1 ? (i - 1) : 0)
            + numOpsPerIteration * i;
        /* one empty window or all 2 windows full */
        final long rollingTotal = i > 1 ? 2 * numOpsPerIteration
            : numOpsPerIteration;
        verify(rb).addGauge(
            info("[Foo2]RollingAvgTime", "Rolling average time for foo2"),
            rollingSum / rollingTotal);

        /* Verify the metrics were added the right number of times */
        verify(rb, times(i)).addGauge(
            eq(info("[Foo2]RollingAvgTime", "Rolling average time for foo2")),
            anyDouble());
      }
    }
  }

  /**
   * Test that MutableRollingAverages gives expected results after
   * initialization.
   * @throws Exception
   */
  @Test(timeout = 30000)
  public void testMutableRollingAveragesMetric() throws Exception {
    DummyTestMetric testMetric = new DummyTestMetric();
    testMetric.create();

    testMetric.add("metric1", 100);
    testMetric.add("metric1", 900);
    testMetric.add("metric2", 1000);
    testMetric.add("metric2", 1000);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        testMetric.collectThreadLocalStates();
        return testMetric.getStats().size() > 0;
      }
    }, 500, 5000);

    MetricsRecordBuilder rb = getMetrics(DummyTestMetric.METRIC_NAME);

    double metric1Avg = getDoubleGauge("[Metric1]RollingAvgTesting", rb);
    double metric2Avg = getDoubleGauge("[Metric2]RollingAvgTesting", rb);
    Assert.assertTrue("The rolling average of metric1 is not as expected",
        metric1Avg == 500.0);
    Assert.assertTrue("The rolling average of metric2 is not as expected",
        metric2Avg == 1000.0);

  }

  class DummyTestMetric {
    @Metric (valueName = "testing")
    private MutableRollingAverages rollingAverages;

    static final String METRIC_NAME = "RollingAveragesTestMetric";

    protected void create() {
      DefaultMetricsSystem.instance().register(METRIC_NAME,
          "mutable rolling averages test", this);
      rollingAverages.replaceScheduledTask(10, 1000, TimeUnit.MILLISECONDS);
    }

    void add(String name, long latency) {
      rollingAverages.add(name, latency);
    }

    void collectThreadLocalStates() {
      rollingAverages.collectThreadLocalStates();
    }

    Map<String, Double> getStats() {
      return rollingAverages.getStats(0);
    }

  }
}
