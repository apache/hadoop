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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_PATCH_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_POST_REQUEST;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMeanStatistic;

public class TestAbfsNetworkStatistics extends AbstractAbfsIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAbfsNetworkStatistics.class);
  private static final int LARGE_OPERATIONS = 1000;
  private static final AbfsStatistic[] HTTP_DURATION_TRACKER_LIST = {
      HTTP_POST_REQUEST,
      HTTP_PATCH_REQUEST
  };

  public TestAbfsNetworkStatistics() throws Exception {
  }

  /**
   * Test to check correct values of read and write throttling statistics in
   * {@code AbfsClientThrottlingAnalyzer}.
   */
  @Test
  public void testAbfsThrottlingStatistics() throws IOException {
    describe("Test to check correct values of read throttle and write "
        + "throttle statistics in Abfs");

    AbfsCounters statistics =
        new AbfsCountersImpl(getFileSystem().getUri());

    /*
     * Calling the throttle methods to check correct summation and values of
     * the counters.
     */
    for (int i = 0; i < LARGE_OPERATIONS; i++) {
      statistics.incrementCounter(AbfsStatistic.READ_THROTTLES, 1);
      statistics.incrementCounter(AbfsStatistic.WRITE_THROTTLES, 1);
    }

    Map<String, Long> metricMap = statistics.toMap();

    /*
     * Test to check read and write throttle statistics gave correct values for
     * 1000 calls.
     */
    assertAbfsStatistics(AbfsStatistic.READ_THROTTLES, LARGE_OPERATIONS,
        metricMap);
    assertAbfsStatistics(AbfsStatistic.WRITE_THROTTLES, LARGE_OPERATIONS,
        metricMap);
  }

  /**
   * Test to check if the DurationTrackers are tracking as expected whilst
   * doing some work.
   */
  @Test
  public void testAbfsNetworkDurationTrackers()
      throws IOException, InterruptedException {
    describe("Test to verify the actual values of DurationTrackers are "
        + "greater than 0.0 while tracking some work.");

    AbfsCounters abfsCounters = new AbfsCountersImpl(getFileSystem().getUri());
    // Start dummy work for the DurationTrackers and start tracking.
    try (DurationTracker ignoredPatch =
        abfsCounters.trackDuration(AbfsStatistic.getStatNameFromHttpCall(AbfsHttpConstants.HTTP_METHOD_PATCH));
        DurationTracker ignoredPost =
            abfsCounters.trackDuration(AbfsStatistic.getStatNameFromHttpCall(AbfsHttpConstants.HTTP_METHOD_POST))
    ) {
      // Emulates doing some work.
      Thread.sleep(10);
      LOG.info("Execute some Http requests...");
    }

    // Extract the iostats from the abfsCounters instance.
    IOStatistics ioStatistics = extractStatistics(abfsCounters);
    // Asserting that the durationTrackers have mean > 0.0.
    for (AbfsStatistic abfsStatistic : HTTP_DURATION_TRACKER_LIST) {
      Assertions.assertThat(lookupMeanStatistic(ioStatistics,
          abfsStatistic.getStatName() + StoreStatisticNames.SUFFIX_MEAN).mean())
          .describedAs("The DurationTracker Named " + abfsStatistic.getStatName()
                  + " Doesn't match the expected value")
          .isGreaterThan(0.0);
    }
  }

  /**
   * Test to check if abfs counter for HTTP 503 statusCode works correctly
   * when incremented.
   */
  @Test
  public void testAbfsHTTP503ErrorCounter() throws IOException {
    describe("tests to verify the expected value of the HTTP 503 error "
        + "counter is equal to number of times incremented.");

    AbfsCounters abfsCounters = new AbfsCountersImpl(getFileSystem().getUri());
    // Incrementing the server_unavailable counter.
    for (int i = 0; i < LARGE_OPERATIONS; i++) {
      abfsCounters.incrementCounter(AbfsStatistic.SERVER_UNAVAILABLE, 1);
    }
    // Getting the IOStatistics counter map from abfsCounters.
    Map<String, Long> metricsMap = abfsCounters.toMap();
    assertAbfsStatistics(AbfsStatistic.SERVER_UNAVAILABLE, LARGE_OPERATIONS,
        metricsMap);
  }
}
