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

import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.statistics.DurationTracker;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_FALLBACK_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_LISTING_LATENCY;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_PARSE_FAILURE_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_REQUEST_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_RESPONSE_COUNT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests verifying that the Photon (Apache Arrow based ListBlob) telemetry
 * counters and the listing latency duration tracker are registered on the ABFS
 * counters instance and can be incremented / tracked.
 */
public class TestPhotonListBlobMetrics {

  private static final URI TEST_URI =
      URI.create("abfs://container@account.dfs.core.windows.net/");

  private AbfsCounters newCounters() {
    return new AbfsCountersImpl(TEST_URI);
  }

  /**
   * Verify each Photon counter is registered and reflects increments.
   */
  @Test
  public void testPhotonCountersIncrement() {
    AbfsCounters counters = newCounters();

    counters.incrementCounter(PHOTON_REQUEST_COUNT, 3);
    counters.incrementCounter(PHOTON_RESPONSE_COUNT, 2);
    counters.incrementCounter(PHOTON_FALLBACK_COUNT, 1);
    counters.incrementCounter(PHOTON_PARSE_FAILURE_COUNT, 1);

    Map<String, Long> metricMap = counters.toMap();

    assertThat(metricMap)
        .as("Photon telemetry counters must be registered")
        .containsKeys(PHOTON_REQUEST_COUNT.getStatName(),
            PHOTON_RESPONSE_COUNT.getStatName(),
            PHOTON_FALLBACK_COUNT.getStatName(),
            PHOTON_PARSE_FAILURE_COUNT.getStatName());

    assertThat(metricMap.get(PHOTON_REQUEST_COUNT.getStatName())).isEqualTo(3L);
    assertThat(metricMap.get(PHOTON_RESPONSE_COUNT.getStatName())).isEqualTo(2L);
    assertThat(metricMap.get(PHOTON_FALLBACK_COUNT.getStatName())).isEqualTo(1L);
    assertThat(metricMap.get(PHOTON_PARSE_FAILURE_COUNT.getStatName()))
        .isEqualTo(1L);
  }

  /**
   * Verify the Photon listing latency duration tracker is registered and can be
   * tracked without error.
   */
  @Test
  public void testPhotonListingLatencyDurationTracker() {
    AbfsCounters counters = newCounters();

    try (DurationTracker tracker =
        counters.trackDuration(PHOTON_LISTING_LATENCY.getStatName())) {
      assertThat(tracker).isNotNull();
    }

    assertThat(counters.toMap().keySet())
        .as("Photon listing latency duration tracker must be registered")
        .anyMatch(key -> key.startsWith(PHOTON_LISTING_LATENCY.getStatName()));
  }
}
