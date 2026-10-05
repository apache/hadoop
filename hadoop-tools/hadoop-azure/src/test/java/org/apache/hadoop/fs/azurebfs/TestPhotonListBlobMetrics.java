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

import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.statistics.DurationTracker;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_FALLBACK_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_LISTING_LATENCY;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_PARSE_FAILURE_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_REQUEST_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_RESPONSE_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_APACHE_ARROW_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.withSettings;

/**
 * Unit tests verifying that the Photon (Apache Arrow based ListBlob) telemetry
 * counters and the listing latency duration tracker are registered on the ABFS
 * counters instance and can be incremented / tracked, and that the Photon
 * metric-update rules in {@link AbfsBlobClient} fire only under the intended
 * conditions.
 */
public class TestPhotonListBlobMetrics {

  private static final URI TEST_URI =
      URI.create("abfs://container@account.dfs.core.windows.net/");

  private AbfsCounters newCounters() {
    return new AbfsCountersImpl(TEST_URI);
  }

  /**
   * Verify each Photon counter starts at zero on a freshly constructed counters
   * instance (all Photon counters are registered at construction). A counter
   * registered with a stale or non-zero initial value would otherwise go
   * unnoticed.
   */
  @Test
  public void testPhotonCountersStartAtZero() {
    Map<String, Long> metricMap = newCounters().toMap();

    assertThat(metricMap)
        .containsEntry(PHOTON_REQUEST_COUNT.getStatName(), 0L)
        .containsEntry(PHOTON_RESPONSE_COUNT.getStatName(), 0L)
        .containsEntry(PHOTON_FALLBACK_COUNT.getStatName(), 0L)
        .containsEntry(PHOTON_PARSE_FAILURE_COUNT.getStatName(), 0L);
  }

  /**
   * Verify {@code updatePhotonResponseMetrics} only classifies successful
   * (HTTP 200) listings: an Arrow 200 counts a response, an XML 200 counts a
   * fallback, and a non-200 (e.g. 409) is ignored so an error body is never
   * mistaken for an XML fallback.
   */
  @Test
  public void testResponseMetricsOnlyClassifySuccessfulListings()
      throws Exception {
    AbfsCounters counters = newCounters();
    AbfsBlobClient client = clientWithCounters(counters);

    invokeResponseMetrics(client, true,
        result(HttpURLConnection.HTTP_OK, APPLICATION_APACHE_ARROW_STREAM));
    invokeResponseMetrics(client, true,
        result(HttpURLConnection.HTTP_OK, APPLICATION_XML));
    // Non-200 Arrow response: an error body, not a genuine fallback - ignored.
    invokeResponseMetrics(client, true,
        result(HttpURLConnection.HTTP_CONFLICT, APPLICATION_APACHE_ARROW_STREAM));
    // Photon not requested: nothing is classified.
    invokeResponseMetrics(client, false,
        result(HttpURLConnection.HTTP_OK, APPLICATION_XML));

    Map<String, Long> metricMap = counters.toMap();
    assertThat(metricMap.get(PHOTON_RESPONSE_COUNT.getStatName())).isEqualTo(1L);
    assertThat(metricMap.get(PHOTON_FALLBACK_COUNT.getStatName())).isEqualTo(1L);
  }

  /**
   * Verify {@code updatePhotonParseFailureMetric} fires only when the failing
   * response was actually Arrow (an XML fallback failure is an XML-parser
   * problem, not a Photon one) and only when Photon was requested.
   */
  @Test
  public void testParseFailureMetricOnlyForArrowResponses() throws Exception {
    AbfsCounters counters = newCounters();
    AbfsBlobClient client = clientWithCounters(counters);

    invokeParseFailureMetric(client, true,
        result(HttpURLConnection.HTTP_OK, APPLICATION_APACHE_ARROW_STREAM));
    // XML response failing to parse is not a Photon parse failure.
    invokeParseFailureMetric(client, true,
        result(HttpURLConnection.HTTP_OK, APPLICATION_XML));
    // Photon not requested: never a Photon parse failure.
    invokeParseFailureMetric(client, false,
        result(HttpURLConnection.HTTP_OK, APPLICATION_APACHE_ARROW_STREAM));

    assertThat(counters.toMap().get(PHOTON_PARSE_FAILURE_COUNT.getStatName()))
        .isEqualTo(1L);
  }

  /**
   * Verify {@code updatePhotonRequestMetric} counts one request per invocation
   * when Photon was requested (so the extra call on the rename-recovery retry is
   * counted as a distinct request) and is a no-op when Photon was not requested.
   */
  @Test
  public void testRequestMetricCountedPerRequestIncludingRetry()
      throws Exception {
    AbfsCounters counters = newCounters();
    AbfsBlobClient client = clientWithCounters(counters);

    // Initial listing request plus the rename-recovery retry request.
    invokeRequestMetric(client, true);
    invokeRequestMetric(client, true);
    // A listing where Photon was not requested must not be counted.
    invokeRequestMetric(client, false);

    assertThat(counters.toMap().get(PHOTON_REQUEST_COUNT.getStatName()))
        .isEqualTo(2L);
  }

  private AbfsBlobClient clientWithCounters(final AbfsCounters counters) {
    AbfsBlobClient client = mock(AbfsBlobClient.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS));
    doReturn(counters).when(client).getAbfsCounters();
    return client;
  }

  private AbfsHttpOperation result(final int statusCode,
      final String contentType) {
    AbfsHttpOperation op = mock(AbfsHttpOperation.class);
    doReturn(statusCode).when(op).getStatusCode();
    doReturn(contentType).when(op).getResponseHeaderIgnoreCase(CONTENT_TYPE);
    return op;
  }

  private static void invokeResponseMetrics(final AbfsBlobClient client,
      final boolean photonRequested, final AbfsHttpOperation result)
      throws Exception {
    Method method = AbfsBlobClient.class.getDeclaredMethod(
        "updatePhotonResponseMetrics", boolean.class, AbfsHttpOperation.class);
    method.setAccessible(true);
    method.invoke(client, photonRequested, result);
  }

  private static void invokeParseFailureMetric(final AbfsBlobClient client,
      final boolean photonRequested, final AbfsHttpOperation result)
      throws Exception {
    Method method = AbfsBlobClient.class.getDeclaredMethod(
        "updatePhotonParseFailureMetric", boolean.class,
        AbfsHttpOperation.class);
    method.setAccessible(true);
    method.invoke(client, photonRequested, result);
  }

  private static void invokeRequestMetric(final AbfsBlobClient client,
      final boolean photonRequested) throws Exception {
    Method method = AbfsBlobClient.class.getDeclaredMethod(
        "updatePhotonRequestMetric", boolean.class);
    method.setAccessible(true);
    method.invoke(client, photonRequested);
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
