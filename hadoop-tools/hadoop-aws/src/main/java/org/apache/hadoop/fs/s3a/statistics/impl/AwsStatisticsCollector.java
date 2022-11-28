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

package org.apache.hadoop.fs.s3a.statistics.impl;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;

/**
 * Collect statistics from the AWS SDK and forward to an instance of
 * {@link StatisticsFromAwsSdk} and thence into the S3A statistics.
 * <p>
 * See {@code com.facebook.presto.hive.s3.PrestoS3FileSystemMetricCollector}
 * for the inspiration for this.
 * <p>
 * See {@code software.amazon.awssdk.core.metrics.CoreMetric} for metric names.
 */
public class AwsStatisticsCollector implements MetricPublisher {

  /**
   * final destination of updates.
   */
  private final StatisticsFromAwsSdk collector;

  /**
   * Instantiate.
   * @param collector final destination of updates
   */
  public AwsStatisticsCollector(final StatisticsFromAwsSdk collector) {
    this.collector = collector;
  }

  /**
   * This is the callback from the AWS SDK where metrics
   * can be collected.
   * @param metricCollection metrics collection
   */
  @Override
  public void publish(MetricCollection metricCollection) {
    // MetricCollections are nested, so we need to traverse through their
    // "children" to collect the desired metrics. E.g.:
    //
    // ApiCall
    // ┌─────────────────────────────────────────┐
    // │ MarshallingDuration=PT0.002808333S      │
    // │ RetryCount=0                            │
    // │ ApiCallSuccessful=true                  │
    // │ OperationName=DeleteObject              │
    // │ ApiCallDuration=PT0.079801458S          │
    // │ CredentialsFetchDuration=PT0.000007083S │
    // │ ServiceId=S3                            │
    // └─────────────────────────────────────────┘
    //     ApiCallAttempt
    //     ┌─────────────────────────────────────────────────────────────────┐
    //     │ SigningDuration=PT0.000319375S                                  │
    //     │ ServiceCallDuration=PT0.078908584S                              │
    //     │ AwsExtendedRequestId=Kmvb2Sz8NuDgIFJPKzLLBhuHgQGmpAjVYBMrSHDvy= │
    //     │ HttpStatusCode=204                                              │
    //     │ BackoffDelayDuration=PT0S                                       │
    //     │ AwsRequestId=KR0XZCSX                                           │
    //     └─────────────────────────────────────────────────────────────────┘
    //         HttpClient
    //         ┌─────────────────────────────────┐
    //         │ AvailableConcurrency=1          │
    //         │ LeasedConcurrency=0             │
    //         │ ConcurrencyAcquireDuration=PT0S │
    //         │ PendingConcurrencyAcquires=0    │
    //         │ MaxConcurrency=96               │
    //         │ HttpClientName=Apache           │
    //         └─────────────────────────────────┘

    final long[] throttling = {0};
    recurseThroughChildren(metricCollection)
        .collect(Collectors.toList())
        .forEach(m -> {
          counter(m, CoreMetric.RETRY_COUNT, retries -> {
            collector.updateAwsRetryCount(retries);
            collector.updateAwsRequestCount(retries + 1);
          });

          counter(m, HttpMetric.HTTP_STATUS_CODE, statusCode -> {
            if (statusCode == HttpStatusCode.THROTTLING) {
              throttling[0] += 1;
            }
          });

          timing(m, CoreMetric.API_CALL_DURATION,
              collector::noteAwsClientExecuteTime);

          timing(m, CoreMetric.SERVICE_CALL_DURATION,
              collector::noteAwsRequestTime);

          timing(m, CoreMetric.MARSHALLING_DURATION,
              collector::noteRequestMarshallTime);

          timing(m, CoreMetric.SIGNING_DURATION,
              collector::noteRequestSigningTime);

          timing(m, CoreMetric.UNMARSHALLING_DURATION,
              collector::noteResponseProcessingTime);
        });

    collector.updateAwsThrottleExceptionsCount(throttling[0]);
  }

  @Override
  public void close() {

  }

  /**
   * Process a timing.
   * @param collection metric collection
   * @param metric metric
   * @param durationConsumer consumer
   */
  private void timing(
      MetricCollection collection,
      SdkMetric<Duration> metric,
      Consumer<Duration> durationConsumer) {
    collection
        .metricValues(metric)
        .forEach(v -> durationConsumer.accept(v));
  }

  /**
   * Process a counter.
   * @param collection metric collection
   * @param metric metric
   * @param consumer consumer
   */
  private void counter(
      MetricCollection collection,
      SdkMetric<Integer> metric,
      LongConsumer consumer) {
    collection
        .metricValues(metric)
        .forEach(v -> consumer.accept(v.longValue()));
  }

  /**
   * Metric collections can be nested. Exposes a stream of the given
   * collection and its nested children.
   * @param metrics initial collection
   * @return a stream of all nested metric collections
   */
  private static Stream<MetricCollection> recurseThroughChildren(
      MetricCollection metrics) {
    return Stream.concat(
        Stream.of(metrics),
        metrics.children().stream()
            .flatMap(c -> recurseThroughChildren(c)));
  }
}
