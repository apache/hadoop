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

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.util.TimingInfo;

import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;

import static com.amazonaws.util.AWSRequestMetrics.Field.ClientExecuteTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpClientRetryCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpRequestTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestMarshallTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestSigningTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.ResponseProcessingTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.ThrottleException;

/**
 * Collect statistics from the AWS SDK and forward to an instance of
 * {@link StatisticsFromAwsSdk} and thence into the S3A statistics.
 * <p>
 * See {@code com.facebook.presto.hive.s3.PrestoS3FileSystemMetricCollector}
 * for the inspiration for this.
 * <p>
 * See {@code com.amazonaws.util.AWSRequestMetrics} for metric names.
 */
public class AwsStatisticsCollector extends RequestMetricCollector {

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
   * @param request AWS request
   * @param response AWS response
   */
  @Override
  public void collectMetrics(
      final Request<?> request,
      final Response<?> response) {

    TimingInfo timingInfo = request.getAWSRequestMetrics().getTimingInfo();

    counter(timingInfo, HttpClientRetryCount.name(),
        collector::updateAwsRetryCount);
    counter(timingInfo, RequestCount.name(),
        collector::updateAwsRequestCount);
    counter(timingInfo, ThrottleException.name(),
        collector::updateAwsThrottleExceptionsCount);

    timing(timingInfo, ClientExecuteTime.name(),
        collector::noteAwsClientExecuteTime);
    timing(timingInfo, HttpRequestTime.name(),
        collector::noteAwsRequestTime);
    timing(timingInfo, RequestMarshallTime.name(),
        collector::noteRequestMarshallTime);
    timing(timingInfo, RequestSigningTime.name(),
        collector::noteRequestSigningTime);
    timing(timingInfo, ResponseProcessingTime.name(),
        collector::noteResponseProcessingTime);
  }

  /**
   * Process a timing.
   * @param timingInfo timing info
   * @param subMeasurementName sub measurement
   * @param durationConsumer consumer
   */
  private void timing(
      TimingInfo timingInfo,
      String subMeasurementName,
      Consumer<Duration> durationConsumer) {
    TimingInfo t1 = timingInfo.getSubMeasurement(subMeasurementName);
    if (t1 != null && t1.getTimeTakenMillisIfKnown() != null) {
      durationConsumer.accept(Duration.ofMillis(
          t1.getTimeTakenMillisIfKnown().longValue()));
    }
  }

  /**
   * Process a counter.
   * @param timingInfo timing info
   * @param subMeasurementName sub measurement
   * @param consumer consumer
   */
  private void counter(
      TimingInfo timingInfo,
      String subMeasurementName,
      LongConsumer consumer) {
    Number n = timingInfo.getCounter(subMeasurementName);
    if (n != null) {
      consumer.accept(n.longValue());
    }
  }
}
