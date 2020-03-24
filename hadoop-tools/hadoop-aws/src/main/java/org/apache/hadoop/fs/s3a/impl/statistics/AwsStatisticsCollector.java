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

package org.apache.hadoop.fs.s3a.impl.statistics;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.util.TimingInfo;

import static com.amazonaws.util.AWSRequestMetrics.Field.ClientExecuteTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpClientRetryCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpRequestTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestMarshallTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestSigningTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.ResponseProcessingTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.ThrottleException;

/**
 * Collect statistics from the AWS SDK and update our statistics.
 *
 * See {@code com.facebook.presto.hive.s3.PrestoS3FileSystemMetricCollector}
 * for the inspiration for this.
 * See {@code com.amazonaws.util.AWSRequestMetrics} for metric names.
 */
public class AwsStatisticsCollector extends RequestMetricCollector {

  /**
   * final destination of updates.
   */
  private final StatisticsFromAwsSdk collector;

  /**
   * instantiate.
   * @param collector final destination of updates
   */
  public AwsStatisticsCollector(final StatisticsFromAwsSdk collector) {
    this.collector = collector;
  }

  @Override
  public void collectMetrics(
      final Request<?> request,
      final Response<?> response) {

    TimingInfo timingInfo = request.getAWSRequestMetrics().getTimingInfo();
    counter(timingInfo, RequestCount.name(),
        collector::updateAwsRequestCount);
    counter(timingInfo, HttpClientRetryCount.name(),
        collector::updateAwsRetryCount);
    counter(timingInfo, ThrottleException.name(),
        collector::updateAwsThrottleExceptionsCount);
    timing(timingInfo, HttpRequestTime.name(),
        collector::addAwsRequestTime);
    timing(timingInfo, ClientExecuteTime.name(),
        collector::addAwsClientExecuteTime);
    timing(timingInfo, RequestMarshallTime.name(),
        collector::addRequestMarshallTime);
    timing(timingInfo, RequestSigningTime.name(),
        collector::addRequestSigningTime);
    timing(timingInfo, ResponseProcessingTime.name(),
        collector::addResponseProcessingTime);

  }

  /**
   * Process a timing.
   * @param t timing info
   * @param subMeasurementName sub measurement
   * @param durationConsumer consumer
   */
  private void timing(TimingInfo t,
      String subMeasurementName,
      Consumer<Duration> durationConsumer) {
    TimingInfo t1 = t.getSubMeasurement(subMeasurementName);
    if (t1 != null && t1.getTimeTakenMillisIfKnown() != null) {
      durationConsumer.accept(Duration.ofMillis(
          t1.getTimeTakenMillisIfKnown().longValue()));
    }
  }

  /**
   * Process a counter.
   * @param t timing info
   * @param subMeasurementName sub measurement
   * @param consumer consumer
   */
  private void counter(TimingInfo t,
      String subMeasurementName,
      LongConsumer consumer) {
    Number n = t.getCounter(subMeasurementName);
    if (n != null) {
      consumer.accept(n.longValue());
    }
  }
}
