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

package org.apache.hadoop.fs.s3a.statistics;

import java.time.Duration;

import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;

/**
 * interface to receive statistics events from the AWS SDK
 * by way of {@link AwsStatisticsCollector}.
 */
public interface StatisticsFromAwsSdk {

  /**
   * Record a number of AWS requests.
   * @param count number of events.
   */
  void updateAwsRequestCount(long count);

  /**
   * Record a number of AWS request retries.
   * @param count number of events.
   */
  void updateAwsRetryCount(long count);

  /**
   * Record a number of throttle exceptions received.
   * @param count number of events.
   */
  void updateAwsThrottleExceptionsCount(long count);

  /**
   * Record how long a request took overall.
   * @param duration duration of operation.
   */
  void noteAwsRequestTime(Duration duration);

  /**
   * Record how long a request took to execute on the
   * client.
   * @param duration duration of operation.
   */
  void noteAwsClientExecuteTime(Duration duration);

  /**
   * Record how long a request took to marshall into
   * XML.
   * @param duration duration of operation.
   */
  void noteRequestMarshallTime(Duration duration);

  /**
   * Record how long a request took to sign, including
   * any calls to EC2 credential endpoints.
   * @param duration duration of operation.
   */
  void noteRequestSigningTime(Duration duration);

  /**
   * Record how long it took to process the response.
   * @param duration duration of operation.
   */
  void noteResponseProcessingTime(Duration duration);
}
