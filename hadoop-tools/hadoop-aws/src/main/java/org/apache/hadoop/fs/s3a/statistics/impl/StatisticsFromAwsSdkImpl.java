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

import org.apache.hadoop.fs.s3a.statistics.CountersAndGauges;
import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;

import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_RETRY;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLED;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_THROTTLE_RATE;

/**
 * Hook up AWS SDK Statistics to the S3 counters.
 * <p>
 * Durations are not currently being used; that could be
 * changed in future once an effective strategy for reporting
 * them is determined.
 */
public final class StatisticsFromAwsSdkImpl implements
    StatisticsFromAwsSdk {

  private final CountersAndGauges countersAndGauges;

  public StatisticsFromAwsSdkImpl(
      final CountersAndGauges countersAndGauges) {
    this.countersAndGauges = countersAndGauges;
  }

  @Override
  public void updateAwsRequestCount(final long count) {
    countersAndGauges.incrementCounter(STORE_IO_REQUEST, count);
  }

  @Override
  public void updateAwsRetryCount(final long count) {
    countersAndGauges.incrementCounter(STORE_IO_RETRY, count);
  }

  @Override
  public void updateAwsThrottleExceptionsCount(final long count) {
    countersAndGauges.incrementCounter(STORE_IO_THROTTLED, count);
    countersAndGauges.addValueToQuantiles(STORE_IO_THROTTLE_RATE, count);
  }

  @Override
  public void noteAwsRequestTime(final Duration duration) {

  }

  @Override
  public void noteAwsClientExecuteTime(final Duration duration) {

  }

  @Override
  public void noteRequestMarshallTime(final Duration duration) {

  }

  @Override
  public void noteRequestSigningTime(final Duration duration) {

  }

  @Override
  public void noteResponseProcessingTime(final Duration duration) {

  }
}
