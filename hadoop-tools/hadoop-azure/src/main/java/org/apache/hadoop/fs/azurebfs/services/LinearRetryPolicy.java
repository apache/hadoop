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

import java.net.HttpURLConnection;

import org.checkerframework.checker.units.qual.min;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_CONTINUE;

/**
 * Retry policy used by AbfsClient.
 * */
public class LinearRetryPolicy {
  
  /**
   * Represents the default maximum amount of time used when calculating the exponential
   * delay between retries.
   */
  private static final int DEFAULT_MAX_BACKOFF = 1000 * 30; // 30s

  /**
   * Represents the default minimum amount of time used when calculating the exponential
   * delay between retries.
   */
  private static final int DEFAULT_MIN_BACKOFF = 500 * 1; // 500ms

  /**
   * The maximum backoff time.
   */
  private final int maxBackoff;

  /**
   * The minimum backoff time.
   */
  private final int minBackoff;

  /**
   * The maximum number of retry attempts.
   */
  private final int maxRetryCount;

  /**
   * Whether we want to double up the retry interval
   * True: Double Up
   * False: Increase by 1.
   */
  private final boolean doubleStepUpEnabled;

  /**
   * Initializes a new instance of the {@link org.apache.hadoop.fs.azurebfs.services.LinearRetryPolicy} class.
   */
  public LinearRetryPolicy(final int maxIoRetries) {

    this(maxIoRetries, DEFAULT_MIN_BACKOFF, DEFAULT_MAX_BACKOFF,
        true);
  }

  /**
   * Initializes a new instance of the {@link org.apache.hadoop.fs.azurebfs.services.LinearRetryPolicy} class.
   *
   * @param conf The {@link org.apache.hadoop.fs.azurebfs.AbfsConfiguration} from which to retrieve retry configuration.
   */
  public LinearRetryPolicy(AbfsConfiguration conf) {
    this(conf.getMaxIoRetries(),
        conf.getMinBackoffIntervalMillisecondsForConnectionTimeout(),
        conf.getMaxBackoffIntervalMillisecondsForConnectionTimeout(),
        conf.getLinearRetryDoubleStepUpEnabled());
  }

  /**
   * Initializes a new instance of the {@link org.apache.hadoop.fs.azurebfs.services.LinearRetryPolicy} class.
   *
   * @param maxRetryCount The maximum number of retry attempts.
   * @param minBackoff The minimum backoff time.
   * @param maxBackoff The maximum backoff time.
   * @param doubleStepUpEnabled Type of linear increment, double or increment
   */
  public LinearRetryPolicy(final int maxRetryCount, final int minBackoff, final int maxBackoff, final boolean doubleStepUpEnabled) {
    this.maxRetryCount = maxRetryCount;
    this.minBackoff = minBackoff;
    this.maxBackoff = maxBackoff;
    this.doubleStepUpEnabled = doubleStepUpEnabled;
  }

  /**
   * Returns backoff interval between 80% and 120% of the desired backoff,
   * multiply by 2^n-1 for exponential.
   *
   * @param retryCount The current retry attempt count.
   * @return backoff Interval time
   */
  public long getRetryInterval(final int retryCount) {
    final double incrementDelta = doubleStepUpEnabled
        ? minBackoff * Math.pow(2, retryCount)
        : minBackoff * retryCount;

    final long retryInterval = (int) Math.round(Math.min(minBackoff + incrementDelta, maxBackoff));

    return retryInterval;
  }

  @VisibleForTesting
  int getRetryCount() {
    return this.maxRetryCount;
  }

  @VisibleForTesting
  int getMinBackoff() {
    return this.minBackoff;
  }

  @VisibleForTesting
  int getMaxBackoff() {
    return maxBackoff;
  }

  @VisibleForTesting
  boolean getDoubleStepUpEnabled() {
    return this.doubleStepUpEnabled;
  }

}
