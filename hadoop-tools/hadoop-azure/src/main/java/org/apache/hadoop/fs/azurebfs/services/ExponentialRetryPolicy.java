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

import java.util.Random;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Retry policy used by AbfsClient.
 * */
public class ExponentialRetryPolicy extends AbfsRetryPolicy {
  /**
   * Represents the default amount of time used when calculating a random delta in the exponential
   * delay between retries.
   */
  private static final int DEFAULT_CLIENT_BACKOFF = 1000 * 3;

  /**
   * Represents the default maximum amount of time used when calculating the exponential
   * delay between retries.
   */
  private static final int DEFAULT_MAX_BACKOFF = 1000 * 30;

  /**
   * Represents the default minimum amount of time used when calculating the exponential
   * delay between retries.
   */
  private static final int DEFAULT_MIN_BACKOFF = 1000 * 3;

  /**
   *  The minimum random ratio used for delay interval calculation.
   */
  private static final double MIN_RANDOM_RATIO = 0.8;

  /**
   *  The maximum random ratio used for delay interval calculation.
   */
  private static final double MAX_RANDOM_RATIO = 1.2;

  /**
   *  Holds the random number generator used to calculate randomized backoff intervals
   */
  private final Random randRef = new Random();

  /**
   * The value that will be used to calculate a random delta in the exponential delay interval
   */
  private final int deltaBackoff;

  /**
   * The maximum backoff time.
   */
  private final int maxBackoff;

  /**
   * The minimum backoff time.
   */
  private final int minBackoff;

  /**
   * Initializes a new instance of the {@link ExponentialRetryPolicy} class.
   */
  public ExponentialRetryPolicy(final int maxIoRetries) {

    this(maxIoRetries, DEFAULT_MIN_BACKOFF, DEFAULT_MAX_BACKOFF,
        DEFAULT_CLIENT_BACKOFF);
  }

  /**
   * Initializes a new instance of the {@link ExponentialRetryPolicy} class.
   *
   * @param conf The {@link AbfsConfiguration} from which to retrieve retry configuration.
   */
  public ExponentialRetryPolicy(AbfsConfiguration conf) {
    this(conf.getMaxIoRetries(), conf.getMinBackoffIntervalMilliseconds(), conf.getMaxBackoffIntervalMilliseconds(),
        conf.getBackoffIntervalMilliseconds());
  }

  /**
   * Initializes a new instance of the {@link ExponentialRetryPolicy} class.
   *
   * @param maxRetryCount The maximum number of retry attempts.
   * @param minBackoff The minimum backoff time.
   * @param maxBackoff The maximum backoff time.
   * @param deltaBackoff The value that will be used to calculate a random delta in the exponential delay
   *                     between retries.
   */
  public ExponentialRetryPolicy(final int maxRetryCount, final int minBackoff, final int maxBackoff, final int deltaBackoff) {
    super(maxRetryCount, RetryPolicyConstants.EXPONENTIAL_RETRY_POLICY_ABBREVIATION);
    this.minBackoff = minBackoff;
    this.maxBackoff = maxBackoff;
    this.deltaBackoff = deltaBackoff;
  }

  /**
   * Returns backoff interval between 80% and 120% of the desired backoff,
   * multiply by 2^n-1 for exponential.
   *
   * @param retryCount The current retry attempt count.
   * @return backoff Interval time
   */
  @Override
  public long getRetryInterval(final int retryCount) {
    final long boundedRandDelta = (int) (this.deltaBackoff * MIN_RANDOM_RATIO)
        + this.randRef.nextInt((int) (this.deltaBackoff * MAX_RANDOM_RATIO)
        - (int) (this.deltaBackoff * MIN_RANDOM_RATIO));

    final double incrementDelta = (Math.pow(2, retryCount - 1)) * boundedRandDelta;

    final long retryInterval = (int) Math.round(Math.min(
            this.minBackoff + incrementDelta, maxBackoff));

    return retryInterval;
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
  int getDeltaBackoff() {
    return this.deltaBackoff;
  }

}
