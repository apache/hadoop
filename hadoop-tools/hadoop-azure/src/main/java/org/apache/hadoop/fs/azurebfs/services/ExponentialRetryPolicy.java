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
import java.net.HttpURLConnection;

class ExponentialRetryPolicy {
  /**
   * Represents the default number of retry attempts.
   */
  private static final int DEFAULT_CLIENT_RETRY_COUNT = 30;

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
   *Represents the default minimum amount of time used when calculating the exponential
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
   * The maximum number of retry attempts.
   */
  private final int retryCount;

  /**
   * Initializes a new instance of the {@link ExponentialRetryPolicy} class.
   */
  ExponentialRetryPolicy() {
    this(DEFAULT_CLIENT_RETRY_COUNT, DEFAULT_MIN_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_CLIENT_BACKOFF);
  }

  /**
   * Initializes a new instance of the {@link ExponentialRetryPolicy} class.
   *
   * @param retryCount The maximum number of retry attempts.
   * @param minBackoff The minimum backoff time.
   * @param maxBackoff The maximum backoff time.
   * @param deltaBackoff The value that will be used to calculate a random delta in the exponential delay
   *                     between retries.
   */
  ExponentialRetryPolicy(final int retryCount, final int minBackoff, final int maxBackoff, final int deltaBackoff) {
    this.retryCount = retryCount;
    this.minBackoff = minBackoff;
    this.maxBackoff = maxBackoff;
    this.deltaBackoff = deltaBackoff;
  }

  /**
   * Returns if a request should be retried based on the retry count, current response,
   * and the current strategy.
   *
   * @param retryCount The current retry attempt count.
   * @param statusCode The status code of the response, or -1 for socket error.
   * @return true if the request should be retried; false otherwise.
   */
  public boolean shouldRetry(final int retryCount, final int statusCode) {
    return retryCount < this.retryCount
        && (statusCode == -1
        || statusCode == HttpURLConnection.HTTP_CLIENT_TIMEOUT
        || (statusCode >= HttpURLConnection.HTTP_INTERNAL_ERROR
            && statusCode != HttpURLConnection.HTTP_NOT_IMPLEMENTED
            && statusCode != HttpURLConnection.HTTP_VERSION));
  }

  /**
   * Returns backoff interval between 80% and 120% of the desired backoff,
   * multiply by 2^n-1 for exponential.
   *
   * @param retryCount The current retry attempt count.
   * @return backoff Interval time
   */
  public long getRetryInterval(final int retryCount) {
    final long boundedRandDelta = (int) (this.deltaBackoff * MIN_RANDOM_RATIO)
        + this.randRef.nextInt((int) (this.deltaBackoff * MAX_RANDOM_RATIO)
        - (int) (this.deltaBackoff * MIN_RANDOM_RATIO));

    final double incrementDelta = (Math.pow(2, retryCount - 1)) * boundedRandDelta;

    final long retryInterval = (int) Math.round(Math.min(this.minBackoff + incrementDelta, maxBackoff));

    return retryInterval;
  }
}
