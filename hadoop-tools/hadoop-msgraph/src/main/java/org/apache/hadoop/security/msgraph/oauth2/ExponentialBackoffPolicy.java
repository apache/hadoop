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

package org.apache.hadoop.security.msgraph.oauth2;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


/**
 * Policy to retry a call for OAuth2 using exponential backoff.
 */
public class ExponentialBackoffPolicy implements RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      ExponentialBackoffPolicy.class);

  /** Number of retries. */
  private int retryCount = 0;
  /** Maximum retries.*/
  private int maxRetries;
  /** Exponential retry interval in milliseconds.*/
  private int exponentialRetryInterval;
  /** Exponential factor. */
  private int exponentialFactor;
  /** Last attempt start time in milliseconds. */
  private long lastAttemptStartTime;

  public ExponentialBackoffPolicy(
      int maxRetries, int exponentialRetryInterval, int exponentialFactor) {
    this.maxRetries = maxRetries;
    this.exponentialRetryInterval = exponentialRetryInterval;
    this.exponentialFactor = exponentialFactor;
    setLastAttemptStartTime();
  }

  @Override
  public boolean shouldRetry(int httpResponseCode, Exception lastException) {
    if ((httpResponseCode < HttpStatus.MULTIPLE_CHOICES_300 ||
        httpResponseCode >= HttpStatus.INTERNAL_SERVER_ERROR_500 ||
        httpResponseCode == HttpStatus.REQUEST_TIMEOUT_408 ||
        httpResponseCode == HttpStatus.TOO_MANY_REQUESTS_429 ||
        httpResponseCode == HttpStatus.UNAUTHORIZED_401) &&
        httpResponseCode != HttpStatus.NOT_IMPLEMENTED_501 &&
        httpResponseCode != HttpStatus.HTTP_VERSION_NOT_SUPPORTED_505) {
      if (lastException == null &&
          httpResponseCode < HttpStatus.INTERNAL_SERVER_ERROR_500 &&
          httpResponseCode != HttpStatus.REQUEST_TIMEOUT_408 &&
          httpResponseCode != HttpStatus.TOO_MANY_REQUESTS_429 &&
          httpResponseCode != HttpStatus.UNAUTHORIZED_401) {
        return httpResponseCode < HttpStatus.CONTINUE_100;
      } else if (this.retryCount < this.maxRetries) {
        int timeSpent = (int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
            - this.lastAttemptStartTime);
        this.wait(this.exponentialRetryInterval - timeSpent);
        this.exponentialRetryInterval *= this.exponentialFactor;
        this.retryCount++;
        setLastAttemptStartTime();
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  private void setLastAttemptStartTime() {
    long now = System.nanoTime();
    if (now < 0) {
      LOG.error("System.nanoTime() returned {}, resetting to 0.", now);
      now = 0;
    }
    this.lastAttemptStartTime = now;
  }

  private void wait(int milliseconds) {
    if (milliseconds > 0) {
      try {
        Thread.sleep((long) milliseconds);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }
}