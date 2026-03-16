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

package org.apache.hadoop.fs.bos;

import org.apache.hadoop.io.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A retry policy that uses random sleep times between a
 * configured minimum and maximum, with sleep duration
 * increasing proportionally to the number of retries.
 */
public class BosRandomRetryPolicy implements RetryPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(
          BosRandomRetryPolicy.class);

  private int maxRetries;
  private TimeUnit timeUnit;
  private int minSleep;
  private int maxSleep;
  private Random random = new Random();

  /**
   * Constructs a BosRandomRetryPolicy.
   *
   * @param maxRetries the maximum number of retries
   * @param minSleep   the minimum sleep time
   * @param maxSleep   the maximum sleep time
   * @param timeUnit   the time unit for sleep values
   * @throws IllegalArgumentException if minSleep is negative
   *         or maxSleep is not greater than minSleep
   */
  public BosRandomRetryPolicy(
      int maxRetries, int minSleep,
      int maxSleep, TimeUnit timeUnit) {
    if (minSleep < 0 || maxSleep <= minSleep) {
      throw new IllegalArgumentException(
          "min sleep time must be positive number and"
              + " max sleep time must be greater than"
              + " min sleep time !");
    }
    this.maxRetries = maxRetries;
    this.minSleep = minSleep;
    this.maxSleep = maxSleep;
    this.timeUnit = timeUnit;
  }

  /**
   * Determines whether the operation should be retried.
   *
   * @param e the exception that triggered the retry
   * @param retries the number of retries so far
   * @param failovers the number of failovers so far
   * @param isIdempotentOrAtMostOnce whether the operation is
   *        idempotent or at-most-once
   * @return the retry action indicating whether to retry or
   *         fail
   */
  @Override
  public RetryAction shouldRetry(
      Exception e, int retries, int failovers,
      boolean isIdempotentOrAtMostOnce) {
    if (retries >= this.maxRetries) {
      return RetryAction.FAIL;
    }
    long sleepMillis = this.timeUnit.toMillis(
        this.calculateSleepTime(retries));
    return new RetryAction(
        RetryAction.RetryDecision.RETRY,
        sleepMillis);
  }

  /**
   * Calculates the sleep time for the given retry attempt.
   *
   * @param retries the current retry count
   * @return the sleep time in the configured time unit
   */
  public long calculateSleepTime(int retries) {
    int sleepTime = minSleep
        + (random.nextInt(maxSleep - minSleep));
    LOG.warn("sleep random time : {}", sleepTime);
    return sleepTime * (retries + 1);
  }
}
