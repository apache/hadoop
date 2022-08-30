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

package org.apache.hadoop.util;

import java.time.Duration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.RateLimiter;

/**
 * Factory for Rate Limiting.
 * This should be only place in the code where the guava RateLimiter is imported.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class RateLimitingFactory {

  private static final RateLimiting UNLIMITED = new NoRateLimiting();

  /**
   * No waiting took place.
   */
  private static final Duration INSTANTLY = Duration.ofMillis(0);

  private RateLimitingFactory() {
  }

  /**
   * No Rate Limiting.
   */
  private static class NoRateLimiting implements RateLimiting {


    @Override
    public Duration acquire(int requestedCapacity) {
      return INSTANTLY;
    }
  }

  /**
   * Rate limiting restricted to that of a google rate limiter.
   */
  private static final class RestrictedRateLimiting implements RateLimiting {
    private final RateLimiter limiter;

    /**
     * Constructor.
     * @param capacityPerSecond capacity in permits/second.
     */
    private RestrictedRateLimiting(int capacityPerSecond) {
      this.limiter = RateLimiter.create(capacityPerSecond);
    }

    @Override
    public Duration acquire(int requestedCapacity) {
      final double delayMillis = limiter.acquire(requestedCapacity);
      return delayMillis == 0
          ? INSTANTLY
          : Duration.ofMillis((long) (delayMillis * 1000));
    }

  }

  /**
   * Get the unlimited rate.
   * @return a rate limiter which always has capacity.
   */
  public static RateLimiting unlimitedRate() {
    return UNLIMITED;
  }

  /**
   * Create an instance.
   * If the rate is 0; return the unlimited rate.
   * @param capacity capacity in permits/second.
   * @return limiter restricted to the given capacity.
   */
  public static RateLimiting create(int capacity) {

    return capacity == 0
        ? unlimitedRate()
        : new RestrictedRateLimiting(capacity);
  }

}
