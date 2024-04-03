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

package org.apache.hadoop.fs.impl;

import org.apache.hadoop.fs.IORateLimiter;
import org.apache.hadoop.util.RateLimiting;
import org.apache.hadoop.util.RateLimitingFactory;

import static java.util.Objects.requireNonNull;

/**
 * Implementation support for the IO rate limiter.
 */
public final class IORateLimiterSupport {

  private IORateLimiterSupport() {
  }

  /**
   * Get a rate limiter source which has no rate limiting.
   * @return a rate limiter source which has no rate limiting.
   */
  public static IORateLimiter unlimited() {
    return (operation, src, dest, requestedCapacity) -> {
      requireNonNull(operation, "operation");
      return RateLimitingFactory.unlimitedRate().acquire(requestedCapacity);
    };
  }

  /**
   * Create a rate limiter with a fixed capacity.
   * @param capacityPerSecond capacity per second.
   * @return a rate limiter.
   */
  public static IORateLimiter create(int capacityPerSecond) {
    final RateLimiting limiting = RateLimitingFactory.create(capacityPerSecond);
    return (operation, src, dest, requestedCapacity) -> {
      requireNonNull(operation, "operation");
      return limiting.acquire(requestedCapacity);
    };
  }
}
