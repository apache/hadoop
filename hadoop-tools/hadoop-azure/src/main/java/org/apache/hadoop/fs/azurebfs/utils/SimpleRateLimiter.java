/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

public class SimpleRateLimiter {

  /** The minimum interval between permits, in nanoseconds. */
  private final long intervalNanos;

  /** The next allowed time (in nanoseconds) when a permit may be issued. */
  private final AtomicLong nextAllowedTime = new AtomicLong(0);

  /**
   * Creates a rate limiter with a fixed number of permits allowed per second.
   *
   * @param permitsPerSecond the maximum number of permits allowed per second;
   *                         must be a positive integer
   * @throws IllegalArgumentException if {@code permitsPerSecond <= 0}
   */
  public SimpleRateLimiter(int permitsPerSecond)
      throws InvalidConfigurationValueException {
    if (permitsPerSecond <= 0) {
      throw new InvalidConfigurationValueException(
          "Aggregated Metrics Per Second Call");
    }
    this.intervalNanos = 1_000_000_000L / permitsPerSecond;
  }

  /**
   * Acquires a permit from the rate limiter, blocking if necessary to maintain
   * the configured rate.
   *
   * If the current time is earlier than the next allowed permit time, this
   * method blocks for the required duration. Otherwise, it proceeds
   * immediately.
   */
  public void acquire() {
    while (true) { // In case of failure, it will retry
      long now = System.nanoTime();
      long prev = nextAllowedTime.get();
      long next = Math.max(prev, now) + intervalNanos;

      if (nextAllowedTime.compareAndSet(prev, next)) {
        long wait = next - now - intervalNanos; // adjust for this permit
        if (wait > 0) {
          LockSupport.parkNanos(wait);
        }
        return;
      }
    }
  }
}
