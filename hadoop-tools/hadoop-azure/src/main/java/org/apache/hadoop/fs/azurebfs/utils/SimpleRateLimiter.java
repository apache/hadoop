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

import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

public final class SimpleRateLimiter {

  // Interval between permits in nanoseconds.
  private final long intervalNanos;

  // Next allowed time to acquire a permit in nanoseconds.
  private long nextAllowedTime;

  /** Number of nanoseconds in one second. */
  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  /**
   * Constructs a SimpleRateLimiter that allows the specified number of
   * permits per second.
   *
   * @param permitsPerSecond Number of permits allowed per second.
   * @throws InvalidConfigurationValueException if permitsPerSecond is
   *                                            less than or equal to zero.
   */
  public SimpleRateLimiter(int permitsPerSecond)
      throws InvalidConfigurationValueException {
    if (permitsPerSecond <= 0) {
      throw new InvalidConfigurationValueException(
          "permitsPerSecond must be > 0");
    }
    this.intervalNanos = NANOS_PER_SECOND / permitsPerSecond;
    this.nextAllowedTime = System.nanoTime();
  }

  /**
   * Acquires a permit from the rate limiter, blocking until one is available.
   */
  public synchronized void acquire() {
    while (true) {
      long now = System.nanoTime();
      long wait = nextAllowedTime - now;

      if (wait <= 0) {
        nextAllowedTime = now + intervalNanos;
        return;
      }

      LockSupport.parkNanos(wait);

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
