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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

/**
 * A simple rate limiter that allows a specified number of permits
 * per second. This implementation uses basic synchronization and
 * LockSupport for waiting.
 */
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
   * Acquires a permit from the rate limiter, waiting up to the
   * specified timeout if necessary.
   *
   * @param timeout Maximum time to wait for a permit.
   * @param unit    Time unit of the timeout argument.
   */
  public synchronized void acquire(long timeout, TimeUnit unit) {
    if (timeout <= 0) {
      return;
    }

    final long deadline = System.nanoTime() + unit.toNanos(timeout);
    while (true) {
      long now = System.nanoTime();
      long wait = nextAllowedTime - now;

      if (wait <= 0) {
        nextAllowedTime = now + intervalNanos;
        return;
      }

      long remaining = deadline - now;
      if (remaining <= 0) {
        return; // timeout expired
      }

      LockSupport.parkNanos(Math.min(wait, remaining));

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
