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
package org.apache.hadoop.hdfs.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.util.Timer;

/**
 * A rate limiter which loosely emulates the behavior of Guava's RateLimiter
 * for branches which do not have that class available. The APIs are intended
 * to match RateLimiter's to avoid code changes in calling classes when
 * switching to Guava.
 */
public class RateLimiter {

  private final Timer timer;
  /** The period that should elapse between any two operations. */
  private final long opDispersalPeriodNanos;

  /** The last time an operation completed, in system nanos (not wall time). */
  private final AtomicLong lastOpTimeNanos;

  public static RateLimiter create(double maxOpsPerSecond) {
    return new RateLimiter(new Timer(), maxOpsPerSecond);
  }

  RateLimiter(Timer timer, double maxOpsPerSecond) {
    this.timer = timer;
    if (maxOpsPerSecond <= 0) {
      throw new IllegalArgumentException("RateLimiter max operations per "
          + "second must be > 0 but was " + maxOpsPerSecond);
    }
    opDispersalPeriodNanos =
        (long) (TimeUnit.SECONDS.toNanos(1) / maxOpsPerSecond);
    lastOpTimeNanos = new AtomicLong(Long.MIN_VALUE);
  }

  /**
   * Attempt to acquire a permit to perform an operation. This will block until
   * enough time has elapsed since the last operation to perform another. No
   * fairness is provided; acquisition attempts will be serviced in an arbitrary
   * order rather than FIFO.
   *
   * @return The time, in seconds, it took to acquire a permit.
   */
  public double acquire() {
    boolean interrupted = false;
    long startTimeNanos = Long.MAX_VALUE;
    try {
      while (true) {
        long currTimeNanos = timer.monotonicNowNanos();
        startTimeNanos = Math.min(currTimeNanos, startTimeNanos);
        long lastOpTimeLocal = lastOpTimeNanos.get();
        long nextAllowedOpTime = lastOpTimeLocal + opDispersalPeriodNanos;
        if (currTimeNanos >= nextAllowedOpTime) {
          // enough time has elapsed; attempt to acquire the current permit
          boolean acquired =
              lastOpTimeNanos.compareAndSet(lastOpTimeLocal, currTimeNanos);
          // if the CAS failed, another thread acquired the permit, try again
          if (acquired) {
            return (currTimeNanos - startTimeNanos)
                / ((double) TimeUnit.SECONDS.toNanos(1));
          }
        } else {
          interrupted |= sleep(nextAllowedOpTime - currTimeNanos);
        }
      }
    } finally {
      if (interrupted) {
        // allow other levels to be aware of the interrupt
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Sleep for some amount of nanoseconds. Returns true iff interrupted. */
  boolean sleep(long sleepTimeNanos) {
    long sleepTimeMillis = TimeUnit.NANOSECONDS.toMillis(sleepTimeNanos);
    try {
      Thread.sleep(sleepTimeMillis, (int) (sleepTimeNanos
          - TimeUnit.MILLISECONDS.toNanos(sleepTimeMillis)));
    } catch (InterruptedException ie) {
      // swallow and continue, but allow the interrupt to be remembered
      return true;
    }
    return false;
  }
}
