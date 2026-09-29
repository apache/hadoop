/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

public class TestSimpleRateLimiter {

  /** Number of nanoseconds in one millisecond. */
  private static final long NANOS_PER_MILLISECOND = 1_000_000L;

  /**
   * Verifies that the rate limiter does not introduce unnecessary blocking
   * when calls are naturally spaced apart longer than the required interval.
   *
   * The test creates a limiter allowing 2 permits per second (500ms
   * interval). After calling {@code acquire()}, it waits 600ms—longer than
   * required—so the next {@code acquire()} should return immediately.
   */
  @Test
  void testNoWaitWhenSpacedOut() throws InvalidConfigurationValueException {
    final int millisToSleep = 600; // 600 ms
    // 2 permits per second → 500 ms interval
    SimpleRateLimiter limiter = new SimpleRateLimiter(2);

    limiter.acquire(5, TimeUnit.SECONDS);
    // Sleep longer than required interval
    LockSupport.parkNanos(millisToSleep * NANOS_PER_MILLISECOND);

    long before = System.nanoTime();
    limiter.acquire(5, TimeUnit.SECONDS);  // Should not block
    long after = System.nanoTime();

    long elapsed = after - before;

    // Should be less than 5ms
    Assertions.assertThat(elapsed < 5 * NANOS_PER_MILLISECOND)
        .describedAs("acquire() should not block when enough time has passed")
        .isTrue();
  }

  /**
   * Verifies that the rate limiter enforces the correct delay when
   * {@code acquire()} is called faster than the configured rate.
   *
   * At 5 permits per second (200ms interval), two immediate consecutive
   * calls should cause the second call to block for roughly 200ms.
   */
  @Test
  void testRateLimitingDelay() throws InvalidConfigurationValueException {
    final int permitsPerSecond = 5;
    final long minTimeAllowed = 180;
    final long maxTimeAllowed = 260;
    // 5 permits per second → 200ms interval
    SimpleRateLimiter limiter = new SimpleRateLimiter(permitsPerSecond);

    limiter.acquire(5, TimeUnit.SECONDS); // First call never waits

    long before = System.nanoTime();
    limiter.acquire(5, TimeUnit.SECONDS); // Second call immediately → should wait ~200ms
    long after = System.nanoTime();

    long elapsedMs = (after - before) / NANOS_PER_MILLISECOND;

    // Expect ~200ms, so allow tolerance
    Assertions.assertThat(
            elapsedMs >= minTimeAllowed && elapsedMs <= maxTimeAllowed)
        .describedAs("Expected about 200ms wait, but was " + elapsedMs + " ms")
        .isTrue();
  }

  /**
   * Tests that multiple rapid calls produce cumulative waiting consistent
   * with the configured permit interval.
   *
   * At 10 permits per second (100ms interval), five immediate calls should
   * take around 400ms total (the first call is free; the remaining four
   * require spacing).
   */
  @Test
  void testMultipleBurstCalls()
      throws InvalidConfigurationValueException, InterruptedException {
    final int permitsPerSecond = 10;
    final long minTimeAllowed = 350;
    final long maxTimeAllowed = 550;
    // 10 permits per second → 100ms interval
    SimpleRateLimiter limiter = new SimpleRateLimiter(permitsPerSecond);

    long totalStart = System.nanoTime();

    CountDownLatch latch = new CountDownLatch(5);
    for (int i = 0; i < 5; i++) {
      new Thread(() -> {
        try {
          limiter.acquire(5, TimeUnit.SECONDS);
        } finally {
          latch.countDown();
        }
      }).start();
    }
    //wait for all threads to finish
    latch.await();

    long totalMs = (System.nanoTime() - totalStart) / NANOS_PER_MILLISECOND;

    // 5 calls → should take around 400ms (first is free, next 4 need +100ms each)
    Assertions.assertThat(
            totalMs >= minTimeAllowed && totalMs <= maxTimeAllowed)
        .describedAs("Expected around 400ms total but got " + totalMs + "ms")
        .isTrue();
  }

  /**
   * Verifies that when 10 rapid acquire() calls are made with a rate limit
   * of 3 permits per second (≈333ms interval), the total execution time is
   * spread across ~3 seconds, since each call must be spaced by the interval.
   *
   * Expected timing:
   *   interval = 333ms
   *   first call: no wait
   *   remaining 9 calls must wait: 9 × 333ms ≈ 2997ms
   *
   * Total expected time: around 3.0 seconds.
   */
  @Test
  void testMultipleBurstCallsWhenPermitIsLess()
      throws InvalidConfigurationValueException {
    final int permitsPerSecond = 3;
    final long minTimeAllowed = 2700;
    final long maxTimeAllowed = 3500;
    SimpleRateLimiter limiter = new SimpleRateLimiter(permitsPerSecond);

    long start = System.nanoTime();

    for (int i = 0; i < 10; i++) {
      limiter.acquire(5, TimeUnit.SECONDS);
    }

    long end = System.nanoTime();
    long elapsedMs = (end - start) / NANOS_PER_MILLISECOND;

    // Expected ~3000ms, allow tolerance due to scheduler delays.
    Assertions.assertThat(
            elapsedMs >= minTimeAllowed && elapsedMs <= maxTimeAllowed)
        .describedAs("Expected ~3000ms, but got " + elapsedMs + "ms")
        .isTrue();
  }
}

