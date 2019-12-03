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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Test;

import static org.junit.Assert.*;


/** Tests for {@link RateLimiter}. */
public class TestRateLimiter {

  // epsilon of 1 ns
  private static final double EPSILON = 1.0 / TimeUnit.SECONDS.toNanos(1);

  @Test
  public void testRateLimiter() {
    FakeTimer timer = new FakeTimer();
    Queue<Long> sleepTimesNanos = new LinkedList<>();
    Queue<Long> advanceTimerNanos = new LinkedList<>();
    RateLimiter limiter =
        new TestingRateLimiter(timer, 10, sleepTimesNanos, advanceTimerNanos);

    final long nanos100ms = TimeUnit.MILLISECONDS.toNanos(100);

    // should be able to acquire immediately the first time
    assertEquals(0.0, limiter.acquire(), EPSILON);
    assertTrue(sleepTimesNanos.isEmpty());

    // 100ms of sleep is required the second time
    advanceTimerNanos.add(nanos100ms);
    assertEquals(0.1, limiter.acquire(), EPSILON);
    assertEquals(1, sleepTimesNanos.size());
    assertNextValue(sleepTimesNanos, nanos100ms);

    // test when it takes 2 sleep cycles to be able to acquire
    advanceTimerNanos.add(nanos100ms / 2);
    advanceTimerNanos.add(nanos100ms / 2);
    assertEquals(0.1, limiter.acquire(), EPSILON);
    assertEquals(2, sleepTimesNanos.size());
    assertNextValue(sleepTimesNanos, nanos100ms);
    assertNextValue(sleepTimesNanos, nanos100ms / 2);

    // if some time passes between acquisitions, the next should be immediate
    timer.advanceNanos(nanos100ms * 2);
    assertEquals(0.0, limiter.acquire(), EPSILON);
    assertTrue(sleepTimesNanos.isEmpty());

    // the rate limiter has no memory, so although time passed, the next
    // acquisition is still rate limited
    advanceTimerNanos.add(nanos100ms);
    assertEquals(0.1, limiter.acquire(), EPSILON);
    assertEquals(1, sleepTimesNanos.size());
    assertNextValue(sleepTimesNanos, nanos100ms);
  }

  private static void assertNextValue(Queue<Long> queue, long expected) {
    Long value = queue.poll();
    assertNotNull(value);
    assertEquals(expected, value.longValue());
  }

  private static class TestingRateLimiter extends RateLimiter {

    private final FakeTimer fakeTimer;
    private final Queue<Long> sleepTimesNanos;
    private final Queue<Long> advanceTimerNanos;

    TestingRateLimiter(FakeTimer fakeTimer, double maxOpsPerSecond,
        Queue<Long> sleepTimesNanos, Queue<Long> advanceTimerNanos) {
      super(fakeTimer, maxOpsPerSecond);
      this.fakeTimer = fakeTimer;
      this.sleepTimesNanos = sleepTimesNanos;
      this.advanceTimerNanos = advanceTimerNanos;
    }

    @Override
    boolean sleep(long sleepTimeNanos) {
      sleepTimesNanos.offer(sleepTimeNanos);
      Long advanceNanos = advanceTimerNanos.poll();
      if (advanceNanos == null) {
        fail("Unexpected sleep; no timer advance value found");
      }
      fakeTimer.advanceNanos(advanceNanos);
      return false;
    }
  }
}
