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

package org.apache.hadoop.fs;

import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.IORateLimiterSupport;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.RateLimiting;
import org.apache.hadoop.util.RateLimitingFactory;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE_BULK;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE_DIR;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test IO rate limiting in {@link RateLimiting} and {@link IORateLimiter}.
 * <p>
 * This includes: illegal arguments, and what if more capacity
 * is requested than is available.
 */
public class TestIORateLimiter extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestIORateLimiter.class);

  public static final Path ROOT = new Path("/");

  @Test
  public void testAcquireCapacity() {
    final int size = 10;
    final RateLimiting limiter = RateLimitingFactory.create(size);
    // do a chain of requests
    limiter.acquire(0);
    limiter.acquire(1);
    limiter.acquire(2);

    // now ask for more than is allowed. This MUST work.
    final int excess = size * 2;
    limiter.acquire(excess);
    assertDelayed(limiter, excess);
  }

  @Test
  public void testNegativeCapacityRejected() throws Throwable {
    final RateLimiting limiter = RateLimitingFactory.create(1);
    intercept(IllegalArgumentException.class, () ->
        limiter.acquire(-1));
  }

  @Test
  public void testNegativeLimiterCapacityRejected() throws Throwable {
    intercept(IllegalArgumentException.class, () ->
        RateLimitingFactory.create(-1));
  }

  /**
   * This is a key behavior: it is acceptable to ask for more capacity
   * than the caller has, the initial request must be granted,
   * but the followup request must be delayed until enough capacity
   * has been restored.
   */
  @Test
  public void testAcquireExcessCapacity() {

    // create a small limiter
    final int size = 10;
    final RateLimiting limiter = RateLimitingFactory.create(size);

    // now ask for more than is allowed. This MUST work.
    final int excess = size * 2;
    // first attempt gets more capacity than arrives every second.
    assertNotDelayed(limiter, excess);
    // second attempt will block
    assertDelayed(limiter, excess);
    // third attempt will block
    assertDelayed(limiter, size);
    // as these are short-cut, no delays.
    assertNotDelayed(limiter, 0);
  }

  @Test
  public void testIORateLimiterWithLimitedCapacity() {
    final int size = 10;
    final IORateLimiter limiter = IORateLimiterSupport.createIORateLimiter(size);
    // this size will use more than can be allocated in a second.
    final int excess = size * 2;
    // first attempt gets more capacity than arrives every second.
    assertNotDelayed(limiter, OP_DELETE_DIR, excess);
    // second attempt will block
    assertDelayed(limiter, OP_DELETE_BULK, excess);
    // third attempt will block
    assertDelayed(limiter, OP_DELETE, size);
    // as zero capacity requests are short-cut, no delays, ever.
    assertNotDelayed(limiter, "", 0);
  }

  /**
   * Verify the unlimited rate limiter really is unlimited.
   */
  @Test
  public void testIORateLimiterWithUnlimitedCapacity() {
    final IORateLimiter limiter = IORateLimiterSupport.unlimited();
    // this size will use more than can be allocated in a second.

    assertNotDelayed(limiter, "1", 100_000);
    assertNotDelayed(limiter, "2", 100_000);
  }

  @Test
  public void testUnlimitedRejectsNegativeCapacity() throws Exception {
    intercept(IllegalArgumentException.class, () ->
        IORateLimiterSupport.unlimited().acquireIOCapacity("", ROOT, ROOT, -1));
  }

  @Test
  public void testUnlimitedRejectsNullOperation() throws Exception {
    intercept(IllegalArgumentException.class, () ->
        IORateLimiterSupport.unlimited().acquireIOCapacity(null, ROOT, null, 0));
  }

  @Test
  public void testUnlimitedRejectsNullSource() throws Exception {
    intercept(IllegalArgumentException.class, () ->
        IORateLimiterSupport.unlimited().acquireIOCapacity("", null, null, 1));
  }

  /**
   * Assert that a request for a given capacity is delayed.
   * There's no assertion on the duration, only that it is greater than 0.
   * @param limiter limiter
   * @param capacity capacity
   */
  private static void assertNotDelayed(final RateLimiting limiter, final int capacity) {
    assertZeroDuration(capacity, limiter.acquire(capacity));
  }

  /**
   * Assert that a request for a given capacity is delayed.
   * There's no assertion on the duration, only that it is greater than 0.
   * @param limiter limiter
   * @param capacity capacity
   */
  private static void assertDelayed(final RateLimiting limiter, final int capacity) {
    assertNonZeroDuration(capacity, limiter.acquire(capacity));
  }

  /**
   * Assert that a request for a given capacity is not delayed.
   * @param limiter limiter
   * @param op operation
   * @param capacity capacity
   */
  private static void assertNotDelayed(IORateLimiter limiter, String op, int capacity) {
    assertZeroDuration(capacity, limiter.acquireIOCapacity(op, ROOT, null, capacity));
  }

  /**
   * Assert that a request for a given capacity is delayed.
   * There's no assertion on the duration, only that it is greater than 0.
   * @param limiter limiter
   * @param op operation
   * @param capacity capacity
   */
  private static void assertDelayed(IORateLimiter limiter, String op, int capacity) {
    assertNonZeroDuration(capacity, limiter.acquireIOCapacity(op, ROOT, null, capacity));
  }

  /**
   * Assert that duration was not zero.
   * @param capacity capacity requested
   * @param duration duration
   */
  private static void assertNonZeroDuration(final int capacity, final Duration duration) {
    LOG.info("Delay for {} capacity: {}", capacity, duration);
    Assertions.assertThat(duration)
        .describedAs("delay for %d capacity", capacity)
        .isGreaterThan(Duration.ZERO);
  }

  /**
   * Assert that duration was zero.
   * @param capacity capacity requested
   * @param duration duration
   */
  private static void assertZeroDuration(final int capacity, final Duration duration) {
    Assertions.assertThat(duration)
        .describedAs("delay for %d capacity", capacity)
        .isEqualTo(Duration.ZERO);
  }
}