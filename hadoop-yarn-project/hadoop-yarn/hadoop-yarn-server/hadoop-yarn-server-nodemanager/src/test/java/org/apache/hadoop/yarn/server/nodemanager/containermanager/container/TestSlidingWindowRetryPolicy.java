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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SlidingWindowRetryPolicy}.
 */
public class TestSlidingWindowRetryPolicy {

  private ControlledClock clock;
  private SlidingWindowRetryPolicy retryPolicy;

  @BeforeEach
  public void setup() {
    clock = new ControlledClock();
    retryPolicy = new SlidingWindowRetryPolicy(clock);
  }

  @Test
  public void testNeverRetry() {
    ContainerRetryContext retryContext =
        ContainerRetryContext.NEVER_RETRY_CONTEXT;
    SlidingWindowRetryPolicy.RetryContext windowContext = new
        SlidingWindowRetryPolicy.RetryContext(retryContext);
    assertFalse(retryPolicy.shouldRetry(windowContext,
        12), "never retry");
    assertEquals(0, windowContext.getRemainingRetries(), "remaining retries");
  }

  @Test
  public void testAlwaysRetry() {
    ContainerRetryContext retryContext =  ContainerRetryContext.newInstance(
        ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, -1,
        0, 10);
    SlidingWindowRetryPolicy.RetryContext windowContext = new
        SlidingWindowRetryPolicy.RetryContext(retryContext);
    assertTrue(retryPolicy.shouldRetry(windowContext,
        12), "always retry");
    assertEquals(ContainerRetryContext.RETRY_FOREVER,
        windowContext.getRemainingRetries(), "remaining retries");
  }

  @Test
  public void testFailuresValidityInterval() {
    ContainerRetryContext retryContext = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, 1, 0, 10);
    SlidingWindowRetryPolicy.RetryContext windowRetryContext =
        new SlidingWindowRetryPolicy.RetryContext(retryContext);
    assertTrue(
        retryPolicy.shouldRetry(windowRetryContext, 12), "retry 1");
    retryPolicy.updateRetryContext(windowRetryContext);
    assertEquals(1,
        windowRetryContext.getRemainingRetries(), "remaining retries");

    clock.setTime(20);
    assertTrue(
        retryPolicy.shouldRetry(windowRetryContext, 12), "retry 2");
    retryPolicy.updateRetryContext(windowRetryContext);
    assertEquals(1,
        windowRetryContext.getRemainingRetries(), "remaining retries");

    clock.setTime(40);
    assertTrue(
        retryPolicy.shouldRetry(windowRetryContext, 12), "retry 3");
    retryPolicy.updateRetryContext(windowRetryContext);
    assertEquals(1,
        windowRetryContext.getRemainingRetries(), "remaining retries");

    clock.setTime(45);
    assertFalse(
        retryPolicy.shouldRetry(windowRetryContext, 12), "retry failed");
    retryPolicy.updateRetryContext(windowRetryContext);
    assertEquals(0,
        windowRetryContext.getRemainingRetries(), "remaining retries");
  }
}
