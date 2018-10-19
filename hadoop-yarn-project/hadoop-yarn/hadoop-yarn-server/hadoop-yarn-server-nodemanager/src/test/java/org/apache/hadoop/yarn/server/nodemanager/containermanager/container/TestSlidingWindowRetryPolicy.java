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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SlidingWindowRetryPolicy}.
 */
public class TestSlidingWindowRetryPolicy {

  private ControlledClock clock;
  private SlidingWindowRetryPolicy retryPolicy;

  @Before
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
    Assert.assertFalse("never retry", retryPolicy.shouldRetry(windowContext,
        12));
    Assert.assertEquals("remaining retries", 0,
        windowContext.getRemainingRetries());
  }

  @Test
  public void testAlwaysRetry() {
    ContainerRetryContext retryContext =  ContainerRetryContext.newInstance(
        ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, -1,
        0, 10);
    SlidingWindowRetryPolicy.RetryContext windowContext = new
        SlidingWindowRetryPolicy.RetryContext(retryContext);
    Assert.assertTrue("always retry", retryPolicy.shouldRetry(windowContext,
        12));
    Assert.assertEquals("remaining retries",
        ContainerRetryContext.RETRY_FOREVER,
        windowContext.getRemainingRetries());
  }

  @Test
  public void testFailuresValidityInterval() {
    ContainerRetryContext retryContext = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, 1, 0, 10);
    SlidingWindowRetryPolicy.RetryContext windowRetryContext =
        new SlidingWindowRetryPolicy.RetryContext(retryContext);
    Assert.assertTrue("retry 1",
        retryPolicy.shouldRetry(windowRetryContext, 12));
    retryPolicy.updateRetryContext(windowRetryContext);
    Assert.assertEquals("remaining retries", 1,
        windowRetryContext.getRemainingRetries());

    clock.setTime(20);
    Assert.assertTrue("retry 2",
        retryPolicy.shouldRetry(windowRetryContext, 12));
    retryPolicy.updateRetryContext(windowRetryContext);
    Assert.assertEquals("remaining retries", 1,
        windowRetryContext.getRemainingRetries());

    clock.setTime(40);
    Assert.assertTrue("retry 3",
        retryPolicy.shouldRetry(windowRetryContext, 12));
    retryPolicy.updateRetryContext(windowRetryContext);
    Assert.assertEquals("remaining retries", 1,
        windowRetryContext.getRemainingRetries());

    clock.setTime(45);
    Assert.assertFalse("retry failed",
        retryPolicy.shouldRetry(windowRetryContext, 12));
    retryPolicy.updateRetryContext(windowRetryContext);
    Assert.assertEquals("remaining retries", 0,
        windowRetryContext.getRemainingRetries());
  }
}
