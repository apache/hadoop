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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that {@link FairScheduler#setEntitlement} changes the weight of a
 * queue under the scheduler write lock, so that it cannot interleave with a
 * fair share computation of the update thread.
 */
public class TestFairSchedulerSetEntitlement extends FairSchedulerTestBase {
  private static final String QUEUE = "root.reservation";
  private static final long WAIT_MS = 30_000;

  /**
   * A FairScheduler that lets the test hold the scheduler write lock.
   */
  public static class LockExposingFairScheduler extends FairScheduler {
    ReentrantReadWriteLock.WriteLock getWriteLock() {
      return writeLock;
    }
  }

  @Before
  public void setup() {
    conf = createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        LockExposingFairScheduler.class, ResourceScheduler.class);
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
  }

  @Test(timeout = 120_000)
  public void testSetEntitlementWaitsForSchedulerWriteLock()
      throws Exception {
    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(QUEUE, true);
    float oldWeight = queue.getWeight();
    float newWeight = oldWeight / 2;

    CountDownLatch returned = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread setter = new Thread(() -> {
      try {
        scheduler.setEntitlement(QUEUE, new QueueEntitlement(newWeight, 1f));
      } catch (Throwable t) {
        failure.set(t);
      } finally {
        returned.countDown();
      }
    }, "setEntitlement");
    setter.setDaemon(true);

    ReentrantReadWriteLock.WriteLock writeLock =
        ((LockExposingFairScheduler) scheduler).getWriteLock();
    writeLock.lock();
    try {
      setter.start();
      // Wait until the call either returns or parks. Without the lock in
      // setEntitlement it returns here, with it the thread parks on the
      // write lock held by this thread.
      GenericTestUtils.waitFor(() -> returned.getCount() == 0
          || setter.getState() == Thread.State.WAITING, 10, WAIT_MS);
      assertEquals("setEntitlement returned while the scheduler write lock was held",
          1L, returned.getCount());
      assertEquals(Thread.State.WAITING, setter.getState());
      assertEquals("the weight changed while the scheduler write lock was held",
          oldWeight, queue.getWeight(), 0f);
    } finally {
      writeLock.unlock();
    }

    assertTrue("setEntitlement did not return after the write lock was released",
        returned.await(WAIT_MS, TimeUnit.MILLISECONDS));
    assertNull(failure.get());
    assertEquals(newWeight, queue.getWeight(), 0f);
  }
}
