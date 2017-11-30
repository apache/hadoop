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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCapacitySchedulerDynamicBehavior {
  private static final Log LOG = LogFactory
      .getLog(TestCapacitySchedulerDynamicBehavior.class);
  private static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String B1 = B + ".b1";
  private static final String B2 = B + ".b2";
  private static final String B3 = B + ".b3";
  private static float A_CAPACITY = 10.5f;
  private static float B_CAPACITY = 89.5f;
  private static float A1_CAPACITY = 30;
  private static float A2_CAPACITY = 70;
  private static float B1_CAPACITY = 79.2f;
  private static float B2_CAPACITY = 0.8f;
  private static float B3_CAPACITY = 20;

  private final TestCapacityScheduler tcs = new TestCapacityScheduler();

  private int GB = 1024;

  private MockRM rm;

  @Before
  public void setUp() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupPlanQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, false);
    rm = new MockRM(conf);
    rm.start();
  }

  @Test
  public void testRefreshQueuesWithReservations() throws Exception {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    //set default queue capacity to zero
    ((AutoCreatedLeafQueue) cs
            .getQueue("a" + ReservationConstants.DEFAULT_QUEUE_SUFFIX))
            .setEntitlement(
                    new QueueEntitlement(0f, 1f));

    // Test add one reservation dynamically and manually modify capacity
    AutoCreatedLeafQueue a1 =
        new AutoCreatedLeafQueue(cs, "a1", (PlanQueue) cs.getQueue("a"));
    cs.addQueue(a1);
    a1.setEntitlement(new QueueEntitlement(A1_CAPACITY / 100, 1f));

    // Test add another reservation queue and use setEntitlement to modify
    // capacity
    AutoCreatedLeafQueue a2 =
        new AutoCreatedLeafQueue(cs, "a2", (PlanQueue) cs.getQueue("a"));
    cs.addQueue(a2);
    cs.setEntitlement("a2", new QueueEntitlement(A2_CAPACITY / 100, 1.0f));

    // Verify all allocations match
    tcs.checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    // Reinitialize and verify all dynamic queued survived
    CapacitySchedulerConfiguration conf = cs.getConfiguration();
    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    cs.reinitialize(conf, rm.getRMContext());

    tcs.checkQueueCapacities(cs, 80f, 20f);
  }

  @Test
  public void testAddQueueFailCases() throws Exception {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    try {
      // Test invalid addition (adding non-zero size queue)
      AutoCreatedLeafQueue a1 =
          new AutoCreatedLeafQueue(cs, "a1", (PlanQueue) cs.getQueue("a"));
      a1.setEntitlement(new QueueEntitlement(A1_CAPACITY / 100, 1f));
      cs.addQueue(a1);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Test add one reservation dynamically and manually modify capacity
    AutoCreatedLeafQueue a1 =
        new AutoCreatedLeafQueue(cs, "a1", (PlanQueue) cs.getQueue("a"));
    cs.addQueue(a1);
    //set default queue capacity to zero
    ((AutoCreatedLeafQueue) cs
        .getQueue("a" + ReservationConstants.DEFAULT_QUEUE_SUFFIX))
            .setEntitlement(
                new QueueEntitlement(0f, 1f));
    a1.setEntitlement(new QueueEntitlement(A1_CAPACITY / 100, 1f));

    // Test add another reservation queue and use setEntitlement to modify
    // capacity
    AutoCreatedLeafQueue a2 =
        new AutoCreatedLeafQueue(cs, "a2", (PlanQueue) cs.getQueue("a"));

    cs.addQueue(a2);

    try {
      // Test invalid entitlement (sum of queues exceed 100%)
      cs.setEntitlement("a2", new QueueEntitlement(A2_CAPACITY / 100 + 0.1f,
          1.0f));
      fail();
    } catch (Exception e) {
      // expected
    }

    cs.setEntitlement("a2", new QueueEntitlement(A2_CAPACITY / 100, 1.0f));

    // Verify all allocations match
    tcs.checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    cs.stop();
  }

  @Test
  public void testRemoveQueue() throws Exception {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Test add one reservation dynamically and manually modify capacity
    AutoCreatedLeafQueue a1 =
        new AutoCreatedLeafQueue(cs, "a1", (PlanQueue) cs.getQueue("a"));
    cs.addQueue(a1);
    a1.setEntitlement(new QueueEntitlement(A1_CAPACITY / 100, 1f));

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    // check preconditions
    List<ApplicationAttemptId> appsInA1 = cs.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());
    try {
      cs.removeQueue("a1");
      fail();
    } catch (SchedulerDynamicEditException s) {
      // expected a1 contains applications
    }
    // clear queue by killling all apps
    cs.killAllAppsInQueue("a1");
    // wait for events of move to propagate
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);

    try {
      cs.removeQueue("a1");
      fail();
    } catch (SchedulerDynamicEditException s) {
      // expected a1 is not zero capacity
    }
    // set capacity to zero
    cs.setEntitlement("a1", new QueueEntitlement(0f, 0f));
    cs.removeQueue("a1");

    assertTrue(cs.getQueue("a1") == null);

    rm.stop();
  }

  @Test
  public void testMoveAppToPlanQueue() throws Exception {
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "b1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInB1.size());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertEquals(1, appsInB.size());
    assertTrue(appsInB.contains(appAttemptId));

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    String queue =
        scheduler.getApplicationAttempt(appsInB1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("b1", queue);

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // create the default reservation queue
    String defQName = "a" + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    AutoCreatedLeafQueue defQ =
        new AutoCreatedLeafQueue(scheduler, defQName,
            (PlanQueue) scheduler.getQueue("a"));
    scheduler.addQueue(defQ);
    defQ.setEntitlement(new QueueEntitlement(1f, 1f));

    List<ApplicationAttemptId> appsInDefQ = scheduler.getAppsInQueue(defQName);
    assertTrue(appsInDefQ.isEmpty());

    // now move the app to plan queue
    scheduler.moveApplication(app.getApplicationId(), "a");

    // check postconditions
    appsInDefQ = scheduler.getAppsInQueue(defQName);
    assertEquals(1, appsInDefQ.size());
    queue =
        scheduler.getApplicationAttempt(appsInDefQ.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals(defQName));

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    rm.stop();
  }

  private void setupPlanQueueConfiguration(CapacitySchedulerConfiguration conf) {

    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(B, new String[] { "b1", "b2", "b3" });
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    conf.setReservable(A, true);
    conf.setReservationWindow(A, 86400 * 1000);
    conf.setAverageCapacity(A, 1.0f);

    LOG.info("Setup a as a plan queue");
  }

}
