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
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class TestCapacitySchedulerPlanFollower {

  final static int GB = 1024;

  private Clock mClock = null;
  private CapacityScheduler scheduler = null;
  private RMContext rmContext;
  private RMContext spyRMContext;
  private CapacitySchedulerContext csContext;
  private ReservationAgent mAgent;
  private Plan plan;
  private Resource minAlloc = Resource.newInstance(GB, 1);
  private Resource maxAlloc = Resource.newInstance(GB * 8, 8);
  private ResourceCalculator res = new DefaultResourceCalculator();
  private CapacityOverTimePolicy policy = new CapacityOverTimePolicy();

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    scheduler = spy(spyCs);
    rmContext = TestUtils.getMockRMContext();
    spyRMContext = spy(rmContext);

    ConcurrentMap<ApplicationId, RMApp> spyApps =
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    when(rmApp.getRMAppAttempt((ApplicationAttemptId) Matchers.any()))
        .thenReturn(null);
    Mockito.doReturn(rmApp).when(spyApps).get((ApplicationId) Matchers.any());
    when(spyRMContext.getRMApps()).thenReturn(spyApps);

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(csConf);

    scheduler.setConf(csConf);

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(minAlloc);
    when(csContext.getMaximumResourceCapability()).thenReturn(maxAlloc);
    when(csContext.getClusterResource()).thenReturn(
        Resources.createResource(100 * 16 * GB, 100 * 32));
    when(scheduler.getClusterResource()).thenReturn(
        Resources.createResource(125 * GB, 125));
    when(csContext.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(csConf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

    scheduler.setRMContext(spyRMContext);
    scheduler.init(csConf);
    scheduler.start();

    setupPlanFollower();
  }

  private void setupPlanFollower() throws Exception {
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    mClock = mock(Clock.class);
    mAgent = mock(ReservationAgent.class);

    String reservationQ = testUtil.getFullReservationQueueName();
    CapacitySchedulerConfiguration csConf = scheduler.getConfiguration();
    csConf.setReservationWindow(reservationQ, 20L);
    csConf.setMaximumCapacity(reservationQ, 40);
    csConf.setAverageCapacity(reservationQ, 20);
    policy.init(reservationQ, csConf);
  }

  @Test
  public void testWithMoveOnExpiry() throws PlanningException,
      InterruptedException, AccessControlException {
    // invoke plan follower test with move
    testPlanFollower(true);
  }

  @Test
  public void testWithKillOnExpiry() throws PlanningException,
      InterruptedException, AccessControlException {
    // invoke plan follower test with kill
    testPlanFollower(false);
  }

  private void testPlanFollower(boolean isMove) throws PlanningException,
      InterruptedException, AccessControlException {
    // Initialize plan based on move flag
    plan =
        new InMemoryPlan(scheduler.getRootQueueMetrics(), policy, mAgent,
            scheduler.getClusterResource(), 1L, res,
            scheduler.getMinimumResourceCapability(), maxAlloc, "dedicated",
            null, isMove);

    // add a few reservations to the plan
    long ts = System.currentTimeMillis();
    ReservationId r1 = ReservationId.newInstance(ts, 1);
    int[] f1 = { 10, 10, 10, 10, 10 };
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r1, null, "u3",
            "dedicated", 0, 0 + f1.length, ReservationSystemTestUtil
                .generateAllocation(0L, 1L, f1), res, minAlloc)));

    ReservationId r2 = ReservationId.newInstance(ts, 2);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r2, null, "u3",
            "dedicated", 3, 3 + f1.length, ReservationSystemTestUtil
                .generateAllocation(3L, 1L, f1), res, minAlloc)));

    ReservationId r3 = ReservationId.newInstance(ts, 3);
    int[] f2 = { 0, 10, 20, 10, 0 };
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r3, null, "u4",
            "dedicated", 10, 10 + f2.length, ReservationSystemTestUtil
                .generateAllocation(10L, 1L, f2), res, minAlloc)));

    CapacitySchedulerPlanFollower planFollower =
        new CapacitySchedulerPlanFollower();
    planFollower.init(mClock, scheduler, Collections.singletonList(plan));

    when(mClock.getTime()).thenReturn(0L);
    planFollower.run();

    CSQueue defQ =
        scheduler.getQueue("dedicated" + PlanQueue.DEFAULT_QUEUE_SUFFIX);
    CSQueue q = scheduler.getQueue(r1.toString());
    assertNotNull(q);
    // submit an app to r1
    String user_0 = "test-user";
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId_0 =
        ApplicationAttemptId.newInstance(appId, 0);
    AppAddedSchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, q.getQueueName(), user_0);
    scheduler.handle(addAppEvent);
    AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId_0, false);
    scheduler.handle(appAttemptAddedEvent);

    // initial default reservation queue should have no apps
    Assert.assertEquals(0, defQ.getNumApplications());

    Assert.assertEquals(0.1, q.getCapacity(), 0.01);
    Assert.assertEquals(0.1, q.getMaximumCapacity(), 1.0);
    Assert.assertEquals(1, q.getNumApplications());

    CSQueue q2 = scheduler.getQueue(r2.toString());
    assertNull(q2);
    CSQueue q3 = scheduler.getQueue(r3.toString());
    assertNull(q3);

    when(mClock.getTime()).thenReturn(3L);
    planFollower.run();

    Assert.assertEquals(0, defQ.getNumApplications());
    q = scheduler.getQueue(r1.toString());
    assertNotNull(q);
    Assert.assertEquals(0.1, q.getCapacity(), 0.01);
    Assert.assertEquals(0.1, q.getMaximumCapacity(), 1.0);
    Assert.assertEquals(1, q.getNumApplications());
    q2 = scheduler.getQueue(r2.toString());
    assertNotNull(q2);
    Assert.assertEquals(0.1, q.getCapacity(), 0.01);
    Assert.assertEquals(0.1, q.getMaximumCapacity(), 1.0);
    q3 = scheduler.getQueue(r3.toString());
    assertNull(q3);

    when(mClock.getTime()).thenReturn(10L);
    planFollower.run();

    q = scheduler.getQueue(r1.toString());
    if (isMove) {
      // app should have been moved to default reservation queue
      Assert.assertEquals(1, defQ.getNumApplications());
      assertNull(q);
    } else {
      // app should be killed
      Assert.assertEquals(0, defQ.getNumApplications());
      assertNotNull(q);
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          new AppAttemptRemovedSchedulerEvent(appAttemptId_0,
              RMAppAttemptState.KILLED, false);
      scheduler.handle(appAttemptRemovedEvent);
    }
    q2 = scheduler.getQueue(r2.toString());
    assertNull(q2);
    q3 = scheduler.getQueue(r3.toString());
    assertNotNull(q3);
    Assert.assertEquals(0, q3.getCapacity(), 0.01);
    Assert.assertEquals(1.0, q3.getMaximumCapacity(), 1.0);

    when(mClock.getTime()).thenReturn(11L);
    planFollower.run();

    if (isMove) {
      // app should have been moved to default reservation queue
      Assert.assertEquals(1, defQ.getNumApplications());
    } else {
      // app should be killed
      Assert.assertEquals(0, defQ.getNumApplications());
    }
    q = scheduler.getQueue(r1.toString());
    assertNull(q);
    q2 = scheduler.getQueue(r2.toString());
    assertNull(q2);
    q3 = scheduler.getQueue(r3.toString());
    assertNotNull(q3);
    Assert.assertEquals(0.1, q3.getCapacity(), 0.01);
    Assert.assertEquals(0.1, q3.getMaximumCapacity(), 1.0);

    when(mClock.getTime()).thenReturn(12L);
    planFollower.run();

    q = scheduler.getQueue(r1.toString());
    assertNull(q);
    q2 = scheduler.getQueue(r2.toString());
    assertNull(q2);
    q3 = scheduler.getQueue(r3.toString());
    assertNotNull(q3);
    Assert.assertEquals(0.2, q3.getCapacity(), 0.01);
    Assert.assertEquals(0.2, q3.getMaximumCapacity(), 1.0);

    when(mClock.getTime()).thenReturn(16L);
    planFollower.run();

    q = scheduler.getQueue(r1.toString());
    assertNull(q);
    q2 = scheduler.getQueue(r2.toString());
    assertNull(q2);
    q3 = scheduler.getQueue(r3.toString());
    assertNull(q3);

    assertTrue(defQ.getCapacity() > 0.9);

  }

  public static ApplicationACLsManager mockAppACLsManager() {
    Configuration conf = new Configuration();
    return new ApplicationACLsManager(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.stop();
    }
  }

}
