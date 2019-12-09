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

import java.io.File;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerTestBase;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class TestFairSchedulerPlanFollower extends
    TestSchedulerPlanFollowerBase {
  private final static String ALLOC_FILE = new File(
      FairSchedulerTestBase.TEST_DIR,
      TestSchedulerPlanFollowerBase.class.getName() + ".xml").getAbsolutePath();
  private RMContext rmContext;
  private RMContext spyRMContext;
  private FairScheduler fs;
  private Configuration conf;
  private FairSchedulerTestBase testHelper = new FairSchedulerTestBase();

  @Rule
  public TestName name = new TestName();

  protected Configuration createConfiguration() {
    Configuration conf = testHelper.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    conf = createConfiguration();
    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);

    // Setup
    rmContext = TestUtils.getMockRMContext();
    spyRMContext = spy(rmContext);
    fs = ReservationSystemTestUtil.setupFairScheduler(spyRMContext, conf, 125);
    scheduler = fs;

    ConcurrentMap<ApplicationId, RMApp> spyApps =
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    when(rmApp.getRMAppAttempt((ApplicationAttemptId) Matchers.any()))
        .thenReturn(null);
    Mockito.doReturn(rmApp).when(spyApps).get((ApplicationId) Matchers.any());
    when(spyRMContext.getRMApps()).thenReturn(spyApps);

    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);
    setupPlanFollower();
  }

  private void setupPlanFollower() throws Exception {
    mClock = mock(Clock.class);
    mAgent = mock(ReservationAgent.class);

    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    AllocationConfiguration allocConf = fs.getAllocationConfiguration();
    allocConf.setReservationWindow(20L);
    allocConf.setAverageCapacity(20);
    policy.init(reservationQ, allocConf);
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

  @Override
  protected void checkDefaultQueueBeforePlanFollowerRun() {
    Assert.assertNull(getDefaultQueue());
  }
  @Override
  protected void verifyCapacity(Queue defQ) {
    assertTrue(((FSQueue) defQ).getWeight() > 0.9);
  }

  @Override
  protected Queue getDefaultQueue() {
    return getReservationQueue("dedicated"
        + ReservationConstants.DEFAULT_QUEUE_SUFFIX);
  }

  @Override
  protected int getNumberOfApplications(Queue queue) {
    int numberOfApplications = fs.getAppsInQueue(queue.getQueueName()).size();
    return numberOfApplications;
  }

  @Override
  protected AbstractSchedulerPlanFollower createPlanFollower() {
    FairSchedulerPlanFollower planFollower = new FairSchedulerPlanFollower();
    planFollower.init(mClock, scheduler, Collections.singletonList(plan));
    return planFollower;
  }

  @Override
  protected void assertReservationQueueExists(ReservationId r) {
    Queue q = getReservationQueue(r.toString());
    assertNotNull(q);
  }

  @Override
  protected void assertReservationQueueExists(ReservationId r,
      double expectedCapacity, double expectedMaxCapacity) {
    FSLeafQueue q =
        fs.getQueueManager().getLeafQueue(plan.getQueueName() + "" + "." + r,
            false);
    assertNotNull(q);
    // For now we are setting both to same weight
    Assert.assertEquals(expectedCapacity, q.getWeight(), 0.01);
  }

  @Override
  protected void assertReservationQueueDoesNotExist(ReservationId r) {
    Queue q = getReservationQueue(r.toString());
    assertNull(q);
  }

  @Override
  protected Queue getReservationQueue(String r) {
    return fs.getQueueManager().getLeafQueue(
        plan.getQueueName() + "" + "." + r, false);
  }

  public static ApplicationACLsManager mockAppACLsManager() {
    Configuration conf = new Configuration();
    return new ApplicationACLsManager(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (scheduler != null) {
      fs.stop();
    }
  }
}
