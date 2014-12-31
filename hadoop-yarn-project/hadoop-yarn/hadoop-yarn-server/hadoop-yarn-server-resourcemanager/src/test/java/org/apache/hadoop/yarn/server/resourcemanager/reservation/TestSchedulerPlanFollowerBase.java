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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Assert;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public abstract class TestSchedulerPlanFollowerBase {
  final static int GB = 1024;
  protected Clock mClock = null;
  protected ResourceScheduler scheduler = null;
  protected ReservationAgent mAgent;
  protected Resource minAlloc = Resource.newInstance(GB, 1);
  protected Resource maxAlloc = Resource.newInstance(GB * 8, 8);
  protected CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
  protected Plan plan;
  private ResourceCalculator res = new DefaultResourceCalculator();

  protected void testPlanFollower(boolean isMove) throws PlanningException,
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

    AbstractSchedulerPlanFollower planFollower = createPlanFollower();

    when(mClock.getTime()).thenReturn(0L);
    planFollower.run();

    Queue q = getReservationQueue(r1.toString());
    assertReservationQueueExists(r1);
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

    Queue defQ = getDefaultQueue();
    Assert.assertEquals(0, getNumberOfApplications(defQ));

    assertReservationQueueExists(r1, 0.1, 0.1);
    Assert.assertEquals(1, getNumberOfApplications(q));

    assertReservationQueueDoesNotExist(r2);
    assertReservationQueueDoesNotExist(r3);

    when(mClock.getTime()).thenReturn(3L);
    planFollower.run();

    Assert.assertEquals(0, getNumberOfApplications(defQ));
    assertReservationQueueExists(r1, 0.1, 0.1);
    Assert.assertEquals(1, getNumberOfApplications(q));
    assertReservationQueueExists(r2, 0.1, 0.1);
    assertReservationQueueDoesNotExist(r3);

    when(mClock.getTime()).thenReturn(10L);
    planFollower.run();

    q = getReservationQueue(r1.toString());
    if (isMove) {
      // app should have been moved to default reservation queue
      Assert.assertEquals(1, getNumberOfApplications(defQ));
      assertNull(q);
    } else {
      // app should be killed
      Assert.assertEquals(0, getNumberOfApplications(defQ));
      assertNotNull(q);
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          new AppAttemptRemovedSchedulerEvent(appAttemptId_0,
              RMAppAttemptState.KILLED, false);
      scheduler.handle(appAttemptRemovedEvent);
    }
    assertReservationQueueDoesNotExist(r2);
    assertReservationQueueExists(r3, 0, 1.0);

    when(mClock.getTime()).thenReturn(11L);
    planFollower.run();

    if (isMove) {
      // app should have been moved to default reservation queue
      Assert.assertEquals(1, getNumberOfApplications(defQ));
    } else {
      // app should be killed
      Assert.assertEquals(0, getNumberOfApplications(defQ));
    }
    assertReservationQueueDoesNotExist(r1);
    assertReservationQueueDoesNotExist(r2);
    assertReservationQueueExists(r3, 0.1, 0.1);

    when(mClock.getTime()).thenReturn(12L);
    planFollower.run();

    assertReservationQueueDoesNotExist(r1);
    assertReservationQueueDoesNotExist(r2);
    assertReservationQueueExists(r3, 0.2, 0.2);

    when(mClock.getTime()).thenReturn(16L);
    planFollower.run();

    assertReservationQueueDoesNotExist(r1);
    assertReservationQueueDoesNotExist(r2);
    assertReservationQueueDoesNotExist(r3);

    verifyCapacity(defQ);
  }

  protected abstract Queue getReservationQueue(String reservationId);

  protected abstract void verifyCapacity(Queue defQ);

  protected abstract Queue getDefaultQueue();

  protected abstract int getNumberOfApplications(Queue queue);

  protected abstract AbstractSchedulerPlanFollower createPlanFollower();

  protected abstract void assertReservationQueueExists(ReservationId r);

  protected abstract void assertReservationQueueExists(ReservationId r2,
      double expectedCapacity, double expectedMaxCapacity);

  protected abstract void assertReservationQueueDoesNotExist(ReservationId r2);
}
