/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.NoOverCommitPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.SharingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Test;

public class TestSimpleCapacityReplanner {

  @Test
  public void testReplanningPlanCapacityLoss() throws PlanningException {

    Resource clusterCapacity = Resource.newInstance(100 * 1024, 100);
    Resource minAlloc = Resource.newInstance(1024, 1);
    Resource maxAlloc = Resource.newInstance(1024 * 8, 8);

    ResourceCalculator res = new DefaultResourceCalculator();
    long step = 1L;
    Clock clock = mock(Clock.class);
    ReservationAgent agent = mock(ReservationAgent.class);

    SharingPolicy policy = new NoOverCommitPolicy();
    policy.init("root.dedicated", null);

    QueueMetrics queueMetrics = mock(QueueMetrics.class);

    when(clock.getTime()).thenReturn(0L);
    SimpleCapacityReplanner enf = new SimpleCapacityReplanner(clock);

    RMContext context = ReservationSystemTestUtil.createMockRMContext();
    ReservationSchedulerConfiguration conf =
        mock(ReservationSchedulerConfiguration.class);
    when(conf.getEnforcementWindow(any(String.class))).thenReturn(6L);

    enf.init("blah", conf);

    // Initialize the plan with more resources
    InMemoryPlan plan =
        new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
            res, minAlloc, maxAlloc, "dedicated", enf, true, context, clock);

    // add reservation filling the plan (separating them 1ms, so we are sure
    // s2 follows s1 on acceptance
    long ts = System.currentTimeMillis();
    ReservationId r1 = ReservationId.newInstance(ts, 1);
    int[] f5 = { 20, 20, 20, 20, 20 };
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            0, 0 + f5.length, f5.length);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r1, rDef, "u3",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc), false));
    when(clock.getTime()).thenReturn(1L);
    ReservationId r2 = ReservationId.newInstance(ts, 2);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r2, rDef, "u4",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc), false));
    when(clock.getTime()).thenReturn(2L);
    ReservationId r3 = ReservationId.newInstance(ts, 3);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r3, rDef, "u5",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc), false));
    when(clock.getTime()).thenReturn(3L);
    ReservationId r4 = ReservationId.newInstance(ts, 4);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r4, rDef, "u6",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc), false));
    when(clock.getTime()).thenReturn(4L);
    ReservationId r5 = ReservationId.newInstance(ts, 5);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r5, rDef, "u7",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc), false));

    int[] f6 = { 50, 50, 50, 50, 50 };
    ReservationId r6 = ReservationId.newInstance(ts, 6);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r6, rDef, "u3",
            "dedicated", 10, 10 + f6.length, generateAllocation(10, f6), res,
            minAlloc), false));
    when(clock.getTime()).thenReturn(6L);
    ReservationId r7 = ReservationId.newInstance(ts, 7);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r7, rDef, "u4",
            "dedicated", 10, 10 + f6.length, generateAllocation(10, f6), res,
            minAlloc), false));

    // remove some of the resources (requires replanning)
    plan.setTotalCapacity(Resource.newInstance(70 * 1024, 70));

    when(clock.getTime()).thenReturn(0L);

    // run the replanner
    enf.plan(plan, null);

    // check which reservation are still present
    assertNotNull(plan.getReservationById(r1));
    assertNotNull(plan.getReservationById(r2));
    assertNotNull(plan.getReservationById(r3));
    assertNotNull(plan.getReservationById(r6));
    assertNotNull(plan.getReservationById(r7));

    // and which ones are removed
    assertNull(plan.getReservationById(r4));
    assertNull(plan.getReservationById(r5));

    // check resources at each moment in time no more exceed capacity
    for (int i = 0; i < 20; i++) {
      long tot = 0;
      for (ReservationAllocation r : plan.getReservationsAtTime(i)) {
        tot = r.getResourcesAtTime(i).getMemorySize();
      }
      assertTrue(tot <= 70 * 1024);
    }
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc) {
    Map<ReservationInterval, Resource> req =
        new TreeMap<ReservationInterval, Resource>();
    for (int i = 0; i < alloc.length; i++) {
      req.put(new ReservationInterval(startTime + i, startTime + i + 1),
          ReservationSystemUtil.toResource(
              ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                  alloc[i])));
    }
    return req;
  }

}
