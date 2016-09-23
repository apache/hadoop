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
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Test;

public class TestNoOverCommitPolicy {

  long step;
  long initTime;

  InMemoryPlan plan;
  ReservationAgent mAgent;
  Resource minAlloc;
  ResourceCalculator res;
  Resource maxAlloc;

  int totCont = 1000000;

  @Before
  public void setup() throws Exception {

    // 1 sec step
    step = 1000L;

    initTime = System.currentTimeMillis();
    minAlloc = Resource.newInstance(1024, 1);
    res = new DefaultResourceCalculator();
    maxAlloc = Resource.newInstance(1024 * 8, 8);

    mAgent = mock(ReservationAgent.class);
    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    QueueMetrics rootQueueMetrics = mock(QueueMetrics.class);
    Resource clusterResource =
        ReservationSystemTestUtil.calculateClusterResource(totCont);
    ReservationSchedulerConfiguration conf = mock
        (ReservationSchedulerConfiguration.class);
    NoOverCommitPolicy policy = new NoOverCommitPolicy();
    policy.init(reservationQ, conf);
    RMContext context = ReservationSystemTestUtil.createMockRMContext();

    plan =
        new InMemoryPlan(rootQueueMetrics, policy, mAgent,
            clusterResource, step, res, minAlloc, maxAlloc,
            "dedicated", null, true, context);
  }

  public int[] generateData(int length, int val) {
    int[] data = new int[length];
    for (int i = 0; i < length; i++) {
      data[i] = val;
    }
    return data;
  }

  @Test
  public void testSingleUserEasyFitPass() throws IOException, PlanningException {
    // generate allocation that easily fit within resource constraints
    int[] f = generateData(3600, (int) Math.ceil(0.2 * totCont));
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            initTime, initTime + f.length + 1, f.length);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), rDef, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc), false));
  }

  @Test
  public void testSingleUserBarelyFitPass() throws IOException,
      PlanningException {
    // generate allocation from single tenant that barely fit
    int[] f = generateData(3600, totCont);
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            initTime, initTime + f.length + 1, f.length);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), rDef, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc), false));
  }

  @Test(expected = ResourceOverCommitException.class)
  public void testSingleFail() throws IOException, PlanningException {
    // generate allocation from single tenant that exceed capacity
    int[] f = generateData(3600, (int) (1.1 * totCont));
    plan.addReservation(new InMemoryReservationAllocation(
        ReservationSystemTestUtil.getNewReservationId(), null, "u1",
        "dedicated", initTime, initTime + f.length, ReservationSystemTestUtil
            .generateAllocation(initTime, step, f), res, minAlloc), false);
  }

  @Test
  public void testMultiTenantPass() throws IOException, PlanningException {
    // generate allocation from multiple tenants that barely fit in tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            initTime, initTime + f.length + 1, f.length);
    for (int i = 0; i < 4; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), rDef, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc), false));
    }
  }

  @Test(expected = ResourceOverCommitException.class)
  public void testMultiTenantFail() throws IOException, PlanningException {
    // generate allocation from multiple tenants that exceed tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            initTime, initTime + f.length + 1, f.length);
    for (int i = 0; i < 5; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), rDef, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc), false));
    }
  }
}
