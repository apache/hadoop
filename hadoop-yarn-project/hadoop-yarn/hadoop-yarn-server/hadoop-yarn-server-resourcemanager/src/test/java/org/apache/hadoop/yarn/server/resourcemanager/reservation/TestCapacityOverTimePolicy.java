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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningQuotaException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCapacityOverTimePolicy {

  long timeWindow;
  long step;
  float avgConstraint;
  float instConstraint;
  long initTime;

  InMemoryPlan plan;
  ReservationAgent mAgent;
  Resource minAlloc;
  ResourceCalculator res;
  Resource maxAlloc;

  int totCont = 1000000;

  @Before
  public void setup() throws Exception {

    // 24h window
    timeWindow = 86400000L;
    // 1 sec step
    step = 1000L;

    // 25% avg cap on capacity
    avgConstraint = 25;

    // 70% instantaneous cap on capacity
    instConstraint = 70;

    initTime = System.currentTimeMillis();
    minAlloc = Resource.newInstance(1024, 1);
    res = new DefaultResourceCalculator();
    maxAlloc = Resource.newInstance(1024 * 8, 8);

    mAgent = mock(ReservationAgent.class);
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    QueueMetrics rootQueueMetrics = mock(QueueMetrics.class);
    String reservationQ = testUtil.getFullReservationQueueName();
    Resource clusterResource = testUtil.calculateClusterResource(totCont);
    ReservationSchedulerConfiguration conf =
        ReservationSystemTestUtil.createConf(reservationQ, timeWindow,
            instConstraint, avgConstraint);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);

    plan =
        new InMemoryPlan(rootQueueMetrics, policy, mAgent,
            clusterResource, step, res, minAlloc, maxAlloc,
            "dedicated", null, true);
  }

  public int[] generateData(int length, int val) {
    int[] data = new int[length];
    for (int i = 0; i < length; i++) {
      data[i] = val;
    }
    return data;
  }

  @Test
  public void testSimplePass() throws IOException, PlanningException {
    // generate allocation that simply fit within all constraints
    int[] f = generateData(3600, (int) Math.ceil(0.2 * totCont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
  }

  @Test
  public void testSimplePass2() throws IOException, PlanningException {
    // generate allocation from single tenant that exceed avg momentarily but
    // fit within
    // max instantanesou
    int[] f = generateData(3600, (int) Math.ceil(0.69 * totCont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
  }

  @Test
  public void testMultiTenantPass() throws IOException, PlanningException {
    // generate allocation from multiple tenants that barely fit in tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 4; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }

  @Test(expected = ResourceOverCommitException.class)
  public void testMultiTenantFail() throws IOException, PlanningException {
    // generate allocation from multiple tenants that exceed tot capacity
    int[] f = generateData(3600, (int) Math.ceil(0.25 * totCont));
    for (int i = 0; i < 5; i++) {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u" + i,
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
    }
  }

  @Test(expected = PlanningQuotaException.class)
  public void testInstFail() throws IOException, PlanningException {
    // generate allocation that exceed the instantaneous cap single-show
    int[] f = generateData(3600, (int) Math.ceil(0.71 * totCont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
    Assert.fail("should not have accepted this");
  }

  @Test
  public void testInstFailBySum() throws IOException, PlanningException {
    // generate allocation that exceed the instantaneous cap by sum
    int[] f = generateData(3600, (int) Math.ceil(0.3 * totCont));

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + f.length,
            ReservationSystemTestUtil.generateAllocation(initTime, step, f),
            res, minAlloc)));
    try {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u1",
              "dedicated", initTime, initTime + f.length,
              ReservationSystemTestUtil.generateAllocation(initTime, step, f),
              res, minAlloc)));
      Assert.fail();
    } catch (PlanningQuotaException p) {
      // expected
    }
  }

  @Test(expected = PlanningQuotaException.class)
  public void testFailAvg() throws IOException, PlanningException {
    // generate an allocation which violates the 25% average single-shot
    Map<ReservationInterval, ReservationRequest> req =
        new TreeMap<ReservationInterval, ReservationRequest>();
    long win = timeWindow / 2 + 100;
    int cont = (int) Math.ceil(0.5 * totCont);
    req.put(new ReservationInterval(initTime, initTime + win),
        ReservationRequest.newInstance(Resource.newInstance(1024, 1), cont));

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + win, req, res, minAlloc)));
  }

  @Test
  public void testFailAvgBySum() throws IOException, PlanningException {
    // generate an allocation which violates the 25% average by sum
    Map<ReservationInterval, ReservationRequest> req =
        new TreeMap<ReservationInterval, ReservationRequest>();
    long win = 86400000 / 4 + 1;
    int cont = (int) Math.ceil(0.5 * totCont);
    req.put(new ReservationInterval(initTime, initTime + win),
        ReservationRequest.newInstance(Resource.newInstance(1024, 1), cont));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", initTime, initTime + win, req, res, minAlloc)));

    try {
      assertTrue(plan.toString(),
          plan.addReservation(new InMemoryReservationAllocation(
              ReservationSystemTestUtil.getNewReservationId(), null, "u1",
              "dedicated", initTime, initTime + win, req, res, minAlloc)));

      Assert.fail("should not have accepted this");
    } catch (PlanningQuotaException e) {
      // expected
    }
  }

}
