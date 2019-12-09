/*****************************************************************************
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
 *****************************************************************************/

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import static org.mockito.Mockito.mock;

/**
 * General purpose ReservationAgent tester.
 */
@RunWith(Parameterized.class)
@SuppressWarnings("VisibilityModifier")
public class TestReservationAgents {

  @Parameterized.Parameter(value = 0)
  public Class agentClass;

  @Parameterized.Parameter(value = 1)
  public boolean allocateLeft;

  @Parameterized.Parameter(value = 2)
  public String recurrenceExpression;

  @Parameterized.Parameter(value = 3)
  public int numOfNodes;

  private long step;
  private Random rand = new Random(2);
  private ReservationAgent agent;
  private Plan plan;
  private ResourceCalculator resCalc = new DefaultResourceCalculator();
  private Resource minAlloc = Resource.newInstance(1024, 1);
  private Resource maxAlloc = Resource.newInstance(32 * 1023, 32);

  private long timeHorizon = 2 * 24 * 3600 * 1000; // 2 days

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReservationAgents.class);

  @Parameterized.Parameters(name = "Testing: agent {0}, allocateLeft: {1}," +
          " recurrenceExpression: {2}, numNodes: {3})")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {{GreedyReservationAgent.class, true, "0", 100 },
            {GreedyReservationAgent.class, false, "0", 100 },
            {GreedyReservationAgent.class, true, "7200000", 100 },
            {GreedyReservationAgent.class, false, "7200000", 100 },
            {GreedyReservationAgent.class, true, "86400000", 100 },
            {GreedyReservationAgent.class, false, "86400000", 100 },
            {AlignedPlannerWithGreedy.class, true, "0", 100 },
            {AlignedPlannerWithGreedy.class, false, "0", 100 },
            {AlignedPlannerWithGreedy.class, true, "7200000", 100 },
            {AlignedPlannerWithGreedy.class, false, "7200000", 100 },
            {AlignedPlannerWithGreedy.class, true, "86400000", 100 },
            {AlignedPlannerWithGreedy.class, false, "86400000", 100 } });
  }

  @Before
  public void setup() throws Exception {

    long seed = rand.nextLong();
    rand.setSeed(seed);
    LOG.info("Running with seed: " + seed);

    // setting completely loose quotas
    long timeWindow = 1000000L;
    Resource clusterCapacity =
        Resource.newInstance(numOfNodes * 1024, numOfNodes);
    step = 1000L;
    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();

    float instConstraint = 100;
    float avgConstraint = 100;

    ReservationSchedulerConfiguration conf = ReservationSystemTestUtil
        .createConf(reservationQ, timeWindow, instConstraint, avgConstraint);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);

    // setting conf to
    conf.setBoolean(GreedyReservationAgent.FAVOR_EARLY_ALLOCATION,
        allocateLeft);
    agent = (ReservationAgent) agentClass.newInstance();
    agent.init(conf);

    QueueMetrics queueMetrics = mock(QueueMetrics.class);
    RMContext context = ReservationSystemTestUtil.createMockRMContext();

    plan = new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
        resCalc, minAlloc, maxAlloc, "dedicated", null, true, context);
  }

  @Test
  public void test() throws Exception {

    long period = Long.parseLong(recurrenceExpression);
    for (int i = 0; i < 1000; i++) {
      ReservationDefinition rr = createRandomRequest(i);
      if (rr != null) {
        ReservationId reservationID =
            ReservationSystemTestUtil.getNewReservationId();
        try {
          agent.createReservation(reservationID, "u1", plan, rr);
        } catch (PlanningException p) {
          // happens
        }
      }
    }

  }

  private ReservationDefinition createRandomRequest(int i)
      throws PlanningException {
    long arrival = (long) Math.floor(rand.nextDouble() * timeHorizon);
    long period = Long.parseLong(recurrenceExpression);

    // min between period and rand around 30min
    long duration =
        (long) Math.round(Math.min(rand.nextDouble() * 3600 * 1000, period));

    // min between period and rand around 5x duration
    long deadline = (long) Math
        .ceil(arrival + Math.min(duration * rand.nextDouble() * 10, period));

    assert((deadline - arrival) <= period);

    RLESparseResourceAllocation available = plan
        .getAvailableResourceOverTime("u1", null, arrival, deadline, period);
    NavigableMap<Long, Resource> availableMap = available.getCumulative();

    // look at available space, and for each segment, use half of it with 50%
    // probability
    List<ReservationRequest> reservationRequests = new ArrayList<>();
    for (Map.Entry<Long, Resource> e : availableMap.entrySet()) {
      if (e.getValue() != null && rand.nextDouble() > 0.001) {
        int numContainers = (int) Math.ceil(Resources.divide(resCalc,
            plan.getTotalCapacity(), e.getValue(), minAlloc) / 2);
        long tempDur =
            Math.min(duration, availableMap.higherKey(e.getKey()) - e.getKey());
        reservationRequests.add(ReservationRequest.newInstance(minAlloc,
            numContainers, 1, tempDur));
      }
    }

    if (reservationRequests.size() < 1) {
      return null;
    }

    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(arrival);
    rr.setDeadline(deadline);
    rr.setRecurrenceExpression(recurrenceExpression);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ORDER);
    reqs.setReservationResources(reservationRequests);
    rr.setReservationRequests(reqs);
    rr.setReservationName("res_" + i);

    return rr;
  }

}
