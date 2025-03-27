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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.jcip.annotations.NotThreadSafe;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.StageAllocatorLowCostAligned.DurationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class tests the {@code AlignedPlannerWithGreedy} agent.
 */
@NotThreadSafe
@SuppressWarnings("VisibilityModifier")
public class TestAlignedPlanner {

  public String recurrenceExpression;

  final static String NONPERIODIC = "0";
  final static String THREEHOURPERIOD = "10800000";
  final static String ONEDAYPERIOD = "86400000";

  private static final Logger LOG = LoggerFactory
      .getLogger(TestAlignedPlanner.class);

  private ReservationAgent agentRight;
  private ReservationAgent agentLeft;
  private InMemoryPlan plan;
  private final Resource minAlloc = Resource.newInstance(1024, 1);
  private final ResourceCalculator res = new DefaultResourceCalculator();
  private final Resource maxAlloc = Resource.newInstance(1024 * 8, 8);
  private final Random rand = new Random();
  private Resource clusterCapacity;
  private long step;

  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
            {NONPERIODIC},
            {THREEHOURPERIOD},
            {ONEDAYPERIOD}
    });
  }

  public void initTestAlignedPlanner(String pRecurrenceExpression) {
    this.recurrenceExpression = pRecurrenceExpression;
  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testSingleReservationAccept(String pRecurrenceExpression) throws PlanningException {

    initTestAlignedPlanner(pRecurrenceExpression);

    // Prepare basic plan
    int numJobsInScenario = initializeScenario1();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            5 * step, // Job arrival time
            20 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(2048, 2), // Capability
                10, // Num containers
                5, // Concurrency
                10 * step) }, // Duration
            ReservationRequestInterpreter.R_ORDER, "u1");

    // Add reservation
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agentRight.createReservation(reservationID, "u1", plan, rr1);

    // CHECK: allocation was accepted
    assertTrue(reservationID != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations()
        .size() == numJobsInScenario + 1, "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    // Verify allocation
    assertTrue(check(alloc1, 10 * step, 20 * step,
        10, 2048, 2), alloc1.toString());

    System.out.println("--------AFTER AGENT----------");
    System.out.println(plan.toString());

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testOrderNoGapImpossible(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ORDER_NO_GAP, "u1");

    // Add reservation
    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
      // Expected failure
    }

    // CHECK: allocation was not accepted
    assertTrue(plan.getAllReservations().size() == numJobsInScenario,
        "Agent-based allocation should have failed");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testOrderNoGapImpossible2(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            13 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    10, // Num containers
                    10, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ORDER_NO_GAP, "u1");

    // Add reservation
    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
      // Expected failure
    }

    // CHECK: allocation was not accepted
    assertTrue(plan.getAllReservations().size() == numJobsInScenario,
        "Agent-based allocation should have failed");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testOrderImpossible(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ORDER, "u1");

    // Add reservation
    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
      // Expected failure
    }

    // CHECK: allocation was not accepted
    assertTrue(plan.getAllReservations().size() == numJobsInScenario,
        "Agent-based allocation should have failed");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testAnyImpossible(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    3 * step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ANY, "u1");

    // Add reservation
    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
      // Expected failure
    }

    // CHECK: allocation was not accepted
    assertTrue(plan.getAllReservations().size() == numJobsInScenario,
        "Agent-based allocation should have failed");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testAnyAccept(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ANY, "u1");

    // Add reservation
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agentRight.createReservation(reservationID, "u1", plan, rr1);

    // CHECK: allocation was accepted
    assertTrue(reservationID != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations()
        .size() == numJobsInScenario + 1, "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    // Verify allocation
    assertTrue(check(alloc1, 14 * step, 15 * step, 20, 1024, 1),
        alloc1.toString());

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testAllAccept(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    // Add reservation
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agentRight.createReservation(reservationID, "u1", plan, rr1);

    // CHECK: allocation was accepted
    assertTrue(reservationID != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations()
        .size() == numJobsInScenario + 1, "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    // Verify allocation
    assertTrue(check(alloc1, 10 * step, 11 * step, 20, 1024, 1),
        alloc1.toString());
    assertTrue(check(alloc1, 14 * step, 15 * step, 20, 1024, 1),
        alloc1.toString());

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testAllImpossible(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    // Add reservation
    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
      // Expected failure
    }

    // CHECK: allocation was not accepted
    assertTrue(plan.getAllReservations().size() == numJobsInScenario,
        "Agent-based allocation should have failed");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testUpdate(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Create flexible reservation
    ReservationDefinition rrFlex =
        createReservationDefinition(
            10 * step, // Job arrival time
            14 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                100, // Num containers
                1, // Concurrency
                2 * step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    // Create blocking reservation
    ReservationDefinition rrBlock =
        createReservationDefinition(
            10 * step, // Job arrival time
            11 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                100, // Num containers
                100, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    // Create reservation IDs
    ReservationId flexReservationID =
        ReservationSystemTestUtil.getNewReservationId();
    ReservationId blockReservationID =
        ReservationSystemTestUtil.getNewReservationId();

    // Add block, add flex, remove block, update flex
    agentRight.createReservation(blockReservationID, "uBlock", plan, rrBlock);
    agentRight.createReservation(flexReservationID, "uFlex", plan, rrFlex);
    agentRight.deleteReservation(blockReservationID, "uBlock", plan);
    agentRight.updateReservation(flexReservationID, "uFlex", plan, rrFlex);

    // CHECK: allocation was accepted
    assertTrue(flexReservationID != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations()
        .size() == 1, "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc1 = plan.getReservationById(flexReservationID);

    // Verify allocation
    assertTrue(check(alloc1, 10 * step, 14 * step,
        50, 1024, 1), alloc1.toString());

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testImpossibleDuration(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                20, // Num containers
                20, // Concurrency
                10 * step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    // Add reservation
    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
      // Expected failure
    }

    // CHECK: allocation was not accepted
    assertTrue(plan.getAllReservations().size() == 0,
        "Agent-based allocation should have failed");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testLoadedDurationIntervals(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    int numJobsInScenario = initializeScenario3();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            13 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                80, // Num containers
                10, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    // Add reservation
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agentRight.createReservation(reservationID, "u1", plan, rr1);

    // CHECK: allocation was accepted
    assertTrue(reservationID != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations()
        .size() == numJobsInScenario + 1, "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    // Verify allocation
    assertTrue(check(alloc1, 10 * step, 11 * step, 20, 1024, 1),
        alloc1.toString());
    assertTrue(check(alloc1, 11 * step, 12 * step, 20, 1024, 1),
        alloc1.toString());
    assertTrue(check(alloc1, 12 * step, 13 * step, 40, 1024, 1),
        alloc1.toString());
  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testCostFunction(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Create large memory reservation
    ReservationDefinition rr7Mem1Core =
        createReservationDefinition(
            10 * step, // Job arrival time
            11 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(7 * 1024, 1),// Capability
                1, // Num containers
                1, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    // Create reservation
    ReservationDefinition rr6Mem6Cores =
        createReservationDefinition(
            10 * step, // Job arrival time
            11 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(6 * 1024, 6),// Capability
                1, // Num containers
                1, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u2");

    // Create reservation
    ReservationDefinition rr =
        createReservationDefinition(
            10 * step, // Job arrival time
            12 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                1, // Num containers
                1, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u3");

    // Create reservation IDs
    ReservationId reservationID1 =
        ReservationSystemTestUtil.getNewReservationId();
    ReservationId reservationID2 =
        ReservationSystemTestUtil.getNewReservationId();
    ReservationId reservationID3 =
        ReservationSystemTestUtil.getNewReservationId();

    // Add all
    agentRight.createReservation(reservationID1, "u1", plan, rr7Mem1Core);
    agentRight.createReservation(reservationID2, "u2", plan, rr6Mem6Cores);
    agentRight.createReservation(reservationID3, "u3", plan, rr);

    // Get reservation
    ReservationAllocation alloc3 = plan.getReservationById(reservationID3);

    assertTrue(check(alloc3, 10 * step, 11 * step, 0, 1024, 1),
        alloc3.toString());
    assertTrue(check(alloc3, 11 * step, 12 * step, 1, 1024, 1),
        alloc3.toString());

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testFromCluster(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // int numJobsInScenario = initializeScenario3();

    List<ReservationDefinition> list = new ArrayList<ReservationDefinition>();

    // Create reservation
    list.add(createReservationDefinition(
        1425716392178L, // Job arrival time
        1425722262791L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            587000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u1"));

    list.add(createReservationDefinition(
        1425716406178L, // Job arrival time
        1425721255841L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            485000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u2"));

    list.add(createReservationDefinition(
        1425716399178L, // Job arrival time
        1425723780138L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            738000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u3"));

    list.add(createReservationDefinition(
        1425716437178L, // Job arrival time
        1425722968378L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            653000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u4"));

    list.add(createReservationDefinition(
        1425716406178L, // Job arrival time
        1425721926090L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            552000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u5"));

    list.add(createReservationDefinition(
        1425716379178L, // Job arrival time
        1425722238553L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            586000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u6"));

    list.add(createReservationDefinition(
        1425716407178L, // Job arrival time
        1425722908317L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            650000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u7"));

    list.add(createReservationDefinition(
        1425716452178L, // Job arrival time
        1425722841562L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            639000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u8"));

    list.add(createReservationDefinition(
        1425716384178L, // Job arrival time
        1425721766129L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            538000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u9"));

    list.add(createReservationDefinition(
        1425716437178L, // Job arrival time
        1425722507886L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            5, // Num containers
            1, // Concurrency
            607000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u10"));

    // Add reservation
    int i = 1;
    for (ReservationDefinition rr : list) {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID, "u" + Integer.toString(i),
          plan, rr);
      ++i;
    }

    // CHECK: allocation was accepted
    assertTrue(plan.getAllReservations().size() == list.size(),
        "Agent-based allocation failed");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testSingleReservationAcceptAllocateLeft(String pRecurrenceExpression)
      throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            35 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    10 * step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    10 * step) }, // Duration
            ReservationRequestInterpreter.R_ORDER, "u1");

    // Add reservation
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agentLeft.createReservation(reservationID, "u1", plan, rr1);

    // CHECK: allocation was accepted
    assertTrue(reservationID != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations().size() == 1,
        "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    // Verify allocation
    assertTrue(check(alloc1, 10 * step, 30 * step, 20, 1024, 1),
        alloc1.toString());

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testLeftSucceedsRightFails(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    // Prepare basic plan
    int numJobsInScenario = initializeScenario2();

    // Create reservation
    ReservationDefinition rr1 =
        createReservationDefinition(
            7 * step, // Job arrival time
            16 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                    20, // Num containers
                    20, // Concurrency
                    2 * step), // Duration
                ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                    20, // Num containers
                    20, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ORDER, "u1");

    ReservationDefinition rr2 =
        createReservationDefinition(
            14 * step, // Job arrival time
            16 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    100, // Num containers
                    100, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ORDER, "u2");

    // Add 1st reservation
    ReservationId reservationID1 =
        ReservationSystemTestUtil.getNewReservationId();
    agentLeft.createReservation(reservationID1, "u1", plan, rr1);

    // CHECK: allocation was accepted
    assertTrue(reservationID1 != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations().size() == numJobsInScenario + 1,
        "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc1 = plan.getReservationById(reservationID1);

    // Verify allocation
    assertTrue(check(alloc1, 7 * step, 11 * step, 20, 1024, 1), alloc1.toString());

    // Add second reservation
    ReservationId reservationID2 =
        ReservationSystemTestUtil.getNewReservationId();
    agentLeft.createReservation(reservationID2, "u2", plan, rr2);

    // CHECK: allocation was accepted
    assertTrue(reservationID2 != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations().size() == numJobsInScenario + 2,
        "Agent-based allocation failed");

    // Get reservation
    ReservationAllocation alloc2 = plan.getReservationById(reservationID2);

    // Verify allocation
    assertTrue(check(alloc2, 14 * step, 16 * step, 100, 1024, 1),
        alloc2.toString());

    agentLeft.deleteReservation(reservationID1, "u1", plan);
    agentLeft.deleteReservation(reservationID2, "u2", plan);

 // Now try to allocate the same jobs with agentRight. The second
 // job should fail
 // Add 1st reservation
    ReservationId reservationID3 =
        ReservationSystemTestUtil.getNewReservationId();
    agentRight.createReservation(reservationID3, "u1", plan, rr1);

    // CHECK: allocation was accepted
    assertTrue(reservationID3 != null, "Agent-based allocation failed");
    assertTrue(plan.getAllReservations().size() == numJobsInScenario + 1,
        "Agent-based allocation failed");

 // Add 2nd reservation
    try {
      ReservationId reservationID4 =
          ReservationSystemTestUtil.getNewReservationId();
      agentRight.createReservation(reservationID4, "u2", plan, rr2);
      fail();
    } catch (PlanningException e) {
      // Expected failure
    }

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testValidateOrderNoGap(String pRecurrenceExpression) {
    initTestAlignedPlanner(pRecurrenceExpression);
    //
    // Initialize allocations
    //

    RLESparseResourceAllocation allocation =
        new RLESparseResourceAllocation(res);
    allocation.addInterval(new ReservationInterval(10 * step, 13 * step),
        Resource.newInstance(1024, 1));

    // curAlloc
    Map<ReservationInterval, Resource> curAlloc =
        new HashMap<ReservationInterval, Resource>();

    //
    // Check cases
    //

    // 1. allocateLeft = false, succeed when there is no gap
    curAlloc.clear();
    curAlloc.put(new ReservationInterval(9 * step, 10 * step),
        Resource.newInstance(1024, 1));
    assertTrue(IterativePlanner.validateOrderNoGap(allocation, curAlloc, false),
        "validateOrderNoFap() should have succeeded");

    // 2. allocateLeft = false, fail when curAlloc has a gap
    curAlloc.put(new ReservationInterval(7 * step, 8 * step),
        Resource.newInstance(1024, 1));
    assertFalse(IterativePlanner.validateOrderNoGap(allocation, curAlloc, false),
        "validateOrderNoGap() failed to identify a gap in curAlloc");

    // 3. allocateLeft = false, fail when there is a gap between curAlloc and
    // allocations
    curAlloc.clear();
    curAlloc.put(new ReservationInterval(8 * step, 9 * step),
        Resource.newInstance(1024, 1));
    assertFalse(IterativePlanner.validateOrderNoGap(allocation, curAlloc, false),
        "validateOrderNoGap() failed to identify a gap between "
        + "allocations and curAlloc");

    // 4. allocateLeft = true, succeed when there is no gap
    curAlloc.clear();
    curAlloc.put(new ReservationInterval(13 * step, 14 * step),
        Resource.newInstance(1024, 1));
    assertTrue(IterativePlanner.validateOrderNoGap(allocation, curAlloc, true),
        "validateOrderNoFap() should have succeeded");

    // 5. allocateLeft = true, fail when there is a gap between curAlloc and
    // allocations
    curAlloc.put(new ReservationInterval(15 * step, 16 * step),
        Resource.newInstance(1024, 1));
    assertFalse(IterativePlanner.validateOrderNoGap(allocation, curAlloc, true),
        "validateOrderNoGap() failed to identify a gap in curAlloc");

    // 6. allocateLeft = true, fail when curAlloc has a gap
    curAlloc.clear();
    curAlloc.put(new ReservationInterval(14 * step, 15 * step),
        Resource.newInstance(1024, 1));
    assertFalse(IterativePlanner.validateOrderNoGap(allocation, curAlloc, true),
        "validateOrderNoGap() failed to identify a gap between "
        + "allocations and curAlloc");

  }

  @ParameterizedTest(name = "Testing: periodicity {0}")
  @MethodSource("data")
  public void testGetDurationInterval(String pRecurrenceExpression) throws PlanningException {
    initTestAlignedPlanner(pRecurrenceExpression);
    DurationInterval durationInterval = null;

    // Create netRLERes:
    //    - 4GB & 4VC between [10,20) and [30,40)
    //    - 8GB & 8VC between [20,30)
    RLESparseResourceAllocation netRLERes =
        new RLESparseResourceAllocation(res);
    netRLERes.addInterval(
        new ReservationInterval(10 * step, 40 * step),
        Resource.newInstance(4096, 4)
    );
    netRLERes.addInterval(
        new ReservationInterval(20 * step, 30 * step),
        Resource.newInstance(4096, 4)
    );

    // Create planLoads:
    //    - 5GB & 5VC between [20,30)
    RLESparseResourceAllocation planLoads =
        new RLESparseResourceAllocation(res);
    planLoads.addInterval(
        new ReservationInterval(20 * step, 30 * step),
        Resource.newInstance(5120, 5)
    );

    // Create planModifications:
    //    - 1GB & 1VC between [25,35)
    RLESparseResourceAllocation planModifications =
        new RLESparseResourceAllocation(res);
    planModifications.addInterval(
        new ReservationInterval(25 * step, 35 * step),
        Resource.newInstance(1024, 1)
    );

    // Set requested resources
    Resource requestedResources = Resource.newInstance(1024, 1);


    // 1.
    // currLoad: should start at 20*step, end at 30*step with a null value
    //   (in getTotalCost(), after the for loop we will have loadPrev == null
    // netAvailableResources: should start exactly at startTime (10*step),
    //   end exactly at endTime (30*step) with a null value
    durationInterval =
        StageAllocatorLowCostAligned.getDurationInterval(10*step, 30*step,
            planLoads, planModifications, clusterCapacity, netRLERes, res, step,
            requestedResources);
    assertThat(durationInterval.numCanFit()).isEqualTo(4);
    assertThat(durationInterval.getTotalCost()).
        isCloseTo(0.55, within(0.00001));

    // 2.
    // currLoad: should start at 20*step, end at 31*step with a null value
    //   (in getTotalCost, after the for loop we will have loadPrev == null)
    // netAvailableResources: should start exactly at startTime (10*step),
    //    end exactly at endTime (31*step) with a null value
    durationInterval =
        StageAllocatorLowCostAligned.getDurationInterval(10*step, 31*step,
            planLoads, planModifications, clusterCapacity, netRLERes, res, step,
            requestedResources);
    System.out.println(durationInterval);
    assertThat(durationInterval.numCanFit()).isEqualTo(3);
    assertThat(durationInterval.getTotalCost()).
        isCloseTo(0.56, within(0.00001));

    // 3.
    // currLoad: should start at 20*step, end at 30*step with a null value
    //   (in getTotalCost, after the for loop we will have loadPrev == null)
    // netAvailableResources: should start exactly startTime (15*step),
    //    end exactly at endTime (30*step) with a null value
    durationInterval =
        StageAllocatorLowCostAligned.getDurationInterval(15*step, 30*step,
            planLoads, planModifications, clusterCapacity, netRLERes, res, step,
            requestedResources);
    assertThat(durationInterval.numCanFit()).isEqualTo(4);
    assertThat(durationInterval.getTotalCost()).
        isCloseTo(0.55, within(0.00001));

    // 4.
    // currLoad: should start at 20*step, end at 31*step with a null value
    //   (in getTotalCost, after the for loop we will have loadPrev == null)
    // netAvailableResources: should start exactly at startTime (15*step),
    //    end exactly at endTime (31*step) with a value other than null
    durationInterval =
        StageAllocatorLowCostAligned.getDurationInterval(15*step, 31*step,
            planLoads, planModifications, clusterCapacity, netRLERes, res, step,
            requestedResources);
    System.out.println(durationInterval);
    assertThat(durationInterval.numCanFit()).isEqualTo(3);
    assertThat(durationInterval.getTotalCost()).
        isCloseTo(0.56, within(0.00001));

    // 5.
    // currLoad: should only contain one entry at startTime
    //    (22*step), therefore loadPrev != null and we should enter the if
    //    condition after the for loop in getTotalCost
    // netAvailableResources: should only contain one entry at startTime
    //    (22*step)
    durationInterval =
        StageAllocatorLowCostAligned.getDurationInterval(22*step, 23*step,
            planLoads, planModifications, clusterCapacity, netRLERes, res, step,
            requestedResources);
    System.out.println(durationInterval);
    assertThat(durationInterval.numCanFit()).isEqualTo(8);
    assertThat(durationInterval.getTotalCost()).
        isCloseTo(0.05, within(0.00001));

    // 6.
    // currLoad: should start at 39*step, end at 41*step with a null value
    //   (in getTotalCost, after the for loop we will have loadPrev == null)
    // netAvailableResources: should start exactly at startTime (39*step),
    //    end exactly at endTime (41*step) with a null value
    durationInterval =
        StageAllocatorLowCostAligned.getDurationInterval(39*step, 41*step,
            planLoads, planModifications, clusterCapacity, netRLERes, res, step,
            requestedResources);
    System.out.println(durationInterval);
    assertThat(durationInterval.numCanFit()).isEqualTo(0);
    assertThat(durationInterval.getTotalCost()).
        isCloseTo(0, within(0.00001));

  }

  @BeforeEach
  public void setup() throws Exception {

    // Initialize random seed
    long seed = rand.nextLong();
    rand.setSeed(seed);
    LOG.info("Running with seed: " + seed);

    // Set cluster parameters
    long timeWindow = 1000000L;
    int capacityMem = 100 * 1024;
    int capacityCores = 100;
    step = 60000L;

    clusterCapacity = Resource.newInstance(capacityMem, capacityCores);

    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    float instConstraint = 100;
    float avgConstraint = 100;

    ReservationSchedulerConfiguration conf = ReservationSystemTestUtil
        .createConf(reservationQ, timeWindow, instConstraint, avgConstraint);

    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);

    QueueMetrics queueMetrics = mock(QueueMetrics.class);
    RMContext context = ReservationSystemTestUtil.createMockRMContext();

    conf.setInt(AlignedPlannerWithGreedy.SMOOTHNESS_FACTOR,
        AlignedPlannerWithGreedy.DEFAULT_SMOOTHNESS_FACTOR);
    conf.setBoolean(ReservationAgent.FAVOR_EARLY_ALLOCATION, false);

    // Set planning agent
    agentRight = new AlignedPlannerWithGreedy();
    agentRight.init(conf);

    conf.setBoolean(ReservationAgent.FAVOR_EARLY_ALLOCATION, true);
    agentLeft = new AlignedPlannerWithGreedy();
    agentLeft.init(conf);

    // Create Plan
    plan = new InMemoryPlan(queueMetrics, policy, agentRight, clusterCapacity,
        step, res, minAlloc, maxAlloc, "dedicated", null, true, context);
  }

  private int initializeScenario1() throws PlanningException {

    // insert in the reservation a couple of controlled reservations, to create
    // conditions for assignment that are non-empty

    addFixedAllocation(0L, step, new int[] { 10, 10, 20, 20, 20, 10, 10 });

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    return 1;

  }

  private int initializeScenario2() throws PlanningException {

    // insert in the reservation a couple of controlled reservations, to create
    // conditions for assignment that are non-empty

    addFixedAllocation(11 * step, step, new int[] { 90, 90, 90 });

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    return 1;

  }

  private int initializeScenario3() throws PlanningException {

    // insert in the reservation a couple of controlled reservations, to create
    // conditions for assignment that are non-empty

    addFixedAllocation(10 * step, step, new int[] { 70, 80, 60 });

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    return 1;

  }

  private void addFixedAllocation(long start, long step, int[] f)
      throws PlanningException {

    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + f.length * step, f.length * step);
    assertTrue(plan.addReservation(new InMemoryReservationAllocation(
        ReservationSystemTestUtil.getNewReservationId(), rDef,
        "user_fixed", "dedicated", start, start + f.length * step,
        ReservationSystemTestUtil.generateAllocation(start, step, f), res,
        minAlloc), false), plan.toString());

  }

  private ReservationDefinition createReservationDefinition(long arrival,
      long deadline, ReservationRequest[] reservationRequests,
      ReservationRequestInterpreter rType, String username) {


    return ReservationDefinition.newInstance(arrival,
        deadline, ReservationRequests
            .newInstance(Arrays.asList(reservationRequests), rType),
        username, recurrenceExpression, Priority.UNDEFINED);

  }

  private boolean check(ReservationAllocation alloc, long start, long end,
      int containers, int mem, int cores) {

    Resource expectedResources =
        Resource.newInstance(mem * containers, cores * containers);

    // Verify that all allocations in [start,end) equal containers * (mem,cores)
    for (long i = start; i < end; i++) {
      if (!Resources.equals(alloc.getResourcesAtTime(i), expectedResources)) {
        return false;
      }
    }
    return true;

  }

}
