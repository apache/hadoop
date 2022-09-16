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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationDeleteRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationListRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationSubmissionRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationUpdateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestReservationInputValidator {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestReservationInputValidator.class);

  private static final String PLAN_NAME = "test-reservation";

  private Clock clock;
  private Map<String, Plan> plans = new HashMap<String, Plan>(1);
  private ReservationSystem rSystem;
  private Plan plan;

  private ReservationInputValidator rrValidator;

  @BeforeEach
  public void setUp() {
    clock = mock(Clock.class);
    plan = mock(Plan.class);
    rSystem = mock(ReservationSystem.class);
    plans.put(PLAN_NAME, plan);
    rrValidator = new ReservationInputValidator(clock);
    when(clock.getTime()).thenReturn(1L);
    ResourceCalculator rCalc = new DefaultResourceCalculator();
    Resource resource = Resource.newInstance(10240, 10);
    when(plan.getResourceCalculator()).thenReturn(rCalc);
    when(plan.getTotalCapacity()).thenReturn(resource);
    when(plan.getMaximumPeriodicity()).thenReturn(
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY);
    when(rSystem.getQueueForReservation(any(ReservationId.class))).thenReturn(
        PLAN_NAME);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(plan);
  }

  @AfterEach
  public void tearDown() {
    rrValidator = null;
    clock = null;
    plan = null;
  }

  @Test
  void testSubmitReservationNormal() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
    } catch (YarnException e) {
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(plan);
  }

  @Test
  void testSubmitReservationDoesNotExist() {
    ReservationSubmissionRequest request =
        new ReservationSubmissionRequestPBImpl();
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertEquals("The queue is not specified. Please try again with a "
          + "valid reservable queue.", message);
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationInvalidPlan() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(null);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message
              .endsWith(" is not managed by reservation system. Please try again with a valid reservable queue."));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationNoDefinition() {
    ReservationSubmissionRequest request =
        new ReservationSubmissionRequestPBImpl();
    request.setQueue(PLAN_NAME);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertEquals("Missing reservation definition. Please try again by "
          + "specifying a reservation definition.", message);
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationInvalidDeadline() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 0, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("The specified deadline: 0 is the past"));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationInvalidRR() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(0, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationEmptyRR() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationInvalidDuration() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 3, 4);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message.startsWith("The time difference"));
      Assertions
          .assertTrue(message
              .contains("must  be greater or equal to the minimum resource duration"));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationExceedsGangSize() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 4);
    Resource resource = Resource.newInstance(512, 1);
    when(plan.getTotalCapacity()).thenReturn(resource);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message.startsWith(
          "The size of the largest gang in the reservation definition"));
      Assertions.assertTrue(message.contains(
          "exceed the capacity available "));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationValidRecurrenceExpression() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3, "600000");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
    } catch (YarnException e) {
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(plan);
  }

  @Test
  void testSubmitReservationNegativeRecurrenceExpression() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3, "-1234");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("Negative Period : "));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationMaxPeriodIndivisibleByRecurrenceExp() {
    long indivisibleRecurrence =
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY / 2 + 1;
    String recurrenceExp = Long.toString(indivisibleRecurrence);
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3, recurrenceExp);
    plan = null;
    try {
      plan = rrValidator.validateReservationSubmissionRequest(rSystem, request,
          ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message.startsWith("The maximum periodicity:"));
      LOG.info(message);
    }
  }

  @Test
  void testSubmitReservationInvalidRecurrenceExpression() {
    // first check recurrence expression
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3, "123abc");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("Invalid period "));
      LOG.info(message);
    }

    // now check duration
    request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 50, 3, "10");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("Duration of the requested reservation:"));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationNormal() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
    } catch (YarnException e) {
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(plan);
  }

  @Test
  void testUpdateReservationNoID() {
    ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message
              .startsWith("Missing reservation id. Please try again by specifying a reservation id."));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationDoesnotExist() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 4);
    ReservationId rId = request.getReservationId();
    when(rSystem.getQueueForReservation(rId)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message.equals(MessageFormat
              .format(
                  "The specified reservation with ID: {0} is unknown. Please try again with a valid reservation.",
                  rId)));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationInvalidPlan() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 4);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message
              .endsWith(" is not associated with any valid plan. Please try again with a valid reservation."));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationNoDefinition() {
    ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
    request.setReservationId(ReservationSystemTestUtil.getNewReservationId());
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message
              .startsWith("Missing reservation definition. Please try again by specifying a reservation definition."));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationInvalidDeadline() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 0, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("The specified deadline: 0 is the past"));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationInvalidRR() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(0, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationEmptyRR() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationInvalidDuration() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 3, 4);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message
              .contains("must  be greater or equal to the minimum resource duration"));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationExceedsGangSize() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 4);
    Resource resource = Resource.newInstance(512, 1);
    when(plan.getTotalCapacity()).thenReturn(resource);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message.startsWith(
          "The size of the largest gang in the reservation definition"));
      Assertions.assertTrue(message.contains(
          "exceed the capacity available "));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationValidRecurrenceExpression() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 3, "600000");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationUpdateRequest(rSystem, request);
    } catch (YarnException e) {
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(plan);
  }

  @Test
  void testUpdateReservationNegativeRecurrenceExpression() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 3, "-1234");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("Negative Period : "));
      LOG.info(message);
    }
  }

  @Test
  void testUpdateReservationInvalidRecurrenceExpression() {
    // first check recurrence expression
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 3, "123abc");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("Invalid period "));
      LOG.info(message);
    }

    // now check duration
    request =
        createSimpleReservationUpdateRequest(1, 1, 1, 50, 3, "10");
    plan = null;
    try {
      plan =
          rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message
          .startsWith("Duration of the requested reservation:"));
      LOG.info(message);
    }
  }

  @Test
  void testDeleteReservationNormal() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    request.setReservationId(reservationID);
    ReservationAllocation reservation = mock(ReservationAllocation.class);
    when(plan.getReservationById(reservationID)).thenReturn(reservation);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
    } catch (YarnException e) {
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(plan);
  }

  @Test
  void testDeleteReservationNoID() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message
              .startsWith("Missing reservation id. Please try again by specifying a reservation id."));
      LOG.info(message);
    }
  }

  @Test
  void testDeleteReservationDoesnotExist() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    ReservationId rId = ReservationSystemTestUtil.getNewReservationId();
    request.setReservationId(rId);
    when(rSystem.getQueueForReservation(rId)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message.equals(MessageFormat
              .format(
                  "The specified reservation with ID: {0} is unknown. Please try again with a valid reservation.",
                  rId)));
      LOG.info(message);
    }
  }

  @Test
  void testDeleteReservationInvalidPlan() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    request.setReservationId(reservationID);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions
          .assertTrue(message
              .endsWith(" is not associated with any valid plan. Please try again with a valid reservation."));
      LOG.info(message);
    }
  }

  @Test
  void testListReservationsNormal() {
    ReservationListRequest request = new ReservationListRequestPBImpl();
    request.setQueue(ReservationSystemTestUtil.reservationQ);
    request.setEndTime(1000);
    request.setStartTime(0);
    when(rSystem.getPlan(ReservationSystemTestUtil.reservationQ)).thenReturn
            (this.plan);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationListRequest(rSystem, request);
    } catch (YarnException e) {
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(plan);
  }

  @Test
  void testListReservationsInvalidTimeIntervalDefaults() {
    ReservationListRequest request = new ReservationListRequestPBImpl();
    request.setQueue(ReservationSystemTestUtil.reservationQ);
    // Negative time gets converted to default values for Start Time and End
    // Time which are 0 and Long.MAX_VALUE respectively.
    request.setEndTime(-2);
    request.setStartTime(-1);
    when(rSystem.getPlan(ReservationSystemTestUtil.reservationQ)).thenReturn
            (this.plan);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationListRequest(rSystem, request);
    } catch (YarnException e) {
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(plan);
  }

  @Test
  void testListReservationsInvalidTimeInterval() {
    ReservationListRequest request = new ReservationListRequestPBImpl();
    request.setQueue(ReservationSystemTestUtil.reservationQ);
    request.setEndTime(1000);
    request.setStartTime(2000);
    when(rSystem.getPlan(ReservationSystemTestUtil.reservationQ)).thenReturn
            (this.plan);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationListRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message.equals("The specified end time must be " +
              "greater than the specified start time."));
      LOG.info(message);
    }
  }

  @Test
  void testListReservationsEmptyQueue() {
    ReservationListRequest request = new ReservationListRequestPBImpl();
    request.setQueue("");
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationListRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message.equals(
          "The queue is not specified. Please try again with a valid " +
                                      "reservable queue."));
      LOG.info(message);
    }
  }

  @Test
  void testListReservationsNullPlan() {
    ReservationListRequest request = new ReservationListRequestPBImpl();
    request.setQueue(ReservationSystemTestUtil.reservationQ);
    when(rSystem.getPlan(ReservationSystemTestUtil.reservationQ)).thenReturn
            (null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationListRequest(rSystem, request);
      Assertions.fail();
    } catch (YarnException e) {
      Assertions.assertNull(plan);
      String message = e.getMessage();
      Assertions.assertTrue(message.equals(
              "The specified queue: " + ReservationSystemTestUtil.reservationQ
            + " is not managed by reservation system."
            + " Please try again with a valid reservable queue."
      ));
      LOG.info(message);
    }
  }

  private ReservationSubmissionRequest createSimpleReservationSubmissionRequest(
      int numRequests, int numContainers, long arrival, long deadline,
      long duration) {
    return createSimpleReservationSubmissionRequest(numRequests, numContainers,
        arrival, deadline, duration, "0");
  }

  private ReservationSubmissionRequest createSimpleReservationSubmissionRequest(
      int numRequests, int numContainers, long arrival, long deadline,
      long duration, String recurrence) {
    // create a request with a single atomic ask
    ReservationSubmissionRequest request =
        new ReservationSubmissionRequestPBImpl();
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    rDef.setRecurrenceExpression(recurrence);
    if (numRequests > 0) {
      ReservationRequests reqs = new ReservationRequestsPBImpl();
      rDef.setReservationRequests(reqs);
      if (numContainers > 0) {
        ReservationRequest r =
            ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                numContainers, 1, duration);

        reqs.setReservationResources(Collections.singletonList(r));
        reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
      }
    }
    request.setQueue(PLAN_NAME);
    request.setReservationDefinition(rDef);
    return request;
  }

  private ReservationUpdateRequest createSimpleReservationUpdateRequest(
      int numRequests, int numContainers, long arrival, long deadline,
      long duration) {
    return createSimpleReservationUpdateRequest(numRequests, numContainers,
        arrival, deadline, duration, "0");
  }

  private ReservationUpdateRequest createSimpleReservationUpdateRequest(
      int numRequests, int numContainers, long arrival, long deadline,
      long duration, String recurrence) {
    // create a request with a single atomic ask
    ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    rDef.setRecurrenceExpression(recurrence);
    if (numRequests > 0) {
      ReservationRequests reqs = new ReservationRequestsPBImpl();
      rDef.setReservationRequests(reqs);
      if (numContainers > 0) {
        ReservationRequest r =
            ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                numContainers, 1, duration);

        reqs.setReservationResources(Collections.singletonList(r));
        reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
      }
    }
    request.setReservationDefinition(rDef);
    request.setReservationId(ReservationSystemTestUtil.getNewReservationId());
    return request;
  }

}
