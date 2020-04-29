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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestReservationSystemWithRMHA extends RMHATestBase {

  @Override
  public void setup() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    configuration = conf;

    super.setup();
  }

  @Test
  public void testSubmitReservationAndCheckAfterFailover() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    ClientRMService clientService = rm1.getClientRMService();

    ReservationId reservationID = getNewReservation(clientService)
        .getReservationId();
    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        reservationID);
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);

    // Do the failover
    explicitFailover();

    rm2.registerNode("127.0.0.1:1", 102400, 100);

    RMState state = rm2.getRMContext().getStateStore().loadState();
    Map<ReservationId, ReservationAllocationStateProto> reservationStateMap =
        state.getReservationState().get(ReservationSystemTestUtil.reservationQ);
    Assert.assertNotNull(reservationStateMap);
    Assert.assertNotNull(reservationStateMap.get(reservationID));
  }

  @Test
  public void testUpdateReservationAndCheckAfterFailover() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    ClientRMService clientService = rm1.getClientRMService();

    ReservationId reservationID = getNewReservation(clientService)
        .getReservationId();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        reservationID);
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);
    ReservationDefinition reservationDefinition =
        request.getReservationDefinition();

    // Change any field

    long newDeadline = reservationDefinition.getDeadline() + 100;
    reservationDefinition.setDeadline(newDeadline);
    ReservationUpdateRequest updateRequest = ReservationUpdateRequest
        .newInstance(reservationDefinition, reservationID);
    rm1.updateReservationState(updateRequest);

    // Do the failover
    explicitFailover();

    rm2.registerNode("127.0.0.1:1", 102400, 100);

    RMState state = rm2.getRMContext().getStateStore().loadState();
    Map<ReservationId, ReservationAllocationStateProto> reservationStateMap =
        state.getReservationState().get(ReservationSystemTestUtil.reservationQ);
    Assert.assertNotNull(reservationStateMap);
    ReservationAllocationStateProto reservationState =
        reservationStateMap.get(reservationID);
    Assert.assertEquals(newDeadline,
        reservationState.getReservationDefinition().getDeadline());
  }

  @Test
  public void testDeleteReservationAndCheckAfterFailover() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    ClientRMService clientService = rm1.getClientRMService();

    ReservationId reservationID = getNewReservation(clientService)
        .getReservationId();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        reservationID);
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(reservationID);

    // Delete the reservation
    ReservationDeleteRequest deleteRequest =
        ReservationDeleteRequest.newInstance(reservationID);
    clientService.deleteReservation(deleteRequest);

    // Do the failover
    explicitFailover();

    rm2.registerNode("127.0.0.1:1", 102400, 100);

    RMState state = rm2.getRMContext().getStateStore().loadState();
    Assert.assertNull(state.getReservationState()
        .get(ReservationSystemTestUtil.reservationQ));
  }

  private void addNodeCapacityToPlan(MockRM rm, int memory, int vCores) {
    try {
      rm.registerNode("127.0.0.1:1", memory, vCores);
      int attempts = 10;
      do {
        rm1.drainEvents();
        rm.getRMContext().getReservationSystem()
            .synchronizePlan(ReservationSystemTestUtil.reservationQ, false);
        if (rm.getRMContext().getReservationSystem()
            .getPlan(ReservationSystemTestUtil.reservationQ).getTotalCapacity()
            .getMemorySize() > 0) {
          break;
        }
        LOG.info("Waiting for node capacity to be added to plan");
        Thread.sleep(1000);
      } while (attempts-- > 0);
      if (attempts <= 0) {
        Assert.fail("Exhausted attempts in checking if node capacity was "
            + "added to the plan");
      }

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private ReservationSubmissionRequest createReservationSubmissionRequest(
      ReservationId reservationId) {
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + duration + 1500);
    return ReservationSystemTestUtil.createSimpleReservationRequest(
      reservationId, 4, arrival, deadline, duration);
  }

  private void validateReservation(Plan plan, ReservationId resId,
      ReservationDefinition rDef) {
    ReservationAllocation reservation = plan.getReservationById(resId);
    Assert.assertNotNull(reservation);
    Assert.assertEquals(rDef.getDeadline(),
        reservation.getReservationDefinition().getDeadline());
  }

  @Test
  public void testSubmitReservationFailoverAndDelete() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    ClientRMService clientService = rm1.getClientRMService();

    ReservationId reservationID = getNewReservation(clientService)
        .getReservationId();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        reservationID);
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);
    ReservationDefinition reservationDefinition =
        request.getReservationDefinition();

    // Do the failover
    explicitFailover();

    addNodeCapacityToPlan(rm2, 102400, 100);

    // check if reservation exists after failover
    Plan plan = rm2.getRMContext().getReservationSystem()
        .getPlan(ReservationSystemTestUtil.reservationQ);
    validateReservation(plan, reservationID, reservationDefinition);

    // delete the reservation
    ReservationDeleteRequest deleteRequest =
        ReservationDeleteRequest.newInstance(reservationID);
    ReservationDeleteResponse deleteResponse = null;
    clientService = rm2.getClientRMService();
    try {
      deleteResponse = clientService.deleteReservation(deleteRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(deleteResponse);
    Assert.assertNull(plan.getReservationById(reservationID));
  }

  @Test
  public void testFailoverAndSubmitReservation() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    // Do the failover
    explicitFailover();

    addNodeCapacityToPlan(rm2, 102400, 100);
    ClientRMService clientService = rm2.getClientRMService();

    ReservationId reservationID = getNewReservation(clientService)
        .getReservationId();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        reservationID);
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);
    ReservationDefinition reservationDefinition =
        request.getReservationDefinition();

    // check if reservation is submitted successfully
    Plan plan = rm2.getRMContext().getReservationSystem()
        .getPlan(ReservationSystemTestUtil.reservationQ);
    validateReservation(plan, reservationID, reservationDefinition);
  }

  @Test
  public void testSubmitReservationFailoverAndUpdate() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    ClientRMService clientService = rm1.getClientRMService();

    ReservationId reservationID = getNewReservation(clientService)
        .getReservationId();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        reservationID);
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);
    ReservationDefinition reservationDefinition =
        request.getReservationDefinition();

    // Do the failover
    explicitFailover();

    addNodeCapacityToPlan(rm2, 102400, 100);

    // check if reservation exists after failover
    Plan plan = rm2.getRMContext().getReservationSystem()
        .getPlan(ReservationSystemTestUtil.reservationQ);
    validateReservation(plan, reservationID, reservationDefinition);

    // update the reservation
    long newDeadline = reservationDefinition.getDeadline() + 100;
    reservationDefinition.setDeadline(newDeadline);
    ReservationUpdateRequest updateRequest = ReservationUpdateRequest
        .newInstance(reservationDefinition, reservationID);
    ReservationUpdateResponse updateResponse = null;
    clientService = rm2.getClientRMService();
    try {
      updateResponse = clientService.updateReservation(updateRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(updateResponse);
    validateReservation(plan, reservationID, reservationDefinition);
  }

  @Test
  public void testSubmitUpdateReservationFailoverAndDelete() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    ClientRMService clientService = rm1.getClientRMService();

    ReservationId reservationID = getNewReservation(clientService)
        .getReservationId();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        reservationID);
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);
    ReservationDefinition reservationDefinition =
        request.getReservationDefinition();

    // check if reservation is submitted successfully
    Plan plan = rm1.getRMContext().getReservationSystem()
        .getPlan(ReservationSystemTestUtil.reservationQ);
    validateReservation(plan, reservationID, reservationDefinition);

    // update the reservation
    long newDeadline = reservationDefinition.getDeadline() + 100;
    reservationDefinition.setDeadline(newDeadline);
    ReservationUpdateRequest updateRequest = ReservationUpdateRequest
        .newInstance(reservationDefinition, reservationID);
    ReservationUpdateResponse updateResponse = null;
    try {
      updateResponse = clientService.updateReservation(updateRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(updateResponse);
    validateReservation(plan, reservationID, reservationDefinition);

    // Do the failover
    explicitFailover();

    addNodeCapacityToPlan(rm2, 102400, 100);

    // check if reservation exists after failover
    plan = rm2.getRMContext().getReservationSystem()
        .getPlan(ReservationSystemTestUtil.reservationQ);
    validateReservation(plan, reservationID, reservationDefinition);

    // delete the reservation
    ReservationDeleteRequest deleteRequest =
        ReservationDeleteRequest.newInstance(reservationID);
    ReservationDeleteResponse deleteResponse = null;
    clientService = rm2.getClientRMService();
    try {
      deleteResponse = clientService.deleteReservation(deleteRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(deleteResponse);
    Assert.assertNull(plan.getReservationById(reservationID));
  }

  @Test
  public void testReservationResizeAfterFailover() throws Exception {
    startRMs();

    addNodeCapacityToPlan(rm1, 102400, 100);

    ClientRMService clientService = rm1.getClientRMService();

    ReservationId resID1 = getNewReservation(clientService)
        .getReservationId();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest(
        resID1);
    ReservationDefinition reservationDefinition =
        request.getReservationDefinition();
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(resID1);
    LOG.info("Submit reservation response: " + resID1);

    ReservationId resID2 = getNewReservation(clientService)
        .getReservationId();
    request.setReservationId(resID2);
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(resID2);
    LOG.info("Submit reservation response: " + resID2);

    ReservationId resID3 = getNewReservation(clientService)
        .getReservationId();
    request.setReservationId(resID3);
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertNotNull(resID3);
    LOG.info("Submit reservation response: " + resID3);

    // allow the reservations to become active
    waitForReservationActivation(rm1, resID1,
        ReservationSystemTestUtil.reservationQ);

    // validate reservations before failover
    Plan plan = rm1.getRMContext().getReservationSystem()
        .getPlan(ReservationSystemTestUtil.reservationQ);
    validateReservation(plan, resID1, reservationDefinition);
    validateReservation(plan, resID2, reservationDefinition);
    validateReservation(plan, resID3, reservationDefinition);
    ResourceScheduler scheduler = rm1.getResourceScheduler();
    QueueInfo resQ1 = scheduler.getQueueInfo(resID1.toString(), false, false);
    Assert.assertEquals(0.05, resQ1.getCapacity(), 0.001f);
    QueueInfo resQ2 = scheduler.getQueueInfo(resID2.toString(), false, false);
    Assert.assertEquals(0.05, resQ2.getCapacity(), 0.001f);
    QueueInfo resQ3 = scheduler.getQueueInfo(resID3.toString(), false, false);
    Assert.assertEquals(0.05, resQ3.getCapacity(), 0.001f);

    // Do the failover
    explicitFailover();

    addNodeCapacityToPlan(rm2, 5120, 5);

    // check if reservations exists after failover
    plan = rm2.getRMContext().getReservationSystem()
        .getPlan(ReservationSystemTestUtil.reservationQ);
    validateReservation(plan, resID1, reservationDefinition);
    validateReservation(plan, resID3, reservationDefinition);

    // verify if the reservations have been resized
    scheduler = rm2.getResourceScheduler();
    resQ1 = scheduler.getQueueInfo(resID1.toString(), false, false);
    Assert.assertEquals(1f / 3f, resQ1.getCapacity(), 0.001f);
    resQ2 = scheduler.getQueueInfo(resID2.toString(), false, false);
    Assert.assertEquals(1f / 3f, resQ2.getCapacity(), 0.001f);
    resQ3 = scheduler.getQueueInfo(resID3.toString(), false, false);
    Assert.assertEquals(1f / 3f, resQ3.getCapacity(), 0.001f);
  }

  private void waitForReservationActivation(MockRM rm,
      ReservationId reservationId, String planName) {
    try {
      int attempts = 20;
      do {
        rm.getRMContext().getReservationSystem().synchronizePlan(planName,
            false);
        if (rm.getResourceScheduler()
            .getQueueInfo(reservationId.toString(), false, false)
            .getCapacity() > 0f) {
          break;
        }
        LOG.info("Waiting for reservation to be active");
        Thread.sleep(100);
      } while (attempts-- > 0);
      if (attempts <= 0) {
        Assert
            .fail("Exceeded attempts in waiting for reservation to be active");
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private GetNewReservationResponse getNewReservation(ClientRMService
                                                        clientRMService) {
    GetNewReservationRequest newReservationRequest = GetNewReservationRequest
        .newInstance();
    GetNewReservationResponse getNewReservationResponse = null;
    try {
      getNewReservationResponse = clientRMService.getNewReservation(
        newReservationRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    return getNewReservationResponse;
  }

}
