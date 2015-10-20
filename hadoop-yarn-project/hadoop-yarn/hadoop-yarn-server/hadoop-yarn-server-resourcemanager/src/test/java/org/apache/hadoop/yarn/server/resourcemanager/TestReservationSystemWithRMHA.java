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

import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestReservationSystemWithRMHA extends RMHATestBase{

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

    addNodeCapacityToPlan();

    ClientRMService clientService = rm1.getClientRMService();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest();
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    ReservationId reservationID = response.getReservationId();
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);
    ReservationDefinition reservationDefinition = request
        .getReservationDefinition();

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

    addNodeCapacityToPlan();

    ClientRMService clientService = rm1.getClientRMService();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest();
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    ReservationId reservationID = response.getReservationId();
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);
    ReservationDefinition reservationDefinition = request
        .getReservationDefinition();


    // Change any field

    long newDeadline = reservationDefinition.getDeadline() + 100;
    reservationDefinition.setDeadline(newDeadline);
    ReservationUpdateRequest updateRequest =
        ReservationUpdateRequest.newInstance(
          reservationDefinition, reservationID);
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

    addNodeCapacityToPlan();

    ClientRMService clientService = rm1.getClientRMService();

    // create a reservation
    ReservationSubmissionRequest request = createReservationSubmissionRequest();
    ReservationSubmissionResponse response = null;
    try {
      response = clientService.submitReservation(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    ReservationId reservationID = response.getReservationId();
    Assert.assertNotNull(reservationID);


    // Delete the reservation
    ReservationDeleteRequest deleteRequest =
        ReservationDeleteRequest.newInstance(reservationID);
    clientService.deleteReservation(deleteRequest);

    // Do the failover
    explicitFailover();

    rm2.registerNode("127.0.0.1:1", 102400, 100);

    RMState state = rm2.getRMContext().getStateStore().loadState();
    Assert.assertNull(state.getReservationState().get(
        ReservationSystemTestUtil.reservationQ));
  }

  private void addNodeCapacityToPlan() {
    try {
      rm1.registerNode("127.0.0.1:1", 102400, 100);
      int attempts = 10;
      do {
        DrainDispatcher dispatcher =
            (DrainDispatcher) rm1.getRMContext().getDispatcher();
        dispatcher.await();
        rm1.getRMContext().getReservationSystem().synchronizePlan(
            ReservationSystemTestUtil.reservationQ);
        if (rm1.getRMContext().getReservationSystem().getPlan
            (ReservationSystemTestUtil.reservationQ).getTotalCapacity()
            .getMemory() > 0) {
          break;
        }
        LOG.info("Waiting for node capacity to be added to plan");
        Thread.sleep(100);
      }
      while (attempts-- > 0);
      if (attempts <= 0) {
        Assert.fail("Exhausted attempts in checking if node capacity was " +
            "added to the plan");
      }

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private ReservationSubmissionRequest createReservationSubmissionRequest() {
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    return ReservationSystemTestUtil.createSimpleReservationRequest(4, arrival,
        deadline, duration);
  }
}
