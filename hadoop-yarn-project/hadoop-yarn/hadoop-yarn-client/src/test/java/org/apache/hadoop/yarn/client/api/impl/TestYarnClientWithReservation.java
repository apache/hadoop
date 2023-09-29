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
package org.apache.hadoop.yarn.client.api.impl;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * This class is to test class {@link YarnClient) and {@link YarnClientImpl}
 * with Reservation.
 */
@RunWith(Parameterized.class)
public class TestYarnClientWithReservation {
  protected final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();
  protected final static String FS_ALLOC_FILE =
      new File(TEST_DIR, "test-fs-queues.xml").getAbsolutePath();

  public enum SchedulerType {
    CAPACITY, FAIR
  }

  private SchedulerType schedulerType;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> getParameters() {
    return Arrays.stream(SchedulerType.values()).map(
        type -> new Object[]{type}).collect(Collectors.toList());
  }

  public TestYarnClientWithReservation(SchedulerType scheduler) {
    this.schedulerType = scheduler;
  }


  private MiniYARNCluster setupMiniYARNCluster() throws Exception {
    MiniYARNCluster cluster =
        new MiniYARNCluster("testReservationAPIs", 2, 1, 1);

    cluster.init(getConfigurationForReservation());
    cluster.start();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return cluster.getResourceManager().getRMContext()
            .getReservationSystem()
            .getPlan(ReservationSystemTestUtil.reservationQ)
            .getTotalCapacity().getMemorySize() > 6000;
      }
    }, 10, 10000);

    return cluster;
  }

  private Configuration getConfigurationForReservation() {
    Configuration conf = new Configuration();
    if (schedulerType == SchedulerType.FAIR) {
      conf = configureReservationForFairScheduler();
      conf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    } else if (schedulerType == SchedulerType.CAPACITY) {
      conf = configureReservationForCapacityScheduler();
      conf.set(YarnConfiguration.RM_SCHEDULER,
          CapacityScheduler.class.getName());
    }

    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    return conf;
  }

  private Configuration configureReservationForCapacityScheduler() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    return conf;
  }

  private Configuration configureReservationForFairScheduler() {
    Configuration conf = new Configuration();
    AllocationFileWriter.create()
        .drfDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(new AllocationFileQueue.Builder("default").build())
            .subQueue(new AllocationFileQueue.Builder("dedicated")
                .reservation()
                .weight(10)
                .build())
            .build())
        .writeToFile(FS_ALLOC_FILE);

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
    return conf;
  }

  private YarnClient setupYarnClient(MiniYARNCluster cluster) {
    final Configuration yarnConf = cluster.getConfig();
    YarnClient client = YarnClient.createYarnClient();
    client.init(yarnConf);
    client.start();
    return client;
  }

  private ReservationSubmissionRequest submitReservationTestHelper(
      YarnClient client, long arrival, long deadline, long duration)
      throws IOException, YarnException {
    ReservationId reservationID = client.createReservation().getReservationId();
    ReservationSubmissionRequest sRequest = createSimpleReservationRequest(
        reservationID, 4, arrival, deadline, duration);
    ReservationSubmissionResponse sResponse =
        client.submitReservation(sRequest);
    Assert.assertNotNull(sResponse);
    Assert.assertNotNull(reservationID);
    System.out.println("Submit reservation response: " + reservationID);

    return sRequest;
  }

  @Before
  public void setup() {
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Test
  public void testCreateReservation() throws Exception {
    MiniYARNCluster cluster = setupMiniYARNCluster();
    YarnClient client = setupYarnClient(cluster);
    try {
      Clock clock = new UTCClock();
      long arrival = clock.getTime();
      long duration = 60000;
      long deadline = (long) (arrival + 1.05 * duration);
      ReservationSubmissionRequest sRequest =
          submitReservationTestHelper(client, arrival, deadline, duration);

      // Submit the reservation again with the same request and make sure it
      // passes.
      client.submitReservation(sRequest);

      // Submit the reservation with the same reservation id but different
      // reservation definition, and ensure YarnException is thrown.
      arrival = clock.getTime();
      ReservationDefinition rDef = sRequest.getReservationDefinition();
      rDef.setArrival(arrival + duration);
      sRequest.setReservationDefinition(rDef);
      try {
        client.submitReservation(sRequest);
        Assert.fail("Reservation submission should fail if a duplicate "
            + "reservation id is used, but the reservation definition has been "
            + "updated.");
      } catch (Exception e) {
        Assert.assertTrue(e instanceof YarnException);
      }
    } finally {
      // clean-up
      if (client != null) {
        client.stop();
      }
      cluster.stop();
    }
  }

  @Test
  public void testUpdateReservation() throws Exception {
    MiniYARNCluster cluster = setupMiniYARNCluster();
    YarnClient client = setupYarnClient(cluster);
    try {
      Clock clock = new UTCClock();
      long arrival = clock.getTime();
      long duration = 60000;
      long deadline = (long) (arrival + 1.05 * duration);
      ReservationSubmissionRequest sRequest =
          submitReservationTestHelper(client, arrival, deadline, duration);

      ReservationDefinition rDef = sRequest.getReservationDefinition();
      ReservationRequest rr =
          rDef.getReservationRequests().getReservationResources().get(0);
      ReservationId reservationID = sRequest.getReservationId();
      rr.setNumContainers(5);
      arrival = clock.getTime();
      duration = 30000;
      deadline = (long) (arrival + 1.05 * duration);
      rr.setDuration(duration);
      rDef.setArrival(arrival);
      rDef.setDeadline(deadline);
      ReservationUpdateRequest uRequest =
          ReservationUpdateRequest.newInstance(rDef, reservationID);
      ReservationUpdateResponse uResponse = client.updateReservation(uRequest);
      Assert.assertNotNull(uResponse);
      System.out.println("Update reservation response: " + uResponse);
    } finally {
      // clean-up
      if (client != null) {
        client.stop();
      }
      cluster.stop();
    }
  }

  private ReservationSubmissionRequest createSimpleReservationRequest(
      ReservationId reservationId, int numContainers, long arrival,
      long deadline, long duration) {
    // create a request with a single atomic ask
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1),
            numContainers, 1, duration);
    ReservationRequests reqs =
        ReservationRequests.newInstance(Collections.singletonList(r),
            ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef =
        ReservationDefinition.newInstance(arrival, deadline, reqs,
            "testYarnClient#reservation");
    ReservationSubmissionRequest request =
        ReservationSubmissionRequest.newInstance(rDef,
            ReservationSystemTestUtil.reservationQ, reservationId);
    return request;
  }


  @Test
  public void testListReservationsByReservationId() throws Exception{
    MiniYARNCluster cluster = setupMiniYARNCluster();
    YarnClient client = setupYarnClient(cluster);
    try {
      Clock clock = new UTCClock();
      long arrival = clock.getTime();
      long duration = 60000;
      long deadline = (long) (arrival + 1.05 * duration);
      ReservationSubmissionRequest sRequest =
          submitReservationTestHelper(client, arrival, deadline, duration);

      ReservationId reservationID = sRequest.getReservationId();
      ReservationListRequest request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, reservationID.toString(), -1,
          -1, false);
      ReservationListResponse response = client.listReservations(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(1, response.getReservationAllocationState().size());
      Assert.assertEquals(response.getReservationAllocationState().get(0)
          .getReservationId().getId(), reservationID.getId());
      Assert.assertEquals(response.getReservationAllocationState().get(0)
          .getResourceAllocationRequests().size(), 0);
    } finally {
      // clean-up
      if (client != null) {
        client.stop();
      }
      cluster.stop();
    }
  }

  @Test
  public void testListReservationsByTimeInterval() throws Exception {
    MiniYARNCluster cluster = setupMiniYARNCluster();
    YarnClient client = setupYarnClient(cluster);
    try {
      Clock clock = new UTCClock();
      long arrival = clock.getTime();
      long duration = 60000;
      long deadline = (long) (arrival + 1.05 * duration);
      ReservationSubmissionRequest sRequest =
          submitReservationTestHelper(client, arrival, deadline, duration);

      // List reservations, search by a point in time within the reservation
      // range.
      arrival = clock.getTime();
      ReservationId reservationID = sRequest.getReservationId();
      ReservationListRequest request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, "", arrival + duration / 2,
          arrival + duration / 2, true);

      ReservationListResponse response = client.listReservations(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(1, response.getReservationAllocationState().size());
      Assert.assertEquals(response.getReservationAllocationState().get(0)
          .getReservationId().getId(), reservationID.getId());
      // List reservations, search by time within reservation interval.
      request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, "", 1, Long.MAX_VALUE, true);

      response = client.listReservations(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(1, response.getReservationAllocationState().size());
      Assert.assertEquals(response.getReservationAllocationState().get(0)
          .getReservationId().getId(), reservationID.getId());
      // Verify that the full resource allocations exist.
      Assert.assertTrue(response.getReservationAllocationState().get(0)
          .getResourceAllocationRequests().size() > 0);

      // Verify that the full RDL is returned.
      ReservationRequests reservationRequests =
          response.getReservationAllocationState().get(0)
              .getReservationDefinition().getReservationRequests();
      Assert.assertEquals("R_ALL",
          reservationRequests.getInterpreter().toString());
      Assert.assertTrue(reservationRequests.getReservationResources().get(0)
          .getDuration() == duration);
    } finally {
      // clean-up
      if (client != null) {
        client.stop();
      }
      cluster.stop();
    }
  }

  @Test
  public void testListReservationsByInvalidTimeInterval() throws Exception {
    MiniYARNCluster cluster = setupMiniYARNCluster();
    YarnClient client = setupYarnClient(cluster);
    try {
      Clock clock = new UTCClock();
      long arrival = clock.getTime();
      long duration = 60000;
      long deadline = (long) (arrival + 1.05 * duration);
      ReservationSubmissionRequest sRequest =
          submitReservationTestHelper(client, arrival, deadline, duration);

      // List reservations, search by invalid end time == -1.
      ReservationListRequest request = ReservationListRequest
          .newInstance(ReservationSystemTestUtil.reservationQ, "", 1, -1, true);

      ReservationListResponse response = client.listReservations(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(1, response.getReservationAllocationState().size());
      Assert.assertEquals(response.getReservationAllocationState().get(0)
          .getReservationId().getId(), sRequest.getReservationId().getId());

      // List reservations, search by invalid end time < -1.
      request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, "", 1, -10, true);

      response = client.listReservations(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(1, response.getReservationAllocationState().size());
      Assert.assertEquals(response.getReservationAllocationState().get(0)
          .getReservationId().getId(), sRequest.getReservationId().getId());
    } finally {
      // clean-up
      if (client != null) {
        client.stop();
      }
      cluster.stop();
    }
  }

  @Test
  public void testListReservationsByTimeIntervalContainingNoReservations()
      throws Exception {
    MiniYARNCluster cluster = setupMiniYARNCluster();
    YarnClient client = setupYarnClient(cluster);
    try {
      Clock clock = new UTCClock();
      long arrival = clock.getTime();
      long duration = 60000;
      long deadline = (long) (arrival + 1.05 * duration);
      ReservationSubmissionRequest sRequest =
          submitReservationTestHelper(client, arrival, deadline, duration);

      // List reservations, search by very large start time.
      ReservationListRequest request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, "", Long.MAX_VALUE, -1,
          false);

      ReservationListResponse response = client.listReservations(request);

      // Ensure all reservations are filtered out.
      Assert.assertNotNull(response);
      assertThat(response.getReservationAllocationState()).isEmpty();

      duration = 30000;
      deadline = sRequest.getReservationDefinition().getDeadline();

      // List reservations, search by start time after the reservation
      // end time.
      request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, "", deadline + duration,
          deadline + 2 * duration, false);

      response = client.listReservations(request);

      // Ensure all reservations are filtered out.
      Assert.assertNotNull(response);
      assertThat(response.getReservationAllocationState()).isEmpty();

      arrival = clock.getTime();
      // List reservations, search by end time before the reservation start
      // time.
      request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, "", 0, arrival - duration,
          false);

      response = client.listReservations(request);

      // Ensure all reservations are filtered out.
      Assert.assertNotNull(response);
      assertThat(response.getReservationAllocationState()).isEmpty();

      // List reservations, search by very small end time.
      request = ReservationListRequest
          .newInstance(ReservationSystemTestUtil.reservationQ, "", 0, 1, false);

      response = client.listReservations(request);

      // Ensure all reservations are filtered out.
      Assert.assertNotNull(response);
      assertThat(response.getReservationAllocationState()).isEmpty();

    } finally {
      // clean-up
      if (client != null) {
        client.stop();
      }
      cluster.stop();
    }
  }

  @Test
  public void testReservationDelete() throws Exception {
    MiniYARNCluster cluster = setupMiniYARNCluster();
    YarnClient client = setupYarnClient(cluster);
    try {
      Clock clock = new UTCClock();
      long arrival = clock.getTime();
      long duration = 60000;
      long deadline = (long) (arrival + 1.05 * duration);
      ReservationSubmissionRequest sRequest =
          submitReservationTestHelper(client, arrival, deadline, duration);

      ReservationId reservationID = sRequest.getReservationId();
      // Delete the reservation
      ReservationDeleteRequest dRequest =
          ReservationDeleteRequest.newInstance(reservationID);
      ReservationDeleteResponse dResponse = client.deleteReservation(dRequest);
      Assert.assertNotNull(dResponse);
      System.out.println("Delete reservation response: " + dResponse);

      // List reservations, search by non-existent reservationID
      ReservationListRequest request = ReservationListRequest.newInstance(
          ReservationSystemTestUtil.reservationQ, reservationID.toString(), -1,
          -1, false);

      ReservationListResponse response =  client.listReservations(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(0, response.getReservationAllocationState().size());
    } finally {
      // clean-up
      if (client != null) {
        client.stop();
      }
      cluster.stop();
    }
  }

}
