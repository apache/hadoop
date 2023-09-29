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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ReservationACLsTestBase extends ACLsTestBase {

  private final int defaultDuration = 600000;
  private final ReservationRequest defaultRequest = ReservationRequest
          .newInstance(Resources.createResource(1024), 1, 1,
                  defaultDuration);
  private final ReservationRequests defaultRequests = ReservationRequests
          .newInstance(Collections.singletonList(defaultRequest),
          ReservationRequestInterpreter.R_ALL);
  private Configuration configuration;
  private boolean useFullQueuePath;

  public ReservationACLsTestBase(Configuration conf, boolean useFullPath) {
    configuration = conf;
    useFullQueuePath = useFullPath;
  }

  @After
  public void tearDown() {
    if (resourceManager != null) {
      resourceManager.stop();
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    return Arrays.asList(new Object[][] {
            { createCapacitySchedulerConfiguration(), false },
            { createFairSchedulerConfiguration(), true }
    });
  }

  @Test
  public void testApplicationACLs() throws Exception {
    registerNode("test:1234", 8192, 8);
    String queueA = !useFullQueuePath? QUEUEA : CapacitySchedulerConfiguration
            .ROOT + "." + QUEUEA;
    String queueB = !useFullQueuePath? QUEUEB : CapacitySchedulerConfiguration
            .ROOT + "." + QUEUEB;
    String queueC = !useFullQueuePath? QUEUEC : CapacitySchedulerConfiguration
            .ROOT + "." + QUEUEC;

    // Submit Reservations

    // Users of queue A can submit reservations on QueueA.
    verifySubmitReservationSuccess(QUEUE_A_USER, queueA);
    verifySubmitReservationSuccess(QUEUE_A_ADMIN, queueA);

    // Users of queue B cannot submit reservations on QueueA.
    verifySubmitReservationFailure(QUEUE_B_USER, queueA);
    verifySubmitReservationFailure(QUEUE_B_ADMIN, queueA);

    // Users of queue B can submit reservations on QueueB.
    verifySubmitReservationSuccess(QUEUE_B_USER, queueB);
    verifySubmitReservationSuccess(QUEUE_B_ADMIN, queueB);

    // Users of queue A cannot submit reservations on QueueB.
    verifySubmitReservationFailure(QUEUE_A_USER, queueB);
    verifySubmitReservationFailure(QUEUE_A_ADMIN, queueB);

    // Everyone can submit reservations on QueueC.
    verifySubmitReservationSuccess(QUEUE_B_USER, queueC);
    verifySubmitReservationSuccess(QUEUE_B_ADMIN, queueC);
    verifySubmitReservationSuccess(QUEUE_A_USER, queueC);
    verifySubmitReservationSuccess(QUEUE_A_ADMIN, queueC);
    verifySubmitReservationSuccess(COMMON_USER, queueC);

    // List Reservations

    // User with List Reservations, or Admin ACL can list everyone's
    // reservations.
    verifyListReservationSuccess(QUEUE_A_ADMIN, QUEUE_A_USER, queueA);
    verifyListReservationSuccess(COMMON_USER, QUEUE_A_ADMIN, queueA);
    verifyListReservationSuccess(COMMON_USER, QUEUE_A_USER, queueA);

    // User without Admin or Reservation ACL can only list their own
    // reservations by id.
    verifyListReservationSuccess(QUEUE_A_ADMIN, QUEUE_A_ADMIN, queueA);
    verifyListReservationFailure(QUEUE_A_USER, QUEUE_A_USER, queueA);
    verifyListReservationFailure(QUEUE_A_USER, QUEUE_A_ADMIN, queueA);
    verifyListReservationByIdSuccess(QUEUE_A_USER, QUEUE_A_USER, queueA);
    verifyListReservationByIdFailure(QUEUE_A_USER, QUEUE_A_ADMIN, queueA);

    // User with List Reservations, or Admin ACL can list everyone's
    // reservations.
    verifyListReservationSuccess(QUEUE_B_ADMIN, QUEUE_B_USER, queueB);
    verifyListReservationSuccess(COMMON_USER, QUEUE_B_ADMIN, queueB);
    verifyListReservationSuccess(COMMON_USER, QUEUE_B_USER, queueB);

    // User without Admin or Reservation ACL can only list their own
    // reservations by id.
    verifyListReservationSuccess(QUEUE_B_ADMIN, QUEUE_B_ADMIN, queueB);
    verifyListReservationFailure(QUEUE_B_USER, QUEUE_B_USER, queueB);
    verifyListReservationFailure(QUEUE_B_USER, QUEUE_B_ADMIN, queueB);
    verifyListReservationByIdSuccess(QUEUE_B_USER, QUEUE_B_USER, queueB);
    verifyListReservationByIdFailure(QUEUE_B_USER, QUEUE_B_ADMIN, queueB);

    // Users with Admin ACL in one queue cannot list reservations in
    // another queue
    verifyListReservationFailure(QUEUE_B_ADMIN, QUEUE_A_ADMIN, queueA);
    verifyListReservationFailure(QUEUE_B_ADMIN, QUEUE_A_USER, queueA);
    verifyListReservationFailure(QUEUE_A_ADMIN, QUEUE_B_ADMIN, queueB);
    verifyListReservationFailure(QUEUE_A_ADMIN, QUEUE_B_USER, queueB);

    // All users can list reservations on QueueC because acls are enabled
    // but not defined.
    verifyListReservationSuccess(QUEUE_A_USER, QUEUE_A_ADMIN, queueC);
    verifyListReservationSuccess(QUEUE_B_USER, QUEUE_A_ADMIN, queueC);
    verifyListReservationSuccess(QUEUE_B_ADMIN, QUEUE_A_ADMIN, queueC);
    verifyListReservationSuccess(COMMON_USER, QUEUE_A_ADMIN, queueC);
    verifyListReservationSuccess(QUEUE_A_ADMIN, QUEUE_A_USER, queueC);
    verifyListReservationSuccess(QUEUE_B_USER, QUEUE_A_USER, queueC);
    verifyListReservationSuccess(QUEUE_B_ADMIN, QUEUE_A_USER, queueC);
    verifyListReservationSuccess(COMMON_USER, QUEUE_A_USER, queueC);
    verifyListReservationByIdSuccess(QUEUE_A_USER, QUEUE_A_ADMIN, queueC);
    verifyListReservationByIdSuccess(QUEUE_B_USER, QUEUE_A_ADMIN, queueC);
    verifyListReservationByIdSuccess(QUEUE_B_ADMIN, QUEUE_A_ADMIN, queueC);
    verifyListReservationByIdSuccess(COMMON_USER, QUEUE_A_ADMIN, queueC);
    verifyListReservationByIdSuccess(QUEUE_A_ADMIN, QUEUE_A_USER, queueC);
    verifyListReservationByIdSuccess(QUEUE_B_USER, QUEUE_A_USER, queueC);
    verifyListReservationByIdSuccess(QUEUE_B_ADMIN, QUEUE_A_USER, queueC);
    verifyListReservationByIdSuccess(COMMON_USER, QUEUE_A_USER, queueC);

    // Delete Reservations

    // Only the user who made the reservation or an admin can delete it.
    verifyDeleteReservationSuccess(QUEUE_A_USER, QUEUE_A_USER, queueA);
    verifyDeleteReservationSuccess(QUEUE_A_ADMIN, QUEUE_A_USER, queueA);

    // A non-admin cannot delete another user's reservation.
    verifyDeleteReservationFailure(COMMON_USER, QUEUE_A_USER, queueA);
    verifyDeleteReservationFailure(QUEUE_B_USER, QUEUE_A_USER, queueA);
    verifyDeleteReservationFailure(QUEUE_B_ADMIN, QUEUE_A_USER, queueA);

    // Only the user who made the reservation or an admin can delete it.
    verifyDeleteReservationSuccess(QUEUE_B_USER, QUEUE_B_USER, queueB);
    verifyDeleteReservationSuccess(QUEUE_B_ADMIN, QUEUE_B_USER, queueB);

    // A non-admin cannot delete another user's reservation.
    verifyDeleteReservationFailure(COMMON_USER, QUEUE_B_USER, queueB);
    verifyDeleteReservationFailure(QUEUE_A_USER, QUEUE_B_USER, queueB);
    verifyDeleteReservationFailure(QUEUE_A_ADMIN, QUEUE_B_USER, queueB);

    // All users can delete any reservation on QueueC because acls are enabled
    // but not defined.
    verifyDeleteReservationSuccess(COMMON_USER, QUEUE_B_ADMIN, queueC);
    verifyDeleteReservationSuccess(QUEUE_B_USER, QUEUE_B_ADMIN, queueC);
    verifyDeleteReservationSuccess(QUEUE_B_ADMIN, QUEUE_B_ADMIN, queueC);
    verifyDeleteReservationSuccess(QUEUE_A_USER, QUEUE_B_ADMIN, queueC);
    verifyDeleteReservationSuccess(QUEUE_A_ADMIN, QUEUE_B_ADMIN, queueC);

    // Update Reservation

    // Only the user who made the reservation or an admin can update it.
    verifyUpdateReservationSuccess(QUEUE_A_USER, QUEUE_A_USER, queueA);
    verifyUpdateReservationSuccess(QUEUE_A_ADMIN, QUEUE_A_USER, queueA);

    // A non-admin cannot update another user's reservation.
    verifyUpdateReservationFailure(COMMON_USER, QUEUE_A_USER, queueA);
    verifyUpdateReservationFailure(QUEUE_B_USER,QUEUE_A_USER, queueA);
    verifyUpdateReservationFailure(QUEUE_B_ADMIN, QUEUE_A_USER, queueA);

    // Only the user who made the reservation or an admin can update it.
    verifyUpdateReservationSuccess(QUEUE_B_USER, QUEUE_B_USER, queueB);
    verifyUpdateReservationSuccess(QUEUE_B_ADMIN, QUEUE_B_USER, queueB);

    // A non-admin cannot update another user's reservation.
    verifyUpdateReservationFailure(COMMON_USER, QUEUE_B_USER, queueB);
    verifyUpdateReservationFailure(QUEUE_A_USER, QUEUE_B_USER, queueB);
    verifyUpdateReservationFailure(QUEUE_A_ADMIN, QUEUE_B_USER, queueB);

    // All users can update any reservation on QueueC because acls are enabled
    // but not defined.
    verifyUpdateReservationSuccess(COMMON_USER, QUEUE_B_ADMIN, queueC);
    verifyUpdateReservationSuccess(QUEUE_B_USER, QUEUE_B_ADMIN, queueC);
    verifyUpdateReservationSuccess(QUEUE_B_ADMIN, QUEUE_B_ADMIN, queueC);
    verifyUpdateReservationSuccess(QUEUE_A_USER, QUEUE_B_ADMIN, queueC);
    verifyUpdateReservationSuccess(QUEUE_A_ADMIN, QUEUE_B_ADMIN, queueC);
  }

  private void verifySubmitReservationSuccess(String submitter, String
          queueName) throws Exception {
    ReservationId reservationId = createReservation(submitter);
    submitReservation(submitter, queueName, reservationId);

    deleteReservation(submitter, reservationId);
  }

  private void verifySubmitReservationFailure(String submitter, String
          queueName) throws Exception {
    try {
      ReservationId reservationId = createReservation(submitter);
      submitReservation(submitter, queueName, reservationId);
      Assert.fail("Submit reservation by the enemy should fail!");
    } catch (YarnException e) {
      handleAdministerException(e, submitter, queueName, ReservationACL
              .SUBMIT_RESERVATIONS.name());
    }
  }

  private void verifyListReservationSuccess(String lister, String
          originalSubmitter, String queueName) throws Exception {
    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);

    ReservationListResponse adminResponse = listReservation(lister, queueName);

    assert(adminResponse.getReservationAllocationState().size() == 1);
    assert(adminResponse.getReservationAllocationState().get(0).getUser()
            .equals(originalSubmitter));

    deleteReservation(originalSubmitter, reservationId);
  }

  private void verifyListReservationFailure(String lister,
          String originalSubmitter, String queueName) throws Exception {
    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);

    try {
      listReservation(lister, queueName);
      Assert.fail("List reservation by the enemy should fail!");
    } catch (YarnException e) {
      handleAdministerException(e, lister, queueName, ReservationACL
              .LIST_RESERVATIONS.name());
    }

    deleteReservation(originalSubmitter, reservationId);
  }

  private void verifyListReservationByIdSuccess(String lister, String
          originalSubmitter, String queueName) throws Exception {
    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);

    ReservationListResponse adminResponse = listReservationById(lister,
            reservationId, queueName);

    assert(adminResponse.getReservationAllocationState().size() == 1);
    assert(adminResponse.getReservationAllocationState().get(0).getUser()
            .equals(originalSubmitter));

    deleteReservation(originalSubmitter, reservationId);
  }

  private void verifyListReservationByIdFailure(String lister,
          String originalSubmitter, String queueName) throws Exception {
    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);
    try {
      listReservationById(lister, reservationId, queueName);
      Assert.fail("List reservation by the enemy should fail!");
    } catch (YarnException e) {
      handleAdministerException(e, lister, queueName, ReservationACL
              .LIST_RESERVATIONS.name());
    }

    deleteReservation(originalSubmitter, reservationId);
  }

  private void verifyDeleteReservationSuccess(String killer,
          String originalSubmitter, String queueName) throws Exception {
    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);

    deleteReservation(killer, reservationId);
  }

  private void verifyDeleteReservationFailure(String killer,
          String originalSubmitter, String queueName) throws Exception {

    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);

    try {
      deleteReservation(killer, reservationId);
      Assert.fail("Reservation deletion by the enemy should fail!");
    } catch (YarnException e) {
      handleAdministerException(e, killer, queueName, ReservationACL
              .ADMINISTER_RESERVATIONS.name());
    }

    deleteReservation(originalSubmitter, reservationId);
  }

  private void verifyUpdateReservationSuccess(String updater,
          String originalSubmitter, String queueName) throws Exception {
    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);

    final ReservationUpdateRequest updateRequest =
            ReservationUpdateRequest.newInstance(
                    makeSimpleReservationDefinition(), reservationId);

    ApplicationClientProtocol ownerClient = getRMClientForUser(updater);

    ownerClient.updateReservation(updateRequest);

    deleteReservation(updater, reservationId);
  }

  private void verifyUpdateReservationFailure(String updater,
          String originalSubmitter, String queueName) throws Exception {
    ReservationId reservationId = createReservation(originalSubmitter);
    submitReservation(originalSubmitter, queueName, reservationId);

    final ReservationUpdateRequest updateRequest =
            ReservationUpdateRequest.newInstance(
                    makeSimpleReservationDefinition(), reservationId);

    ApplicationClientProtocol unauthorizedClient = getRMClientForUser(updater);
    try {
      unauthorizedClient.updateReservation(updateRequest);
      Assert.fail("Reservation updating by the enemy should fail.");
    } catch (YarnException e) {
      handleAdministerException(e, updater, queueName, ReservationACL
              .ADMINISTER_RESERVATIONS.name());
    }

    deleteReservation(originalSubmitter, reservationId);
  }

  private ReservationDefinition makeSimpleReservationDefinition() {
    long arrival = System.currentTimeMillis();

    String reservationName = UUID.randomUUID().toString();
    return ReservationDefinition.newInstance
            (arrival, arrival + (int)(defaultDuration * 1.1), defaultRequests,
                    reservationName);
  }

  private ReservationListResponse listReservationById(String lister,
          ReservationId reservationId, String queueName) throws Exception {
    final ReservationListRequest listRequest =
            ReservationListRequest.newInstance(queueName, reservationId
                    .toString(), -1, -1, false);

    ApplicationClientProtocol ownerClient = getRMClientForUser(lister);

    return ownerClient.listReservations(listRequest);
  }

  private ReservationListResponse listReservation(String lister,
                                          String queueName) throws Exception {
    final ReservationListRequest listRequest =
            ReservationListRequest.newInstance(queueName, null, -1, -1, false);

    ApplicationClientProtocol ownerClient = getRMClientForUser(lister);

    return ownerClient.listReservations(listRequest);
  }

  private void deleteReservation(String deleter, ReservationId id) throws
          Exception {

    ApplicationClientProtocol deleteClient = getRMClientForUser(deleter);

    final ReservationDeleteRequest deleteRequest = ReservationDeleteRequest
            .newInstance(id);

    deleteClient.deleteReservation(deleteRequest);
  }

  private ReservationId createReservation(String creator) throws Exception {

    ApplicationClientProtocol creatorClient = getRMClientForUser(creator);
    GetNewReservationRequest getNewReservationRequest =
        GetNewReservationRequest.newInstance();

    GetNewReservationResponse response = creatorClient
        .getNewReservation(getNewReservationRequest);
    return response.getReservationId();
  }

  private void submitReservation(String submitter,
      String queueName, ReservationId reservationId) throws Exception {

    ApplicationClientProtocol submitterClient = getRMClientForUser(submitter);
    ReservationSubmissionRequest reservationSubmissionRequest =
        ReservationSubmissionRequest.newInstance(
        makeSimpleReservationDefinition(), queueName, reservationId);

    ReservationSubmissionResponse response = submitterClient
            .submitReservation(reservationSubmissionRequest);
  }

  private void handleAdministerException(Exception e, String user, String
          queue, String operation) {
    LOG.info("Got exception while killing app as the enemy", e);
    Assert.assertTrue(e.getMessage().contains("User " + user
            + " cannot perform operation " + operation + " on queue "
            + queue));
  }

  private void registerNode(String host, int memory, int vCores) throws
          Exception {
    try {
      resourceManager.registerNode(host, memory, vCores);
      int attempts = 10;
      Collection<Plan> plans;
      do {
        resourceManager.drainEvents();
        LOG.info("Waiting for node capacity to be added to plan");
        plans = resourceManager.getRMContext().getReservationSystem()
                .getAllPlans().values();

        if (checkCapacity(plans)) {
          break;
        }
        Thread.sleep(100);
      } while (attempts-- > 0);
      if (attempts <= 0) {
        Assert.fail("Exhausted attempts in checking if node capacity was "
                + "added to the plan");
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private boolean checkCapacity(Collection<Plan> plans) {
    for (Plan plan : plans) {
      if (plan.getTotalCapacity().getMemorySize() > 0) {
        return true;
      }
    }
    return false;
  }

  private static Configuration createCapacitySchedulerConfiguration() {
    CapacitySchedulerConfiguration csConf =
            new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {
            QUEUEA, QUEUEB, QUEUEC });

    String absoluteQueueA = CapacitySchedulerConfiguration.ROOT + "." + QUEUEA;
    String absoluteQueueB = CapacitySchedulerConfiguration.ROOT + "." + QUEUEB;
    String absoluteQueueC = CapacitySchedulerConfiguration.ROOT + "." + QUEUEC;

    csConf.setCapacity(absoluteQueueA, 50f);
    csConf.setCapacity(absoluteQueueB, 20f);
    csConf.setCapacity(absoluteQueueC, 30f);
    csConf.setReservable(absoluteQueueA, true);
    csConf.setReservable(absoluteQueueB, true);
    csConf.setReservable(absoluteQueueC, true);

    // Set up ACLs on Queue A
    Map<ReservationACL, AccessControlList> reservationAclsOnQueueA =
            new HashMap<>();

    AccessControlList submitACLonQueueA = new AccessControlList(QUEUE_A_USER);
    AccessControlList adminACLonQueueA = new AccessControlList(QUEUE_A_ADMIN);
    AccessControlList listACLonQueueA = new AccessControlList(COMMON_USER);

    reservationAclsOnQueueA.put(ReservationACL.SUBMIT_RESERVATIONS,
            submitACLonQueueA);
    reservationAclsOnQueueA.put(ReservationACL.ADMINISTER_RESERVATIONS,
            adminACLonQueueA);
    reservationAclsOnQueueA.put(ReservationACL.LIST_RESERVATIONS,
            listACLonQueueA);

    csConf.setReservationAcls(absoluteQueueA, reservationAclsOnQueueA);

    // Set up ACLs on Queue B
    Map<ReservationACL, AccessControlList> reservationAclsOnQueueB =
            new HashMap<>();

    AccessControlList submitACLonQueueB = new AccessControlList(QUEUE_B_USER);
    AccessControlList adminACLonQueueB = new AccessControlList(QUEUE_B_ADMIN);
    AccessControlList listACLonQueueB = new AccessControlList(COMMON_USER);

    reservationAclsOnQueueB.put(ReservationACL.SUBMIT_RESERVATIONS,
            submitACLonQueueB);
    reservationAclsOnQueueB.put(ReservationACL.ADMINISTER_RESERVATIONS,
            adminACLonQueueB);
    reservationAclsOnQueueB.put(ReservationACL.LIST_RESERVATIONS,
            listACLonQueueB);

    csConf.setReservationAcls(absoluteQueueB, reservationAclsOnQueueB);

    csConf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    csConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    csConf.setBoolean(YarnConfiguration.YARN_RESERVATION_ACL_ENABLE, true);
    csConf.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getName());

    return csConf;
  }

  private static Configuration createFairSchedulerConfiguration() {
    FairSchedulerConfiguration fsConf = new FairSchedulerConfiguration();

    final String testDir = new File(System.getProperty("test.build.data",
            "/tmp")).getAbsolutePath();
    final String allocFile = new File(testDir, "test-queues.xml")
            .getAbsolutePath();

    AllocationFileWriter.create()
        .drfDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .aclSubmitReservations("queueA_user,common_user ")
            .aclAdministerReservations("queueA_admin ")
            .aclListReservations("common_user ")
            .aclSubmitApps("queueA_user,common_user ")
            .aclAdministerApps("queueA_admin ")
            .reservation().build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .aclSubmitReservations("queueB_user,common_user ")
            .aclAdministerReservations("queueB_admin ")
            .aclListReservations("common_user ")
            .aclSubmitApps("queueB_user,common_user ")
            .aclAdministerApps("queueB_admin ")
            .reservation().build())
        .addQueue(new AllocationFileQueue.Builder("queueC")
            .reservation().build())
        .writeToFile(allocFile);

    fsConf.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);

    fsConf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    fsConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    fsConf.setBoolean(YarnConfiguration.YARN_RESERVATION_ACL_ENABLE, true);
    fsConf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());

    return fsConf;
  }

  @Override
  protected Configuration createConfiguration() {
    return configuration;
  }
}
