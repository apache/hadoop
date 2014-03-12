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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.Test;

/**
 * Test to restart the AM on failure.
 *
 */
public class TestAMRestart {

  @Test
  public void testAMRestartWithExistingContainers() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);

    MockRM rm1 = new MockRM(conf);
    rm1.start();
    RMApp app1 =
        rm1.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE", false, true);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
    nm1.registerNode();
    MockNM nm2 =
        new MockNM("127.0.0.1:2351", 4089, rm1.getResourceTrackerService());
    nm2.registerNode();

    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    int NUM_CONTAINERS = 3;
    // allocate NUM_CONTAINERS containers
    am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
      new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);

    // wait for containers to be allocated.
    List<Container> containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    while (containers.size() != NUM_CONTAINERS) {
      nm1.nodeHeartbeat(true);
      containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(200);
    }

    // launch the 2nd container, for testing running container transferred.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId containerId2 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // launch the 3rd container, for testing container allocated by previous
    // attempt is completed by the next new attempt/
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 3, ContainerState.RUNNING);
    ContainerId containerId3 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 3);
    rm1.waitForState(nm1, containerId3, RMContainerState.RUNNING);

    // 4th container still in AQUIRED state. for testing Acquired container is
    // always killed.
    ContainerId containerId4 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 4);
    rm1.waitForState(nm1, containerId4, RMContainerState.ACQUIRED);

    // 5th container is in Allocated state. for testing allocated container is
    // always killed.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    ContainerId containerId5 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 5);
    rm1.waitForContainerAllocated(nm1, containerId5);
    rm1.waitForState(nm1, containerId5, RMContainerState.ALLOCATED);

    // 6th container is in Reserved state.
    am1.allocate("127.0.0.1", 6000, 1, new ArrayList<ContainerId>());
    ContainerId containerId6 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 6);
    nm1.nodeHeartbeat(true);
    SchedulerApplicationAttempt schedulerAttempt =
        ((CapacityScheduler) rm1.getResourceScheduler())
          .getCurrentAttemptForContainer(containerId6);
    while (schedulerAttempt.getReservedContainers().size() == 0) {
      System.out.println("Waiting for container " + containerId6
          + " to be reserved.");
      nm1.nodeHeartbeat(true);
      Thread.sleep(200);
    }
    // assert containerId6 is reserved.
    Assert.assertEquals(containerId6, schedulerAttempt.getReservedContainers()
      .get(0).getContainerId());

    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am1.waitForState(RMAppAttemptState.FAILED);

    // wait for some time. previous AM's running containers should still remain
    // in scheduler even though am failed
    Thread.sleep(3000);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);
    // acquired/allocated containers are cleaned up.
    Assert.assertNull(rm1.getResourceScheduler().getRMContainer(containerId4));
    Assert.assertNull(rm1.getResourceScheduler().getRMContainer(containerId5));

    // wait for app to start a new attempt.
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    // assert this is a new AM.
    ApplicationAttemptId newAttemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    Assert.assertFalse(newAttemptId.equals(am1.getApplicationAttemptId()));

    // launch the new AM
    RMAppAttempt attempt2 = app1.getCurrentAppAttempt();
    nm1.nodeHeartbeat(true);
    MockAM am2 = rm1.sendAMLaunched(attempt2.getAppAttemptId());
    RegisterApplicationMasterResponse registerResponse =
        am2.registerAppAttempt();

    // Assert two containers are running: container2 and container3;
    Assert.assertEquals(2, registerResponse.getContainersFromPreviousAttempts()
      .size());
    boolean containerId2Exists = false, containerId3Exists = false;
    for (Container container : registerResponse
      .getContainersFromPreviousAttempts()) {
      if (container.getId().equals(containerId2)) {
        containerId2Exists = true;
      }
      if (container.getId().equals(containerId3)) {
        containerId3Exists = true;
      }
    }
    Assert.assertTrue(containerId2Exists && containerId3Exists);
    rm1.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // complete container by sending the container complete event which has earlier
    // attempt's attemptId
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 3, ContainerState.COMPLETE);
    rm1.waitForState(nm1, containerId3, RMContainerState.COMPLETED);

    // Even though the completed container containerId3 event was sent to the
    // earlier failed attempt, new RMAppAttempt can also capture this container
    // info.
    // completed containerId4 is also transferred to the new attempt.
    RMAppAttempt newAttempt =
        app1.getRMAppAttempt(am2.getApplicationAttemptId());
    // 4 containers finished, acquired/allocated/reserved/completed.
    Assert.assertEquals(4, newAttempt.getJustFinishedContainers().size());
    boolean container3Exists = false, container4Exists = false, container5Exists =
        false, container6Exists = false;
    for(ContainerStatus status :  newAttempt.getJustFinishedContainers()) {
      if(status.getContainerId().equals(containerId3)) {
        // containerId3 is the container ran by previous attempt but finished by the
        // new attempt.
        container3Exists = true;
      }
      if (status.getContainerId().equals(containerId4)) {
        // containerId4 is the Acquired Container killed by the previous attempt,
        // it's now inside new attempt's finished container list.
        container4Exists = true;
      }
      if (status.getContainerId().equals(containerId5)) {
        // containerId5 is the Allocated container killed by previous failed attempt.
        container5Exists = true;
      }
      if (status.getContainerId().equals(containerId6)) {
        // containerId6 is the reserved container killed by previous failed attempt.
        container6Exists = true;
      }
    }
    Assert.assertTrue(container3Exists && container4Exists && container5Exists
        && container6Exists);

    // New SchedulerApplicationAttempt also has the containers info.
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // record the scheduler attempt for testing.
    SchedulerApplicationAttempt schedulerNewAttempt =
        ((CapacityScheduler) rm1.getResourceScheduler())
          .getCurrentAttemptForContainer(containerId2);
    // finish this application
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am2);

    // the 2nd attempt released the 1st attempt's running container, when the
    // 2nd attempt finishes.
    Assert.assertFalse(schedulerNewAttempt.getLiveContainers().contains(
      containerId2));
    // all 4 normal containers finished.
    Assert.assertEquals(5, newAttempt.getJustFinishedContainers().size());

    rm1.stop();
  }

  @Test
  public void testNMTokensRebindOnAMRestart() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 3);

    MockRM rm1 = new MockRM(conf);
    rm1.start();
    RMApp app1 =
        rm1.submitApp(200, "myname", "myuser",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE", false, true);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();
    MockNM nm2 =
        new MockNM("127.1.1.1:4321", 8000, rm1.getResourceTrackerService());
    nm2.registerNode();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    int NUM_CONTAINERS = 1;
    List<Container> containers = new ArrayList<Container>();
    // nmTokens keeps track of all the nmTokens issued in the allocate call.
    List<NMToken> expectedNMTokens = new ArrayList<NMToken>();

    // am1 allocate 1 container on nm1.
    while (true) {
      AllocateResponse response =
          am1.allocate("127.0.0.1", 2000, NUM_CONTAINERS,
            new ArrayList<ContainerId>());
      nm1.nodeHeartbeat(true);
      containers.addAll(response.getAllocatedContainers());
      expectedNMTokens.addAll(response.getNMTokens());
      if (containers.size() == NUM_CONTAINERS) {
        break;
      }
      Thread.sleep(200);
      System.out.println("Waiting for container to be allocated.");
    }
    // launch the container
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId containerId2 =
        ContainerId.newInstance(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // fail am1
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am1.waitForState(RMAppAttemptState.FAILED);
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);

    // restart the am
    MockAM am2 = MockRM.launchAM(app1, rm1, nm1);
    RegisterApplicationMasterResponse registerResponse =
        am2.registerAppAttempt();
    rm1.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // check am2 get the nm token from am1.
    Assert.assertEquals(expectedNMTokens,
      registerResponse.getNMTokensFromPreviousAttempts());

    // am2 allocate 1 container on nm2
    containers = new ArrayList<Container>();
    while (true) {
      AllocateResponse allocateResponse =
          am2.allocate("127.1.1.1", 4000, NUM_CONTAINERS,
            new ArrayList<ContainerId>());
      nm2.nodeHeartbeat(true);
      containers.addAll(allocateResponse.getAllocatedContainers());
      expectedNMTokens.addAll(allocateResponse.getNMTokens());
      if (containers.size() == NUM_CONTAINERS) {
        break;
      }
      Thread.sleep(200);
      System.out.println("Waiting for container to be allocated.");
    }
    nm1.nodeHeartbeat(am2.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId am2ContainerId2 =
        ContainerId.newInstance(am2.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, am2ContainerId2, RMContainerState.RUNNING);

    // fail am2.
    nm1.nodeHeartbeat(am2.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am2.waitForState(RMAppAttemptState.FAILED);
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);

    // restart am
    MockAM am3 = MockRM.launchAM(app1, rm1, nm1);
    registerResponse = am3.registerAppAttempt();
    rm1.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // check am3 get the NM token from both am1 and am2;
    List<NMToken> transferredTokens = registerResponse.getNMTokensFromPreviousAttempts();
    Assert.assertEquals(2, transferredTokens.size());
    Assert.assertTrue(transferredTokens.containsAll(expectedNMTokens));
    rm1.stop();
  }
}
