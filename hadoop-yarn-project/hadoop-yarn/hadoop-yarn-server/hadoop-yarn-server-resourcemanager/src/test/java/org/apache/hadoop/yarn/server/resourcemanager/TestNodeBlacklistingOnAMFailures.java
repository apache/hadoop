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

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.TestAMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validate system behavior when the am-scheduling logic 'blacklists' a node for
 * an application because of AM failures.
 */
public class TestNodeBlacklistingOnAMFailures {

  @Test(timeout = 100000)
  public void testNodeBlacklistingOnAMFailure() throws Exception {

    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
        true);

    DrainDispatcher dispatcher = new DrainDispatcher();
    MockRM rm = startRM(conf, dispatcher);
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm.getResourceTrackerService());
    nm1.registerNode();

    MockNM nm2 =
        new MockNM("127.0.0.2:2345", 8000, rm.getResourceTrackerService());
    nm2.registerNode();

    RMApp app = rm.submitApp(200);

    MockAM am1 = MockRM.launchAndRegisterAM(app, rm, nm1);
    ContainerId amContainerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    RMContainer rmContainer = scheduler.getRMContainer(amContainerId);
    NodeId nodeWhereAMRan = rmContainer.getAllocatedNode();

    MockNM currentNode, otherNode;
    if (nodeWhereAMRan.equals(nm1.getNodeId())) {
      currentNode = nm1;
      otherNode = nm2;
    } else {
      currentNode = nm2;
      otherNode = nm1;
    }

    // Set the exist status to INVALID so that we can verify that the system
    // automatically blacklisting the node
    makeAMContainerExit(rm, amContainerId, currentNode,
        ContainerExitStatus.INVALID);

    // restart the am
    RMAppAttempt attempt = MockRM.waitForAttemptScheduled(app, rm);
    System.out.println("New AppAttempt launched " + attempt.getAppAttemptId());

    // Try the current node a few times
    for (int i = 0; i <= 2; i++) {
      currentNode.nodeHeartbeat(true);
      dispatcher.await();

      Assert.assertEquals(
          "AppAttemptState should still be SCHEDULED if currentNode is "
              + "blacklisted correctly", RMAppAttemptState.SCHEDULED,
          attempt.getAppAttemptState());
    }

    // Now try the other node
    otherNode.nodeHeartbeat(true);
    dispatcher.await();

    // Now the AM container should be allocated
    MockRM.waitForState(attempt, RMAppAttemptState.ALLOCATED, 20000);

    MockAM am2 = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    amContainerId =
        ContainerId.newContainerId(am2.getApplicationAttemptId(), 1);
    rmContainer = scheduler.getRMContainer(amContainerId);
    nodeWhereAMRan = rmContainer.getAllocatedNode();

    // The other node should now receive the assignment
    Assert.assertEquals(
        "After blacklisting, AM should have run on the other node",
        otherNode.getNodeId(), nodeWhereAMRan);

    am2.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);

    List<Container> allocatedContainers =
        TestAMRestart.allocateContainers(currentNode, am2, 1);
    Assert.assertEquals(
        "Even though AM is blacklisted from the node, application can "
            + "still allocate non-AM containers there",
        currentNode.getNodeId(), allocatedContainers.get(0).getNodeId());
  }

  @Test(timeout = 100000)
  public void testNoBlacklistingForNonSystemErrors() throws Exception {

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
        true);
    // disable the float so it is possible to blacklist the entire cluster
    conf.setFloat(
        YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD,
        1.5f);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 100);

    DrainDispatcher dispatcher = new DrainDispatcher();
    MockRM rm = startRM(conf, dispatcher);

    MockNM node =
        new MockNM("127.0.0.1:1234", 8000, rm.getResourceTrackerService());
    node.registerNode();

    RMApp app = rm.submitApp(200);
    ApplicationId appId = app.getApplicationId();

    int numAppAttempts = 1;

    // Now the AM container should be allocated
    RMAppAttempt attempt = MockRM.waitForAttemptScheduled(app, rm);
    node.nodeHeartbeat(true);
    dispatcher.await();
    MockRM.waitForState(attempt, RMAppAttemptState.ALLOCATED, 20000);
    rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, numAppAttempts);
    ContainerId amContainerId = ContainerId.newContainerId(appAttemptId, 1);

    for (int containerExitStatus : new int[] {
        ContainerExitStatus.PREEMPTED,
        ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
        // ContainerExitStatus.KILLED_BY_APPMASTER,
        ContainerExitStatus.KILLED_AFTER_APP_COMPLETION,
        ContainerExitStatus.ABORTED, ContainerExitStatus.DISKS_FAILED,
        ContainerExitStatus.KILLED_EXCEEDED_VMEM,
        ContainerExitStatus.KILLED_EXCEEDED_PMEM }) {

      // Set the exist status to be containerExitStatus so that we can verify
      // that the system automatically blacklisting the node
      makeAMContainerExit(rm, amContainerId, node, containerExitStatus);

      // restart the am
      attempt = MockRM.waitForAttemptScheduled(app, rm);
      System.out
          .println("New AppAttempt launched " + attempt.getAppAttemptId());

      node.nodeHeartbeat(true);
      dispatcher.await();

      MockRM.waitForState(attempt, RMAppAttemptState.ALLOCATED, 20000);
      rm.sendAMLaunched(attempt.getAppAttemptId());
      rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);

      numAppAttempts++;
      appAttemptId = ApplicationAttemptId.newInstance(appId, numAppAttempts);
      amContainerId = ContainerId.newContainerId(appAttemptId, 1);
      rm.waitForState(node, amContainerId, RMContainerState.ACQUIRED);
    }
  }

  private void makeAMContainerExit(MockRM rm, ContainerId amContainer,
      MockNM node, int exitStatus) throws Exception, InterruptedException {
    ContainerStatus containerStatus =
        BuilderUtils.newContainerStatus(amContainer, ContainerState.COMPLETE,
            "", exitStatus, Resources.createResource(200));
    node.containerStatus(containerStatus);
    ApplicationAttemptId amAttemptID = amContainer.getApplicationAttemptId();
    rm.waitForState(amAttemptID, RMAppAttemptState.FAILED);
    rm.waitForState(amAttemptID.getApplicationId(), RMAppState.ACCEPTED);
  }

  private MockRM startRM(YarnConfiguration conf,
      final DrainDispatcher dispatcher) {

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    MockRM rm1 = new MockRM(conf, memStore) {
      @Override
      protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        return new EventDispatcher<SchedulerEvent>(this.scheduler,
            this.scheduler.getClass().getName()) {
          @Override
          public void handle(SchedulerEvent event) {
            super.handle(event);
          }
        };
      }

      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };

    rm1.start();
    return rm1;
  }
}
