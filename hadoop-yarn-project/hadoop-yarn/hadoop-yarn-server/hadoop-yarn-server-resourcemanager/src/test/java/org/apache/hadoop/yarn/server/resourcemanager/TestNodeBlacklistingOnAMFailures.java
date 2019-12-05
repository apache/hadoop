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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.TestAMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Validate system behavior when the am-scheduling logic 'blacklists' a node for
 * an application because of AM failures.
 */
public class TestNodeBlacklistingOnAMFailures {

  private RMApp submitAppWithAMResourceRequests(MockRM rm,
      List<ResourceRequest> amResourceRequests) throws Exception {
    int maxAttempts =
        rm.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder.create()
        .withAmResourceRequests(amResourceRequests)
        .withAppName("app1")
        .withUser("user")
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue(null)
        .withMaxAppAttempts(maxAttempts)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(true)
        .withKeepContainers(false)
        .withApplicationId(null)
        .withAttemptFailuresValidityInterval(0)
        .withLogAggregationContext(null)
        .withCancelTokensWhenComplete(true)
        .withAppPriority(amResourceRequests.get(0).getPriority())
        .withAmLabel(amResourceRequests.get(0).getNodeLabelExpression())
        .withApplicationTimeouts(null)
        .withTokensConf(null)
        .build();
    return MockRMAppSubmitter.submit(rm, data);
  }

  @Before
  public void setup() {
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Test(timeout = 100000)
  public void testNodeBlacklistingOnAMFailure() throws Exception {

    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
        true);

    MockRM rm = startRM(conf);
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();

    // Register 5 nodes, so that we can blacklist atleast one if AM container
    // is failed. As per calculation it will be like, 5nodes * 0.2 (default)=1.
    // First register 2 nodes, and after AM lauched register 3 more nodes.
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm.getResourceTrackerService());
    nm1.registerNode();

    MockNM nm2 =
        new MockNM("127.0.0.2:2345", 8000, rm.getResourceTrackerService());
    nm2.registerNode();

    RMApp app = MockRMAppSubmitter.submitWithMemory(200, rm);

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

    // register 3 nodes now
    MockNM nm3 =
        new MockNM("127.0.0.3:2345", 8000, rm.getResourceTrackerService());
    nm3.registerNode();

    MockNM nm4 =
        new MockNM("127.0.0.4:2345", 8000, rm.getResourceTrackerService());
    nm4.registerNode();

    MockNM nm5 =
        new MockNM("127.0.0.5:2345", 8000, rm.getResourceTrackerService());
    nm5.registerNode();

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
      rm.drainEvents();

      Assert.assertEquals(
          "AppAttemptState should still be SCHEDULED if currentNode is "
              + "blacklisted correctly", RMAppAttemptState.SCHEDULED,
          attempt.getAppAttemptState());
    }

    // Now try the other node
    otherNode.nodeHeartbeat(true);
    rm.drainEvents();

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
  public void testNodeBlacklistingOnAMFailureStrictNodeLocality()
      throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
        true);

    MockRM rm = startRM(conf);
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();

    // Register 5 nodes, so that we can blacklist atleast one if AM container
    // is failed. As per calculation it will be like, 5nodes * 0.2 (default)=1.
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm.getResourceTrackerService());
    nm1.registerNode();

    MockNM nm2 =
        new MockNM("127.0.0.2:2345", 8000, rm.getResourceTrackerService());
    nm2.registerNode();

    MockNM nm3 =
        new MockNM("127.0.0.3:2345", 8000, rm.getResourceTrackerService());
    nm3.registerNode();

    MockNM nm4 =
        new MockNM("127.0.0.4:2345", 8000, rm.getResourceTrackerService());
    nm4.registerNode();

    MockNM nm5 =
        new MockNM("127.0.0.5:2345", 8000, rm.getResourceTrackerService());
    nm5.registerNode();

    // Specify a strict locality on nm2
    List<ResourceRequest> reqs = new ArrayList<>();
    ResourceRequest nodeReq = ResourceRequest.newInstance(
        Priority.newInstance(0), nm2.getNodeId().getHost(),
        Resource.newInstance(200, 1), 1, true);
    ResourceRequest rackReq = ResourceRequest.newInstance(
        Priority.newInstance(0), "/default-rack",
        Resource.newInstance(200, 1), 1, false);
    ResourceRequest anyReq = ResourceRequest.newInstance(
        Priority.newInstance(0), ResourceRequest.ANY,
        Resource.newInstance(200, 1), 1, false);
    reqs.add(anyReq);
    reqs.add(rackReq);
    reqs.add(nodeReq);
    RMApp app = submitAppWithAMResourceRequests(rm, reqs);

    MockAM am1 = MockRM.launchAndRegisterAM(app, rm, nm2);
    ContainerId amContainerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    RMContainer rmContainer = scheduler.getRMContainer(amContainerId);
    NodeId nodeWhereAMRan = rmContainer.getAllocatedNode();
    Assert.assertEquals(nm2.getNodeId(), nodeWhereAMRan);

    // Set the exist status to INVALID so that we can verify that the system
    // automatically blacklisting the node
    makeAMContainerExit(rm, amContainerId, nm2, ContainerExitStatus.INVALID);

    // restart the am
    RMAppAttempt attempt = MockRM.waitForAttemptScheduled(app, rm);
    System.out.println("New AppAttempt launched " + attempt.getAppAttemptId());

    nm2.nodeHeartbeat(true);
    rm.drainEvents();

    // Now the AM container should be allocated
    MockRM.waitForState(attempt, RMAppAttemptState.ALLOCATED, 20000);

    MockAM am2 = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    amContainerId =
        ContainerId.newContainerId(am2.getApplicationAttemptId(), 1);
    rmContainer = scheduler.getRMContainer(amContainerId);
    nodeWhereAMRan = rmContainer.getAllocatedNode();

    // The second AM should be on the same node because the strict locality
    // made the eligible nodes only 1, so the blacklisting threshold kicked in
    System.out.println("AM ran on " + nodeWhereAMRan);
    Assert.assertEquals(nm2.getNodeId(), nodeWhereAMRan);

    am2.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
  }

  @Test(timeout = 100000)
  public void testNodeBlacklistingOnAMFailureRelaxedNodeLocality()
      throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
        true);

    MockRM rm = startRM(conf);
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();

    // Register 5 nodes, so that we can blacklist atleast one if AM container
    // is failed. As per calculation it will be like, 5nodes * 0.2 (default)=1.
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm.getResourceTrackerService());
    nm1.registerNode();

    MockNM nm2 =
        new MockNM("127.0.0.2:2345", 8000, rm.getResourceTrackerService());
    nm2.registerNode();

    MockNM nm3 =
        new MockNM("127.0.0.3:2345", 8000, rm.getResourceTrackerService());
    nm3.registerNode();

    MockNM nm4 =
        new MockNM("127.0.0.4:2345", 8000, rm.getResourceTrackerService());
    nm4.registerNode();

    MockNM nm5 =
        new MockNM("127.0.0.5:2345", 8000, rm.getResourceTrackerService());
    nm5.registerNode();

    // Specify a relaxed locality on nm2
    List<ResourceRequest> reqs = new ArrayList<>();
    ResourceRequest nodeReq = ResourceRequest.newInstance(
        Priority.newInstance(0), nm2.getNodeId().getHost(),
        Resource.newInstance(200, 1), 1, true);
    ResourceRequest rackReq = ResourceRequest.newInstance(
        Priority.newInstance(0), "/default-rack",
        Resource.newInstance(200, 1), 1, true);
    ResourceRequest anyReq = ResourceRequest.newInstance(
        Priority.newInstance(0), ResourceRequest.ANY,
        Resource.newInstance(200, 1), 1, true);
    reqs.add(anyReq);
    reqs.add(rackReq);
    reqs.add(nodeReq);
    RMApp app = submitAppWithAMResourceRequests(rm, reqs);

    MockAM am1 = MockRM.launchAndRegisterAM(app, rm, nm2);
    ContainerId amContainerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    RMContainer rmContainer = scheduler.getRMContainer(amContainerId);
    NodeId nodeWhereAMRan = rmContainer.getAllocatedNode();
    Assert.assertEquals(nm2.getNodeId(), nodeWhereAMRan);

    // Set the exist status to INVALID so that we can verify that the system
    // automatically blacklisting the node
    makeAMContainerExit(rm, amContainerId, nm2, ContainerExitStatus.INVALID);

    // restart the am
    RMAppAttempt attempt = MockRM.waitForAttemptScheduled(app, rm);
    System.out.println("New AppAttempt launched " + attempt.getAppAttemptId());

    nm2.nodeHeartbeat(true);
    nm1.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    nm4.nodeHeartbeat(true);
    nm5.nodeHeartbeat(true);
    rm.drainEvents();

    // Now the AM container should be allocated
    MockRM.waitForState(attempt, RMAppAttemptState.ALLOCATED, 20000);

    MockAM am2 = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    amContainerId =
        ContainerId.newContainerId(am2.getApplicationAttemptId(), 1);
    rmContainer = scheduler.getRMContainer(amContainerId);
    nodeWhereAMRan = rmContainer.getAllocatedNode();

    // The second AM should be on a different node because the relaxed locality
    // made the app schedulable on other nodes and nm2 is blacklisted
    System.out.println("AM ran on " + nodeWhereAMRan);
    Assert.assertNotEquals(nm2.getNodeId(), nodeWhereAMRan);

    am2.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
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

    MockRM rm = startRM(conf);

    MockNM node =
        new MockNM("127.0.0.1:1234", 8000, rm.getResourceTrackerService());
    node.registerNode();

    RMApp app = MockRMAppSubmitter.submitWithMemory(200, rm);
    ApplicationId appId = app.getApplicationId();

    int numAppAttempts = 1;

    // Now the AM container should be allocated
    RMAppAttempt attempt = MockRM.waitForAttemptScheduled(app, rm);
    node.nodeHeartbeat(true);
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    rm.drainEvents();
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
      ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
      rm.drainEvents();

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

  private MockRM startRM(YarnConfiguration conf) {
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MockRM rm = new MockRM(conf);
    rm.start();
    return rm;
  }
}
