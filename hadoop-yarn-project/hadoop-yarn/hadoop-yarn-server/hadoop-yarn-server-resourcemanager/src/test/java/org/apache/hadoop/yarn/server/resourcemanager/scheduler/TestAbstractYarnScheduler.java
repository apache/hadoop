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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class TestAbstractYarnScheduler extends ParameterizedSchedulerTestBase {

  public TestAbstractYarnScheduler(SchedulerType type) throws IOException {
    super(type);
  }

  @Test
  public void testMaximimumAllocationMemory() throws Exception {
    final int node1MaxMemory = 15 * 1024;
    final int node2MaxMemory = 5 * 1024;
    final int node3MaxMemory = 6 * 1024;
    final int configuredMaxMemory = 10 * 1024;
    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        configuredMaxMemory);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        1000 * 1000);
    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationMemoryHelper(
          rm.getResourceScheduler(),
          node1MaxMemory, node2MaxMemory, node3MaxMemory,
          configuredMaxMemory, configuredMaxMemory, configuredMaxMemory,
          configuredMaxMemory, configuredMaxMemory, configuredMaxMemory);
    } finally {
      rm.stop();
    }

    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);
    rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationMemoryHelper(
          rm.getResourceScheduler(),
          node1MaxMemory, node2MaxMemory, node3MaxMemory,
          configuredMaxMemory, configuredMaxMemory, configuredMaxMemory,
          node2MaxMemory, node3MaxMemory, node2MaxMemory);
    } finally {
      rm.stop();
    }
  }

  private void testMaximumAllocationMemoryHelper(
       YarnScheduler scheduler,
       final int node1MaxMemory, final int node2MaxMemory,
       final int node3MaxMemory, final int... expectedMaxMemory)
       throws Exception {
    Assert.assertEquals(6, expectedMaxMemory.length);

    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    long maxMemory = scheduler.getMaximumResourceCapability().getMemorySize();
    Assert.assertEquals(expectedMaxMemory[0], maxMemory);

    RMNode node1 = MockNodes.newNodeInfo(
        0, Resources.createResource(node1MaxMemory), 1, "127.0.0.2");
    scheduler.handle(new NodeAddedSchedulerEvent(node1));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemorySize();
    Assert.assertEquals(expectedMaxMemory[1], maxMemory);

    scheduler.handle(new NodeRemovedSchedulerEvent(node1));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemorySize();
    Assert.assertEquals(expectedMaxMemory[2], maxMemory);

    RMNode node2 = MockNodes.newNodeInfo(
        0, Resources.createResource(node2MaxMemory), 2, "127.0.0.3");
    scheduler.handle(new NodeAddedSchedulerEvent(node2));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemorySize();
    Assert.assertEquals(expectedMaxMemory[3], maxMemory);

    RMNode node3 = MockNodes.newNodeInfo(
        0, Resources.createResource(node3MaxMemory), 3, "127.0.0.4");
    scheduler.handle(new NodeAddedSchedulerEvent(node3));
    Assert.assertEquals(2, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemorySize();
    Assert.assertEquals(expectedMaxMemory[4], maxMemory);

    scheduler.handle(new NodeRemovedSchedulerEvent(node3));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemorySize();
    Assert.assertEquals(expectedMaxMemory[5], maxMemory);

    scheduler.handle(new NodeRemovedSchedulerEvent(node2));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
  }

  @Test
  public void testMaximimumAllocationVCores() throws Exception {
    final int node1MaxVCores = 15;
    final int node2MaxVCores = 5;
    final int node3MaxVCores = 6;
    final int configuredMaxVCores = 10;
    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        configuredMaxVCores);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        1000 * 1000);
    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationVCoresHelper(
          rm.getResourceScheduler(),
          node1MaxVCores, node2MaxVCores, node3MaxVCores,
          configuredMaxVCores, configuredMaxVCores, configuredMaxVCores,
          configuredMaxVCores, configuredMaxVCores, configuredMaxVCores);
    } finally {
      rm.stop();
    }

    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);
    rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationVCoresHelper(
          rm.getResourceScheduler(),
          node1MaxVCores, node2MaxVCores, node3MaxVCores,
          configuredMaxVCores, configuredMaxVCores, configuredMaxVCores,
          node2MaxVCores, node3MaxVCores, node2MaxVCores);
    } finally {
      rm.stop();
    }
  }

  private void testMaximumAllocationVCoresHelper(
      YarnScheduler scheduler,
      final int node1MaxVCores, final int node2MaxVCores,
      final int node3MaxVCores, final int... expectedMaxVCores)
      throws Exception {
    Assert.assertEquals(6, expectedMaxVCores.length);

    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    int maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[0], maxVCores);

    RMNode node1 = MockNodes.newNodeInfo(
        0, Resources.createResource(1024, node1MaxVCores), 1, "127.0.0.2");
    scheduler.handle(new NodeAddedSchedulerEvent(node1));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[1], maxVCores);

    scheduler.handle(new NodeRemovedSchedulerEvent(node1));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[2], maxVCores);

    RMNode node2 = MockNodes.newNodeInfo(
        0, Resources.createResource(1024, node2MaxVCores), 2, "127.0.0.3");
    scheduler.handle(new NodeAddedSchedulerEvent(node2));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[3], maxVCores);

    RMNode node3 = MockNodes.newNodeInfo(
        0, Resources.createResource(1024, node3MaxVCores), 3, "127.0.0.4");
    scheduler.handle(new NodeAddedSchedulerEvent(node3));
    Assert.assertEquals(2, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[4], maxVCores);

    scheduler.handle(new NodeRemovedSchedulerEvent(node3));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[5], maxVCores);

    scheduler.handle(new NodeRemovedSchedulerEvent(node2));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
  }

  @Test
  public void testUpdateMaxAllocationUsesTotal() throws IOException {
    final int configuredMaxVCores = 20;
    final int configuredMaxMemory = 10 * 1024;
    Resource configuredMaximumResource = Resource.newInstance
        (configuredMaxMemory, configuredMaxVCores);

    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        configuredMaxVCores);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        configuredMaxMemory);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);

    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      AbstractYarnScheduler scheduler = (AbstractYarnScheduler) rm
          .getResourceScheduler();

      Resource emptyResource = Resource.newInstance(0, 0);
      Resource fullResource1 = Resource.newInstance(1024, 5);
      Resource fullResource2 = Resource.newInstance(2048, 10);

      SchedulerNode mockNode1 = mock(SchedulerNode.class);
      when(mockNode1.getNodeID()).thenReturn(NodeId.newInstance("foo", 8080));
      when(mockNode1.getUnallocatedResource()).thenReturn(emptyResource);
      when(mockNode1.getTotalResource()).thenReturn(fullResource1);

      SchedulerNode mockNode2 = mock(SchedulerNode.class);
      when(mockNode1.getNodeID()).thenReturn(NodeId.newInstance("bar", 8081));
      when(mockNode2.getUnallocatedResource()).thenReturn(emptyResource);
      when(mockNode2.getTotalResource()).thenReturn(fullResource2);

      verifyMaximumResourceCapability(configuredMaximumResource, scheduler);

      scheduler.nodeTracker.addNode(mockNode1);
      verifyMaximumResourceCapability(fullResource1, scheduler);

      scheduler.nodeTracker.addNode(mockNode2);
      verifyMaximumResourceCapability(fullResource2, scheduler);

      scheduler.nodeTracker.removeNode(mockNode2.getNodeID());
      verifyMaximumResourceCapability(fullResource1, scheduler);

      scheduler.nodeTracker.removeNode(mockNode1.getNodeID());
      verifyMaximumResourceCapability(configuredMaximumResource, scheduler);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testMaxAllocationAfterUpdateNodeResource() throws IOException {
    final int configuredMaxVCores = 20;
    final int configuredMaxMemory = 10 * 1024;
    Resource configuredMaximumResource = Resource.newInstance
        (configuredMaxMemory, configuredMaxVCores);

    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        configuredMaxVCores);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        configuredMaxMemory);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);

    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      AbstractYarnScheduler scheduler = (AbstractYarnScheduler) rm
          .getResourceScheduler();
      verifyMaximumResourceCapability(configuredMaximumResource, scheduler);

      Resource resource1 = Resource.newInstance(2048, 5);
      Resource resource2 = Resource.newInstance(4096, 10);
      Resource resource3 = Resource.newInstance(512, 1);
      Resource resource4 = Resource.newInstance(1024, 2);

      RMNode node1 = MockNodes.newNodeInfo(
          0, resource1, 1, "127.0.0.2");
      scheduler.handle(new NodeAddedSchedulerEvent(node1));
      RMNode node2 = MockNodes.newNodeInfo(
          0, resource3, 2, "127.0.0.3");
      scheduler.handle(new NodeAddedSchedulerEvent(node2));
      verifyMaximumResourceCapability(resource1, scheduler);

      // increase node1 resource
      scheduler.updateNodeResource(node1, ResourceOption.newInstance(
          resource2, 0));
      verifyMaximumResourceCapability(resource2, scheduler);

      // decrease node1 resource
      scheduler.updateNodeResource(node1, ResourceOption.newInstance(
          resource1, 0));
      verifyMaximumResourceCapability(resource1, scheduler);

      // increase node2 resource
      scheduler.updateNodeResource(node2, ResourceOption.newInstance(
          resource4, 0));
      verifyMaximumResourceCapability(resource1, scheduler);

      // decrease node2 resource
      scheduler.updateNodeResource(node2, ResourceOption.newInstance(
          resource3, 0));
      verifyMaximumResourceCapability(resource1, scheduler);
    } finally {
      rm.stop();
    }
  }

  /*
   * This test case is to test the pending containers are cleared from the
   * attempt even if one of the application in the list have current attempt as
   * null (no attempt).
   */
  @SuppressWarnings({ "rawtypes" })
  @Test(timeout = 10000)
  public void testReleasedContainerIfAppAttemptisNull() throws Exception {
    YarnConfiguration conf=getConf();
    MockRM rm1 = new MockRM(conf);
    try {
      rm1.start();
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
      nm1.registerNode();

      AbstractYarnScheduler scheduler =
          (AbstractYarnScheduler) rm1.getResourceScheduler();
      // Mock App without attempt
      RMApp mockAPp =
          new MockRMApp(125, System.currentTimeMillis(), RMAppState.NEW);
      SchedulerApplication<FiCaSchedulerApp> application =
          new SchedulerApplication<FiCaSchedulerApp>(null, mockAPp.getUser());

      // Second app with one app attempt
      RMApp app = rm1.submitApp(200);
      MockAM am1 = MockRM.launchAndRegisterAM(app, rm1, nm1);
      final ContainerId runningContainer =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
      am1.allocate(null, Arrays.asList(runningContainer));

      Map schedulerApplications = scheduler.getSchedulerApplications();
      SchedulerApplication schedulerApp =
          (SchedulerApplication) scheduler.getSchedulerApplications().get(
              app.getApplicationId());
      schedulerApplications.put(mockAPp.getApplicationId(), application);

      scheduler.clearPendingContainerCache();

      Assert.assertEquals("Pending containers are not released "
          + "when one of the application attempt is null !", schedulerApp
          .getCurrentAppAttempt().getPendingRelease().size(), 0);
    } finally {
      if (rm1 != null) {
        rm1.stop();
      }
    }
  }

  @Test(timeout=60000)
  public void testContainerReleasedByNode() throws Exception {
    System.out.println("Starting testContainerReleasedByNode");
    YarnConfiguration conf = getConf();
    MockRM rm1 = new MockRM(conf);
    try {
      rm1.start();
      RMApp app1 =
          rm1.submitApp(200, "name", "user",
              new HashMap<ApplicationAccessType, String>(), false, "default",
              -1, null, "Test", false, true);
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
      nm1.registerNode();

      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

      // allocate a container that fills more than half the node
      am1.allocate("127.0.0.1", 8192, 1, new ArrayList<ContainerId>());
      nm1.nodeHeartbeat(true);

      // wait for containers to be allocated.
      List<Container> containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.isEmpty()) {
        Thread.sleep(10);
        nm1.nodeHeartbeat(true);
        containers = am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
      }

      // release the container from the AM
      ContainerId cid = containers.get(0).getId();
      List<ContainerId> releasedContainers = new ArrayList<>(1);
      releasedContainers.add(cid);
      List<ContainerStatus> completedContainers = am1.allocate(
          new ArrayList<ResourceRequest>(), releasedContainers)
          .getCompletedContainersStatuses();
      while (completedContainers.isEmpty()) {
        Thread.sleep(10);
        completedContainers = am1.allocate(
          new ArrayList<ResourceRequest>(), releasedContainers)
          .getCompletedContainersStatuses();
      }

      // verify new container can be allocated immediately because container
      // never launched on the node
      containers = am1.allocate("127.0.0.1", 8192, 1,
          new ArrayList<ContainerId>()).getAllocatedContainers();
      nm1.nodeHeartbeat(true);
      while (containers.isEmpty()) {
        Thread.sleep(10);
        nm1.nodeHeartbeat(true);
        containers = am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
      }

      // launch the container on the node
      cid = containers.get(0).getId();
      nm1.nodeHeartbeat(cid.getApplicationAttemptId(), cid.getContainerId(),
          ContainerState.RUNNING);
      rm1.waitForState(nm1, cid, RMContainerState.RUNNING);

      // release the container from the AM
      releasedContainers.clear();
      releasedContainers.add(cid);
      completedContainers = am1.allocate(
          new ArrayList<ResourceRequest>(), releasedContainers)
          .getCompletedContainersStatuses();
      while (completedContainers.isEmpty()) {
        Thread.sleep(10);
        completedContainers = am1.allocate(
          new ArrayList<ResourceRequest>(), releasedContainers)
          .getCompletedContainersStatuses();
      }

      // verify new container cannot be allocated immediately because container
      // has not been released by the node
      containers = am1.allocate("127.0.0.1", 8192, 1,
          new ArrayList<ContainerId>()).getAllocatedContainers();
      nm1.nodeHeartbeat(true);
      Assert.assertTrue("new container allocated before node freed old",
          containers.isEmpty());
      for (int i = 0; i < 10; ++i) {
        Thread.sleep(10);
        containers = am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
        nm1.nodeHeartbeat(true);
        Assert.assertTrue("new container allocated before node freed old",
            containers.isEmpty());
      }

      // free the old container from the node
      nm1.nodeHeartbeat(cid.getApplicationAttemptId(), cid.getContainerId(),
          ContainerState.COMPLETE);

      // verify new container is now allocated
      containers = am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.isEmpty()) {
        Thread.sleep(10);
        nm1.nodeHeartbeat(true);
        containers = am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
      }
    } finally {
      rm1.stop();
      System.out.println("Stopping testContainerReleasedByNode");
    }
  }

  @Test(timeout = 60000)
  public void testResourceRequestRestoreWhenRMContainerIsAtAllocated()
      throws Exception {
    YarnConfiguration conf = getConf();
    MockRM rm1 = new MockRM(conf);
    try {
      rm1.start();
      RMApp app1 =
          rm1.submitApp(200, "name", "user",
              new HashMap<ApplicationAccessType, String>(), false, "default",
              -1, null, "Test", false, true);
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
      nm1.registerNode();

      MockNM nm2 =
          new MockNM("127.0.0.1:2351", 10240, rm1.getResourceTrackerService());
      nm2.registerNode();

      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

      int NUM_CONTAINERS = 1;
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
      nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
      ContainerId containerId2 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
      rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

      // 3rd container is in Allocated state.
      am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
          new ArrayList<ContainerId>());
      nm2.nodeHeartbeat(true);
      ContainerId containerId3 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
      rm1.waitForState(nm2, containerId3, RMContainerState.ALLOCATED);

      // NodeManager restart
      nm2.registerNode();

      // NM restart kills all allocated and running containers.
      rm1.waitForState(nm2, containerId3, RMContainerState.KILLED);

      // The killed RMContainer request should be restored. In successive
      // nodeHeartBeats AM should be able to get container allocated.
      containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.size() != NUM_CONTAINERS) {
        nm2.nodeHeartbeat(true);
        containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers());
        Thread.sleep(200);
      }

      nm2.nodeHeartbeat(am1.getApplicationAttemptId(), 4,
          ContainerState.RUNNING);
      ContainerId containerId4 =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), 4);
      rm1.waitForState(nm2, containerId4, RMContainerState.RUNNING);
    } finally {
      rm1.stop();
    }
  }

  /**
   * Test to verify that ResourceRequests recovery back to the right app-attempt
   * after a container gets killed at ACQUIRED state: YARN-4502.
   *
   * @throws Exception
   */
  @Test
  public void testResourceRequestRecoveryToTheRightAppAttempt()
      throws Exception {

    YarnConfiguration conf = getConf();
    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      RMApp rmApp =
          rm.submitApp(200, "name", "user",
            new HashMap<ApplicationAccessType, String>(), false, "default", -1,
            null, "Test", false, true);
      MockNM node =
          new MockNM("127.0.0.1:1234", 10240, rm.getResourceTrackerService());
      node.registerNode();

      MockAM am1 = MockRM.launchAndRegisterAM(rmApp, rm, node);
      ApplicationAttemptId applicationAttemptOneID =
          am1.getApplicationAttemptId();
      ContainerId am1ContainerID =
          ContainerId.newContainerId(applicationAttemptOneID, 1);

      // allocate NUM_CONTAINERS containers
      am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
      node.nodeHeartbeat(true);

      // wait for containers to be allocated.
      List<Container> containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
      while (containers.size() != 1) {
        node.nodeHeartbeat(true);
        containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
        Thread.sleep(200);
      }

      // launch a 2nd container, for testing running-containers transfer.
      node.nodeHeartbeat(applicationAttemptOneID, 2, ContainerState.RUNNING);
      ContainerId runningContainerID =
          ContainerId.newContainerId(applicationAttemptOneID, 2);
      rm.waitForState(node, runningContainerID, RMContainerState.RUNNING);

      // 3rd container is in Allocated state.
      int ALLOCATED_CONTAINER_PRIORITY = 1047;
      am1.allocate("127.0.0.1", 1024, 1, ALLOCATED_CONTAINER_PRIORITY,
        new ArrayList<ContainerId>(), null);
      node.nodeHeartbeat(true);
      ContainerId allocatedContainerID =
          ContainerId.newContainerId(applicationAttemptOneID, 3);
      rm.waitForState(node, allocatedContainerID, RMContainerState.ALLOCATED);
      RMContainer allocatedContainer =
          rm.getResourceScheduler().getRMContainer(allocatedContainerID);

      // Capture scheduler app-attempt before AM crash.
      SchedulerApplicationAttempt firstSchedulerAppAttempt =
          ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) rm
            .getResourceScheduler())
            .getApplicationAttempt(applicationAttemptOneID);

      // AM crashes, and a new app-attempt gets created
      node.nodeHeartbeat(applicationAttemptOneID, 1, ContainerState.COMPLETE);
      rm.drainEvents();
      RMAppAttempt rmAppAttempt2 = MockRM.waitForAttemptScheduled(rmApp, rm);
      ApplicationAttemptId applicationAttemptTwoID =
          rmAppAttempt2.getAppAttemptId();
      Assert.assertEquals(2, applicationAttemptTwoID.getAttemptId());

      // All outstanding allocated containers will be killed (irrespective of
      // keep-alive of container across app-attempts)
      Assert.assertEquals(RMContainerState.KILLED,
        allocatedContainer.getState());

      // The core part of this test
      // The killed containers' ResourceRequests are recovered back to the
      // original app-attempt, not the new one
      for (SchedulerRequestKey key : firstSchedulerAppAttempt.getSchedulerKeys()) {
        if (key.getPriority().getPriority() == 0) {
          Assert.assertEquals(0,
              firstSchedulerAppAttempt.getOutstandingAsksCount(key));
        } else if (key.getPriority().getPriority() ==
            ALLOCATED_CONTAINER_PRIORITY) {
          Assert.assertEquals(1,
              firstSchedulerAppAttempt.getOutstandingAsksCount(key));
        }
      }

      // Also, only one running container should be transferred after AM
      // launches
      MockRM.launchAM(rmApp, rm, node);
      List<Container> transferredContainers =
          rm.getResourceScheduler().getTransferredContainers(
            applicationAttemptTwoID);
      Assert.assertEquals(1, transferredContainers.size());
      Assert.assertEquals(runningContainerID, transferredContainers.get(0)
        .getId());

    } finally {
      rm.stop();
    }
  }

  private void verifyMaximumResourceCapability(
      Resource expectedMaximumResource, YarnScheduler scheduler) {

    final Resource schedulerMaximumResourceCapability = scheduler
        .getMaximumResourceCapability();
    Assert.assertEquals(expectedMaximumResource.getMemorySize(),
        schedulerMaximumResourceCapability.getMemorySize());
    Assert.assertEquals(expectedMaximumResource.getVirtualCores(),
        schedulerMaximumResourceCapability.getVirtualCores());
  }

  private class SleepHandler implements EventHandler<SchedulerEvent> {
    boolean sleepFlag = false;
    int sleepTime = 20;
    @Override
    public void handle(SchedulerEvent event) {
      try {
        if (sleepFlag) {
          Thread.sleep(sleepTime);
        }
      } catch(InterruptedException ie) {
      }
    }
  }

  private ResourceTrackerService getPrivateResourceTrackerService(
      Dispatcher privateDispatcher, ResourceManager rm,
      SleepHandler sleepHandler) {
    Configuration conf = getConf();

    RMContext privateContext =
        new RMContextImpl(privateDispatcher, null, null, null, null, null, null,
            null, null, null);
    privateContext.setNodeLabelManager(Mockito.mock(RMNodeLabelsManager.class));

    privateDispatcher.register(SchedulerEventType.class, sleepHandler);
    privateDispatcher.register(SchedulerEventType.class,
        rm.getResourceScheduler());
    privateDispatcher.register(RMNodeEventType.class,
        new ResourceManager.NodeEventDispatcher(privateContext));
    ((Service) privateDispatcher).init(conf);
    ((Service) privateDispatcher).start();
    NMLivelinessMonitor nmLivelinessMonitor =
        new NMLivelinessMonitor(privateDispatcher);
    nmLivelinessMonitor.init(conf);
    nmLivelinessMonitor.start();
    NodesListManager nodesListManager = new NodesListManager(privateContext);
    nodesListManager.init(conf);
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.start();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        new NMTokenSecretManagerInRM(conf);
    nmTokenSecretManager.start();
    ResourceTrackerService privateResourceTrackerService =
        new ResourceTrackerService(privateContext, nodesListManager,
            nmLivelinessMonitor, containerTokenSecretManager,
            nmTokenSecretManager);
    privateResourceTrackerService.init(conf);
    privateResourceTrackerService.start();
    rm.getResourceScheduler().setRMContext(privateContext);
    return privateResourceTrackerService;
  }

  /**
   * Test the behavior of the scheduler when a node reconnects
   * with changed capabilities. This test is to catch any race conditions
   * that might occur due to the use of the RMNode object.
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testNodemanagerReconnect() throws Exception {
    Configuration conf = getConf();
    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      DrainDispatcher privateDispatcher = new DrainDispatcher();
      privateDispatcher.disableExitOnDispatchException();
      SleepHandler sleepHandler = new SleepHandler();
      ResourceTrackerService privateResourceTrackerService =
          getPrivateResourceTrackerService(privateDispatcher, rm, sleepHandler);

      // Register node1
      String hostname1 = "localhost1";
      Resource capability = BuilderUtils.newResource(4096, 4);
      RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);

      RegisterNodeManagerRequest request1 =
          recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
      NodeId nodeId1 = NodeId.newInstance(hostname1, 0);
      request1.setNodeId(nodeId1);
      request1.setHttpPort(0);
      request1.setResource(capability);
      privateResourceTrackerService.registerNodeManager(request1);
      privateDispatcher.await();
      Resource clusterResource =
          rm.getResourceScheduler().getClusterResource();
      Assert.assertEquals("Initial cluster resources don't match", capability,
          clusterResource);

      Resource newCapability = BuilderUtils.newResource(1024, 1);
      RegisterNodeManagerRequest request2 =
          recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
      request2.setNodeId(nodeId1);
      request2.setHttpPort(0);
      request2.setResource(newCapability);
      // hold up the disaptcher and register the same node with lower capability
      sleepHandler.sleepFlag = true;
      privateResourceTrackerService.registerNodeManager(request2);
      privateDispatcher.await();
      Assert.assertEquals("Cluster resources don't match", newCapability,
          rm.getResourceScheduler().getClusterResource());
      privateResourceTrackerService.stop();
    } finally {
      rm.stop();
    }
  }

  @Test(timeout = 10000)
  public void testUpdateThreadLifeCycle() throws Exception {
    MockRM rm = new MockRM(getConf());
    try {
      rm.start();
      AbstractYarnScheduler scheduler =
          (AbstractYarnScheduler) rm.getResourceScheduler();

      if (getSchedulerType().equals(SchedulerType.FAIR)) {
        Thread updateThread = scheduler.updateThread;
        Assert.assertTrue(updateThread.isAlive());
        scheduler.stop();

        int numRetries = 100;
        while (numRetries-- > 0 && updateThread.isAlive()) {
          Thread.sleep(50);
        }

        Assert.assertNotEquals("The Update thread is still alive", 0, numRetries);
      } else if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
        Assert.assertNull("updateThread shouldn't have been created",
            scheduler.updateThread);
      } else {
        Assert.fail("Unhandled SchedulerType, " + getSchedulerType() +
            ", please update this unit test.");
      }
    } finally {
      rm.stop();
    }
  }
}
