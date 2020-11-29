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

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
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
      RMApp app = MockRMAppSubmitter.submitWithMemory(200, rm1);
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

  @Test(timeout = 30000l)
  public void testContainerReleaseWithAllocationTags() throws Exception {
    // Currently only can be tested against capacity scheduler.
    if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
      final String testTag1 = "some-tag";
      final String testTag2 = "some-other-tag";
      YarnConfiguration conf = getConf();
      conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER, "scheduler");
      MockRM rm1 = new MockRM(conf);
      rm1.start();
      MockNM nm1 = new MockNM("127.0.0.1:1234",
          10240, rm1.getResourceTrackerService());
      nm1.registerNode();
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
          .withAppName("name")
          .withUser("user")
          .withAcls(new HashMap<>())
          .withUnmanagedAM(false)
          .withQueue("default")
          .withMaxAppAttempts(-1)
          .withCredentials(null)
          .withAppType("Test")
          .withWaitForAppAcceptedState(false)
          .withKeepContainers(true)
          .build();
      RMApp app1 =
          MockRMAppSubmitter.submit(rm1, data);
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

      // allocate 1 container with tag1
      SchedulingRequest sr = SchedulingRequest
          .newInstance(1l, Priority.newInstance(1),
              ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
              Sets.newHashSet(testTag1),
              ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
              null);

      // allocate 3 containers with tag2
      SchedulingRequest sr1 = SchedulingRequest
          .newInstance(2l, Priority.newInstance(1),
              ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
              Sets.newHashSet(testTag2),
              ResourceSizing.newInstance(3, Resource.newInstance(1024, 1)),
              null);

      AllocateRequest ar = AllocateRequest.newBuilder()
          .schedulingRequests(Lists.newArrayList(sr, sr1)).build();
      am1.allocate(ar);
      nm1.nodeHeartbeat(true);

      List<Container> allocated = new ArrayList<>();
      while (allocated.size() < 4) {
        AllocateResponse rsp = am1
            .allocate(new ArrayList<>(), new ArrayList<>());
        allocated.addAll(rsp.getAllocatedContainers());
        nm1.nodeHeartbeat(true);
        Thread.sleep(1000);
      }

      Assert.assertEquals(4, allocated.size());

      Set<Container> containers = allocated.stream()
          .filter(container -> container.getAllocationRequestId() == 1l)
          .collect(Collectors.toSet());
      Assert.assertNotNull(containers);
      Assert.assertEquals(1, containers.size());
      ContainerId cid = containers.iterator().next().getId();

      // mock container start
      rm1.getRMContext().getScheduler()
          .getSchedulerNode(nm1.getNodeId()).containerStarted(cid);

      // verifies the allocation is made with correct number of tags
      Map<String, Long> nodeTags = rm1.getRMContext()
          .getAllocationTagsManager()
          .getAllocationTagsWithCount(nm1.getNodeId());
      Assert.assertNotNull(nodeTags.get(testTag1));
      Assert.assertEquals(1, nodeTags.get(testTag1).intValue());

      // release a container
      am1.allocate(new ArrayList<>(), Lists.newArrayList(cid));

      // before NM confirms, the tag should still exist
      nodeTags = rm1.getRMContext().getAllocationTagsManager()
          .getAllocationTagsWithCount(nm1.getNodeId());
      Assert.assertNotNull(nodeTags);
      Assert.assertNotNull(nodeTags.get(testTag1));
      Assert.assertEquals(1, nodeTags.get(testTag1).intValue());

      // NM reports back that container is released
      // RM should cleanup the tag
      ContainerStatus cs = ContainerStatus.newInstance(cid,
          ContainerState.COMPLETE, "", 0);
      nm1.nodeHeartbeat(Lists.newArrayList(cs), true);

      // Wait on condition
      // 1) tag1 doesn't exist anymore
      // 2) num of tag2 is still 3
      GenericTestUtils.waitFor(() -> {
        Map<String, Long> tags = rm1.getRMContext()
            .getAllocationTagsManager()
            .getAllocationTagsWithCount(nm1.getNodeId());
        return tags.get(testTag1) == null &&
            tags.get(testTag2).intValue() == 3;
      }, 500, 3000);
    }
  }


  @Test(timeout = 30000L)
  public void testNodeRemovedWithAllocationTags() throws Exception {
    // Currently only can be tested against capacity scheduler.
    if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
      final String testTag1 = "some-tag";
      YarnConfiguration conf = getConf();
      conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER, "scheduler");
      MockRM rm1 = new MockRM(conf);
      rm1.start();
      MockNM nm1 = new MockNM("127.0.0.1:1234",
          10240, rm1.getResourceTrackerService());
      nm1.registerNode();
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
              .withAppName("name")
              .withUser("user")
              .withAcls(new HashMap<>())
              .withUnmanagedAM(false)
              .withQueue("default")
              .withMaxAppAttempts(-1)
              .withCredentials(null)
              .withAppType("Test")
              .withWaitForAppAcceptedState(false)
              .withKeepContainers(true)
              .build();
      RMApp app1 =
          MockRMAppSubmitter.submit(rm1, data);
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

      // allocate 1 container with tag1
      SchedulingRequest sr = SchedulingRequest
          .newInstance(1L, Priority.newInstance(1),
              ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
              Sets.newHashSet(testTag1),
              ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
              null);

      AllocateRequest ar = AllocateRequest.newBuilder()
          .schedulingRequests(Lists.newArrayList(sr)).build();
      am1.allocate(ar);
      nm1.nodeHeartbeat(true);

      List<Container> allocated = new ArrayList<>();
      while (allocated.size() < 1) {
        AllocateResponse rsp = am1
            .allocate(new ArrayList<>(), new ArrayList<>());
        allocated.addAll(rsp.getAllocatedContainers());
        nm1.nodeHeartbeat(true);
        Thread.sleep(1000);
      }

      Assert.assertEquals(1, allocated.size());

      Set<Container> containers = allocated.stream()
          .filter(container -> container.getAllocationRequestId() == 1L)
          .collect(Collectors.toSet());
      Assert.assertNotNull(containers);
      Assert.assertEquals(1, containers.size());
      ContainerId cid = containers.iterator().next().getId();

      // mock container start
      rm1.getRMContext().getScheduler()
          .getSchedulerNode(nm1.getNodeId()).containerStarted(cid);

      // verifies the allocation is made with correct number of tags
      Map<String, Long> nodeTags = rm1.getRMContext()
          .getAllocationTagsManager()
          .getAllocationTagsWithCount(nm1.getNodeId());
      Assert.assertNotNull(nodeTags.get(testTag1));
      Assert.assertEquals(1, nodeTags.get(testTag1).intValue());

      // remove the  node
      RMNode node1 = MockNodes.newNodeInfo(
          0, Resources.createResource(nm1.getMemory()), 1, "127.0.0.1", 1234);
      rm1.getRMContext().getScheduler().handle(
          new NodeRemovedSchedulerEvent(node1));

      // Once the node is removed, the tag should be removed immediately
      nodeTags = rm1.getRMContext().getAllocationTagsManager()
          .getAllocationTagsWithCount(nm1.getNodeId());
      Assert.assertNull(nodeTags);
    }
  }


  @Test(timeout=60000)
  public void testContainerReleasedByNode() throws Exception {
    System.out.println("Starting testContainerReleasedByNode");
    YarnConfiguration conf = getConf();
    MockRM rm1 = new MockRM(conf);
    try {
      rm1.start();
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
          .withAppName("name")
          .withUser("user")
          .withAcls(new HashMap<ApplicationAccessType, String>())
          .withUnmanagedAM(false)
          .withQueue("default")
          .withMaxAppAttempts(-1)
          .withCredentials(null)
          .withAppType("Test")
          .withWaitForAppAcceptedState(false)
          .withKeepContainers(true)
          .build();
      RMApp app1 =
          MockRMAppSubmitter.submit(rm1, data);
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
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
          .withAppName("name")
          .withUser("user")
          .withAcls(new HashMap<ApplicationAccessType, String>())
          .withUnmanagedAM(false)
          .withQueue("default")
          .withMaxAppAttempts(-1)
          .withCredentials(null)
          .withAppType("Test")
          .withWaitForAppAcceptedState(false)
          .withKeepContainers(true)
          .build();
      RMApp app1 =
          MockRMAppSubmitter.submit(rm1, data);
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
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
          .withAppName("name")
          .withUser("user")
          .withAcls(new HashMap<ApplicationAccessType, String>())
          .withUnmanagedAM(false)
          .withQueue("default")
          .withMaxAppAttempts(-1)
          .withCredentials(null)
          .withAppType("Test")
          .withWaitForAppAcceptedState(false)
          .withKeepContainers(true)
          .build();
      RMApp rmApp =
          MockRMAppSubmitter.submit(rm, data);
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
      NodeStatus mockNodeStatus = createMockNodeStatus();

      request1.setNodeId(nodeId1);
      request1.setHttpPort(0);
      request1.setResource(capability);
      request1.setNodeStatus(mockNodeStatus);
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

  @Test(timeout=60000)
  public void testContainerRecoveredByNode() throws Exception {
    System.out.println("Starting testContainerRecoveredByNode");
    final int maxMemory = 10 * 1024;
    YarnConfiguration conf = getConf();
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.set(
        YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MockRM rm1 = new MockRM(conf);
    try {
      rm1.start();
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
          .withAppName("name")
          .withUser("user")
          .withAcls(new HashMap<ApplicationAccessType, String>())
          .withUnmanagedAM(false)
          .withQueue("default")
          .withMaxAppAttempts(-1)
          .withCredentials(null)
          .withAppType("Test")
          .withWaitForAppAcceptedState(false)
          .withKeepContainers(true)
          .build();
      RMApp app1 =
          MockRMAppSubmitter.submit(rm1, data);
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
      nm1.registerNode();
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
      am1.allocate("127.0.0.1", 8192, 1, new ArrayList<ContainerId>());

      YarnScheduler scheduler = rm1.getResourceScheduler();

      RMNode node1 = MockNodes.newNodeInfo(
          0, Resources.createResource(maxMemory), 1, "127.0.0.2");
      ContainerId containerId = ContainerId.newContainerId(
          app1.getCurrentAppAttempt().getAppAttemptId(), 2);
      NMContainerStatus containerReport =
          NMContainerStatus.newInstance(containerId, 0, ContainerState.RUNNING,
              Resource.newInstance(1024, 1), "recover container", 0,
              Priority.newInstance(0), 0);
      List<NMContainerStatus> containerReports = new ArrayList<>();
      containerReports.add(containerReport);
      scheduler.handle(new NodeAddedSchedulerEvent(node1, containerReports));
      RMContainer rmContainer = scheduler.getRMContainer(containerId);

      //verify queue name when rmContainer is recovered
      if (scheduler instanceof CapacityScheduler) {
        Assert.assertEquals(
            CapacitySchedulerConfiguration.ROOT + "." + app1.getQueue(),
            rmContainer.getQueueName());
      } else {
        Assert.assertEquals(app1.getQueue(), rmContainer.getQueueName());
      }

    } finally {
      rm1.stop();
      System.out.println("Stopping testContainerRecoveredByNode");
    }
  }

  /**
   * Test the order we get the containers to kill. It should respect the order
   * described in {@link SchedulerNode#getContainersToKill()}.
   */
  @Test
  public void testGetRunningContainersToKill() {
    final SchedulerNode node = new MockSchedulerNode();
    assertEquals(Collections.emptyList(), node.getContainersToKill());

    // AM0
    RMContainer am0 = newMockRMContainer(
        true, ExecutionType.GUARANTEED, "AM0");
    node.allocateContainer(am0);
    assertEquals(Arrays.asList(am0), node.getContainersToKill());

    // OPPORTUNISTIC0, AM0
    RMContainer opp0 = newMockRMContainer(
        false, ExecutionType.OPPORTUNISTIC, "OPPORTUNISTIC0");
    node.allocateContainer(opp0);
    assertEquals(Arrays.asList(opp0, am0), node.getContainersToKill());

    // OPPORTUNISTIC0, GUARANTEED0, AM0
    RMContainer regular0 = newMockRMContainer(
        false, ExecutionType.GUARANTEED, "GUARANTEED0");
    node.allocateContainer(regular0);
    assertEquals(Arrays.asList(opp0, regular0, am0),
        node.getContainersToKill());

    // OPPORTUNISTIC1, OPPORTUNISTIC0, GUARANTEED0, AM0
    RMContainer opp1 = newMockRMContainer(
        false, ExecutionType.OPPORTUNISTIC, "OPPORTUNISTIC1");
    node.allocateContainer(opp1);
    assertEquals(Arrays.asList(opp1, opp0, regular0, am0),
        node.getContainersToKill());

    // OPPORTUNISTIC1, OPPORTUNISTIC0, GUARANTEED0, AM1, AM0
    RMContainer am1 = newMockRMContainer(
        true, ExecutionType.GUARANTEED, "AM1");
    node.allocateContainer(am1);
    assertEquals(Arrays.asList(opp1, opp0, regular0, am1, am0),
        node.getContainersToKill());

    // OPPORTUNISTIC1, OPPORTUNISTIC0, GUARANTEED1, GUARANTEED0, AM1, AM0
    RMContainer regular1 = newMockRMContainer(
        false, ExecutionType.GUARANTEED, "GUARANTEED1");
    node.allocateContainer(regular1);
    assertEquals(Arrays.asList(opp1, opp0, regular1, regular0, am1, am0),
        node.getContainersToKill());
  }

  private static RMContainer newMockRMContainer(boolean isAMContainer,
      ExecutionType executionType, String name) {
    RMContainer container = mock(RMContainer.class);
    when(container.isAMContainer()).thenReturn(isAMContainer);
    when(container.getExecutionType()).thenReturn(executionType);
    when(container.getCreationTime()).thenReturn(Time.now());
    when(container.toString()).thenReturn(name);
    return container;
  }

  /**
   * SchedulerNode mock to test launching containers.
   */
  class MockSchedulerNode extends SchedulerNode {
    private final List<RMContainer> containers = new ArrayList<>();

    MockSchedulerNode() {
      super(MockNodes.newNodeInfo(0, Resource.newInstance(1, 1)), false);
    }

    @Override
    protected List<RMContainer> getLaunchedContainers() {
      return containers;
    }

    @Override
    public void allocateContainer(RMContainer rmContainer) {
      containers.add(rmContainer);
      // Shuffle for testing
      Collections.shuffle(containers);
    }

    @Override
    public void reserveResource(SchedulerApplicationAttempt attempt,
        SchedulerRequestKey schedulerKey, RMContainer container) {}

    @Override
    public void unreserveResource(SchedulerApplicationAttempt attempt) {}
  }
}
