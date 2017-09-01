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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistributedSchedulingAllocateRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistributedSchedulingAllocateResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterDistributedSchedulingAMResponsePBImpl;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo
    .FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Test cases for {@link OpportunisticContainerAllocatorAMService}.
 */
public class TestOpportunisticContainerAllocatorAMService {

  private static final int GB = 1024;

  private MockRM rm;
  private DrainDispatcher dispatcher;

  @Before
  public void createAndStartRM() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(
        YarnConfiguration.NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS, 100);
    startRM(conf);
  }

  public void createAndStartRMWithAutoUpdateContainer() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setBoolean(YarnConfiguration.RM_AUTO_UPDATE_CONTAINERS, true);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(
        YarnConfiguration.NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS, 100);
    startRM(conf);
  }

  private void startRM(final YarnConfiguration conf) {
    dispatcher = new DrainDispatcher();
    rm = new MockRM(conf) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
  }

  @After
  public void stopRM() {
    if (rm != null) {
      rm.stop();
    }
  }

  @Test(timeout = 600000)
  public void testContainerPromoteAndDemoteBeforeContainerStart() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h1:4321", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h2:4321", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm.getRMContext().getRMNodes().get(nm2.getNodeId());
    RMNode rmNode3 = rm.getRMContext().getRMNodes().get(nm3.getNodeId());
    RMNode rmNode4 = rm.getRMContext().getRMNodes().get(nm4.getNodeId());

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    nm4.nodeHeartbeat(true);

    // Send add and update node events to AM Service.
    amservice.handle(new NodeAddedSchedulerEvent(rmNode1));
    amservice.handle(new NodeAddedSchedulerEvent(rmNode2));
    amservice.handle(new NodeAddedSchedulerEvent(rmNode3));
    amservice.handle(new NodeAddedSchedulerEvent(rmNode4));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode1));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode2));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode3));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode4));
    // All nodes 1 - 4 will be applicable for scheduling.
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    nm4.nodeHeartbeat(true);

    Thread.sleep(1000);

    QueueMetrics metrics = ((CapacityScheduler) scheduler).getRootQueue()
        .getMetrics();

    // Verify Metrics
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);

    AllocateResponse allocateResponse = am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))),
        null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    Assert.assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());
    MockNM sameHostDiffNode = null;
    for (NodeId n : nodes.keySet()) {
      if (n.getHost().equals(allocNode.getNodeId().getHost()) &&
          n.getPort() != allocNode.getNodeId().getPort()) {
        sameHostDiffNode = nodes.get(n);
      }
    }

    // Verify Metrics After OPP allocation (Nothing should change)
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);

    am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));
    // Node on same host should not result in allocation
    sameHostDiffNode.nodeHeartbeat(true);
    Thread.sleep(200);
    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    Assert.assertEquals(0, allocateResponse.getUpdatedContainers().size());

    // Wait for scheduler to process all events
    dispatcher.waitForEventThreadToWait();
    Thread.sleep(1000);
    // Verify Metrics After OPP allocation (Nothing should change again)
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);

    // Send Promotion req again... this should result in update error
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));
    Assert.assertEquals(0, allocateResponse.getUpdatedContainers().size());
    Assert.assertEquals(1, allocateResponse.getUpdateErrors().size());
    Assert.assertEquals("UPDATE_OUTSTANDING_ERROR",
        allocateResponse.getUpdateErrors().get(0).getReason());
    Assert.assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Send Promotion req again with incorrect version...
    // this should also result in update error
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(1,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));

    Assert.assertEquals(0, allocateResponse.getUpdatedContainers().size());
    Assert.assertEquals(1, allocateResponse.getUpdateErrors().size());
    Assert.assertEquals("INCORRECT_CONTAINER_VERSION_ERROR",
        allocateResponse.getUpdateErrors().get(0).getReason());
    Assert.assertEquals(0,
        allocateResponse.getUpdateErrors().get(0)
            .getCurrentContainerVersion());
    Assert.assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Ensure after correct node heartbeats, we should get the allocation
    allocNode.nodeHeartbeat(true);
    Thread.sleep(200);
    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    Assert.assertEquals(1, allocateResponse.getUpdatedContainers().size());
    Container uc =
        allocateResponse.getUpdatedContainers().get(0).getContainer();
    Assert.assertEquals(ExecutionType.GUARANTEED, uc.getExecutionType());
    Assert.assertEquals(uc.getId(), container.getId());
    Assert.assertEquals(uc.getVersion(), container.getVersion() + 1);

    // Verify Metrics After OPP allocation :
    // Allocated cores+mem should have increased, available should decrease
    verifyMetrics(metrics, 14336, 14, 2048, 2, 2);

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    nm4.nodeHeartbeat(true);
    Thread.sleep(200);

    // Verify that the container is still in ACQUIRED state wrt the RM.
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
        uc.getId().getApplicationAttemptId()).getRMContainer(uc.getId());
    Assert.assertEquals(RMContainerState.ACQUIRED, rmContainer.getState());

    // Now demote the container back..
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(uc.getVersion(),
            uc.getId(), ContainerUpdateType.DEMOTE_EXECUTION_TYPE,
            null, ExecutionType.OPPORTUNISTIC)));
    // This should happen in the same heartbeat..
    Assert.assertEquals(1, allocateResponse.getUpdatedContainers().size());
    uc = allocateResponse.getUpdatedContainers().get(0).getContainer();
    Assert.assertEquals(ExecutionType.OPPORTUNISTIC, uc.getExecutionType());
    Assert.assertEquals(uc.getId(), container.getId());
    Assert.assertEquals(uc.getVersion(), container.getVersion() + 2);

    // Wait for scheduler to finish processing events
    dispatcher.waitForEventThreadToWait();
    // Verify Metrics After OPP allocation :
    // Everything should have reverted to what it was
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);
  }

  @Test(timeout = 60000)
  public void testContainerPromoteAfterContainerStart() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    nm1.registerNode();
    nm2.registerNode();

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm.getRMContext().getRMNodes().get(nm2.getNodeId());

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    ((RMNodeImpl) rmNode1)
        .setOpportunisticContainersStatus(getOppurtunisticStatus(-1, 100));
    ((RMNodeImpl) rmNode2)
        .setOpportunisticContainersStatus(getOppurtunisticStatus(-1, 100));

    OpportunisticContainerContext ctxt = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(attemptId).getOpportunisticContainerContext();
    // Send add and update node events to AM Service.
    amservice.handle(new NodeAddedSchedulerEvent(rmNode1));
    amservice.handle(new NodeAddedSchedulerEvent(rmNode2));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode1));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode2));

    // All nodes 1 to 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    Thread.sleep(1000);

    QueueMetrics metrics = ((CapacityScheduler) scheduler).getRootQueue()
        .getMetrics();

    // Verify Metrics
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    AllocateResponse allocateResponse = am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))),
        null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    Assert.assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.RUNNING, "", 0)),
        true);
    Thread.sleep(200);

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    Assert.assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Verify Metrics After OPP allocation (Nothing should change)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));

    // Verify Metrics After OPP allocation (Nothing should change again)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    // Send Promotion req again... this should result in update error
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));
    Assert.assertEquals(0, allocateResponse.getUpdatedContainers().size());
    Assert.assertEquals(1, allocateResponse.getUpdateErrors().size());
    Assert.assertEquals("UPDATE_OUTSTANDING_ERROR",
        allocateResponse.getUpdateErrors().get(0).getReason());
    Assert.assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.RUNNING, "", 0)),
        true);
    Thread.sleep(200);

    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    Assert.assertEquals(1, allocateResponse.getUpdatedContainers().size());
    Container uc =
        allocateResponse.getUpdatedContainers().get(0).getContainer();
    Assert.assertEquals(ExecutionType.GUARANTEED, uc.getExecutionType());
    Assert.assertEquals(uc.getId(), container.getId());
    Assert.assertEquals(uc.getVersion(), container.getVersion() + 1);

    // Verify that the Container is still in RUNNING state wrt RM..
    rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            uc.getId().getApplicationAttemptId()).getRMContainer(uc.getId());
    Assert.assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Verify Metrics After OPP allocation :
    // Allocated cores+mem should have increased, available should decrease
    verifyMetrics(metrics, 6144, 6, 2048, 2, 2);
  }

  @Test(timeout = 600000)
  public void testContainerPromoteAfterContainerComplete() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    nm1.registerNode();
    nm2.registerNode();

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm.getRMContext().getRMNodes().get(nm2.getNodeId());

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    ((RMNodeImpl) rmNode1)
        .setOpportunisticContainersStatus(getOppurtunisticStatus(-1, 100));
    ((RMNodeImpl) rmNode2)
        .setOpportunisticContainersStatus(getOppurtunisticStatus(-1, 100));

    OpportunisticContainerContext ctxt = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(attemptId).getOpportunisticContainerContext();
    // Send add and update node events to AM Service.
    amservice.handle(new NodeAddedSchedulerEvent(rmNode1));
    amservice.handle(new NodeAddedSchedulerEvent(rmNode2));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode1));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode2));

    // All nodes 1 to 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    Thread.sleep(1000);

    QueueMetrics metrics = ((CapacityScheduler) scheduler).getRootQueue()
        .getMetrics();

    // Verify Metrics
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    AllocateResponse allocateResponse = am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))),
        null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    Assert.assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.RUNNING, "", 0)),
        true);
    Thread.sleep(200);

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    Assert.assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Container Completed in the NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.COMPLETE, "", 0)),
        true);
    Thread.sleep(200);

    // Verify that container has been removed..
    rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    Assert.assertNull(rmContainer);

    // Verify Metrics After OPP allocation (Nothing should change)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    // Send Promotion req... this should result in update error
    // Since the container doesn't exist anymore..
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));

    Assert.assertEquals(1,
        allocateResponse.getCompletedContainersStatuses().size());
    Assert.assertEquals(container.getId(),
        allocateResponse.getCompletedContainersStatuses().get(0)
            .getContainerId());
    Assert.assertEquals(0, allocateResponse.getUpdatedContainers().size());
    Assert.assertEquals(1, allocateResponse.getUpdateErrors().size());
    Assert.assertEquals("INVALID_CONTAINER_ID",
        allocateResponse.getUpdateErrors().get(0).getReason());
    Assert.assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Verify Metrics After OPP allocation (Nothing should change again)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);
  }

  @Test(timeout = 600000)
  public void testContainerAutoUpdateContainer() throws Exception {
    rm.stop();
    createAndStartRMWithAutoUpdateContainer();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nm1.registerNode();

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());

    nm1.nodeHeartbeat(true);

    ((RMNodeImpl) rmNode1)
        .setOpportunisticContainersStatus(getOppurtunisticStatus(-1, 100));

    OpportunisticContainerContext ctxt =
        ((CapacityScheduler) scheduler).getApplicationAttempt(attemptId)
            .getOpportunisticContainerContext();
    // Send add and update node events to AM Service.
    amservice.handle(new NodeAddedSchedulerEvent(rmNode1));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode1));

    nm1.nodeHeartbeat(true);
    Thread.sleep(1000);

    AllocateResponse allocateResponse = am1.allocate(Arrays.asList(
        ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest
                .newInstance(ExecutionType.OPPORTUNISTIC, true))), null);
    List<Container> allocatedContainers =
        allocateResponse.getAllocatedContainers();
    Assert.assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    // Start Container in NM
    nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.OPPORTUNISTIC,
            ContainerState.RUNNING, "", 0)), true);
    Thread.sleep(200);

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(container.getId().getApplicationAttemptId())
        .getRMContainer(container.getId());
    Assert.assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Send Promotion req... this should result in update error
    // Since the container doesn't exist anymore..
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(0, container.getId(),
            ContainerUpdateType.PROMOTE_EXECUTION_TYPE, null,
            ExecutionType.GUARANTEED)));

    nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.OPPORTUNISTIC,
            ContainerState.RUNNING, "", 0)), true);
    Thread.sleep(200);
    // Get the update response on next allocate
    allocateResponse = am1.allocate(new ArrayList<>(), new ArrayList<>());
    // Check the update response from YARNRM
    Assert.assertEquals(1, allocateResponse.getUpdatedContainers().size());
    UpdatedContainer uc = allocateResponse.getUpdatedContainers().get(0);
    Assert.assertEquals(container.getId(), uc.getContainer().getId());
    Assert.assertEquals(ExecutionType.GUARANTEED,
        uc.getContainer().getExecutionType());
    // Check that the container is updated in NM through NM heartbeat response
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);
    Assert.assertEquals(1, response.getContainersToUpdate().size());
    Container containersFromNM = response.getContainersToUpdate().get(0);
    Assert.assertEquals(container.getId(), containersFromNM.getId());
    Assert.assertEquals(ExecutionType.GUARANTEED,
        containersFromNM.getExecutionType());

    //Increase resources
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(1, container.getId(),
            ContainerUpdateType.INCREASE_RESOURCE,
            Resources.createResource(2 * GB, 1), null)));
    response = nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.GUARANTEED,
            ContainerState.RUNNING, "", 0)), true);

    Thread.sleep(200);
    if (allocateResponse.getUpdatedContainers().size() == 0) {
      allocateResponse = am1.allocate(new ArrayList<>(), new ArrayList<>());
    }
    Assert.assertEquals(1, allocateResponse.getUpdatedContainers().size());
    uc = allocateResponse.getUpdatedContainers().get(0);
    Assert.assertEquals(container.getId(), uc.getContainer().getId());
    Assert.assertEquals(Resource.newInstance(2 * GB, 1),
        uc.getContainer().getResource());
    Thread.sleep(1000);

    // Check that the container resources are increased in
    // NM through NM heartbeat response
    if (response.getContainersToUpdate().size() == 0) {
      response = nm1.nodeHeartbeat(true);
    }
    Assert.assertEquals(1, response.getContainersToUpdate().size());
    Assert.assertEquals(Resource.newInstance(2 * GB, 1),
        response.getContainersToUpdate().get(0).getResource());

    //Decrease resources
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(2, container.getId(),
            ContainerUpdateType.DECREASE_RESOURCE,
            Resources.createResource(1 * GB, 1), null)));
    Assert.assertEquals(1, allocateResponse.getUpdatedContainers().size());
    Thread.sleep(1000);

    // Check that the container resources are decreased
    // in NM through NM heartbeat response
    response = nm1.nodeHeartbeat(true);
    Assert.assertEquals(1, response.getContainersToUpdate().size());
    Assert.assertEquals(Resource.newInstance(1 * GB, 1),
        response.getContainersToUpdate().get(0).getResource());

    nm1.nodeHeartbeat(true);
    // DEMOTE the container
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(3, container.getId(),
            ContainerUpdateType.DEMOTE_EXECUTION_TYPE, null,
            ExecutionType.OPPORTUNISTIC)));

    response = nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.GUARANTEED,
            ContainerState.RUNNING, "", 0)), true);
    Thread.sleep(200);
    if (allocateResponse.getUpdatedContainers().size() == 0) {
      // Get the update response on next allocate
      allocateResponse = am1.allocate(new ArrayList<>(), new ArrayList<>());
    }
    // Check the update response from YARNRM
    Assert.assertEquals(1, allocateResponse.getUpdatedContainers().size());
    uc = allocateResponse.getUpdatedContainers().get(0);
    Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
        uc.getContainer().getExecutionType());
    // Check that the container is updated in NM through NM heartbeat response
    if (response.getContainersToUpdate().size() == 0) {
      response = nm1.nodeHeartbeat(true);
    }
    Assert.assertEquals(1, response.getContainersToUpdate().size());
    Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
        response.getContainersToUpdate().get(0).getExecutionType());
  }

  private void verifyMetrics(QueueMetrics metrics, long availableMB,
      int availableVirtualCores, long allocatedMB,
      int allocatedVirtualCores, int allocatedContainers) {
    Assert.assertEquals(availableMB, metrics.getAvailableMB());
    Assert.assertEquals(availableVirtualCores, metrics.getAvailableVirtualCores());
    Assert.assertEquals(allocatedMB, metrics.getAllocatedMB());
    Assert.assertEquals(allocatedVirtualCores, metrics.getAllocatedVirtualCores());
    Assert.assertEquals(allocatedContainers, metrics.getAllocatedContainers());
  }

  @Test(timeout = 60000)
  public void testNodeRemovalDuringAllocate() throws Exception {
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode();
    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm.getRMContext().getRMNodes().get(nm2.getNodeId());
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    ((RMNodeImpl) rmNode1)
        .setOpportunisticContainersStatus(getOppurtunisticStatus(-1, 100));
    ((RMNodeImpl) rmNode2)
        .setOpportunisticContainersStatus(getOppurtunisticStatus(-1, 100));
    OpportunisticContainerContext ctxt = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(attemptId).getOpportunisticContainerContext();
    // Send add and update node events to AM Service.
    amservice.handle(new NodeAddedSchedulerEvent(rmNode1));
    amservice.handle(new NodeAddedSchedulerEvent(rmNode2));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode1));
    amservice.handle(new NodeUpdateSchedulerEvent(rmNode2));
    // Both node 1 and node 2 will be applicable for scheduling.
    for (int i = 0; i < 10; i++) {
      am1.allocate(
          Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
              "*", Resources.createResource(1 * GB), 2)),
          null);
      if (ctxt.getNodeMap().size() == 2) {
        break;
      }
      Thread.sleep(50);
    }
    Assert.assertEquals(2, ctxt.getNodeMap().size());
    // Remove node from scheduler but not from AM Service.
    scheduler.handle(new NodeRemovedSchedulerEvent(rmNode1));
    // After removal of node 1, only 1 node will be applicable for scheduling.
    for (int i = 0; i < 10; i++) {
      try {
        am1.allocate(
            Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
                "*", Resources.createResource(1 * GB), 2)),
            null);
      } catch (Exception e) {
        Assert.fail("Allocate request should be handled on node removal");
      }
      if (ctxt.getNodeMap().size() == 1) {
        break;
      }
      Thread.sleep(50);
    }
    Assert.assertEquals(1, ctxt.getNodeMap().size());
  }

  private OpportunisticContainersStatus getOppurtunisticStatus(int waitTime,
      int queueLength) {
    OpportunisticContainersStatus status1 =
        Mockito.mock(OpportunisticContainersStatus.class);
    Mockito.when(status1.getEstimatedQueueWaitTime()).thenReturn(waitTime);
    Mockito.when(status1.getWaitQueueLength()).thenReturn(queueLength);
    return status1;
  }

  // Test if the OpportunisticContainerAllocatorAMService can handle both
  // DSProtocol as well as AMProtocol clients
  @Test
  public void testRPCWrapping() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, HadoopYarnProtoRPC.class
        .getName());
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    conf.setSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS, addr);
    final RecordFactory factory = RecordFactoryProvider.getRecordFactory(null);
    final RMContext rmContext = new RMContextImpl() {
      @Override
      public AMLivelinessMonitor getAMLivelinessMonitor() {
        return null;
      }

      @Override
      public Configuration getYarnConfiguration() {
        return new YarnConfiguration();
      }

      @Override
      public RMContainerTokenSecretManager getContainerTokenSecretManager() {
        return new RMContainerTokenSecretManager(conf);
      }

      @Override
      public ResourceScheduler getScheduler() {
        return new FifoScheduler();
      }
    };
    Container c = factory.newRecordInstance(Container.class);
    c.setExecutionType(ExecutionType.OPPORTUNISTIC);
    c.setId(
        ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(
                ApplicationId.newInstance(12345, 1), 2), 3));
    AllocateRequest allReq =
        (AllocateRequestPBImpl)factory.newRecordInstance(AllocateRequest.class);
    allReq.setAskList(Arrays.asList(
        ResourceRequest.newInstance(Priority.UNDEFINED, "a",
            Resource.newInstance(1, 2), 1, true, "exp",
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))));
    OpportunisticContainerAllocatorAMService service =
        createService(factory, rmContext, c);
    conf.setBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED, true);
    Server server = service.getServer(rpc, conf, addr, null);
    server.start();

    // Verify that the OpportunisticContainerAllocatorAMSercvice can handle
    // vanilla ApplicationMasterProtocol clients
    RPC.setProtocolEngine(conf, ApplicationMasterProtocolPB.class,
        ProtobufRpcEngine.class);
    ApplicationMasterProtocolPB ampProxy =
        RPC.getProxy(ApplicationMasterProtocolPB
            .class, 1, NetUtils.getConnectAddress(server), conf);
    RegisterApplicationMasterResponse regResp =
        new RegisterApplicationMasterResponsePBImpl(
            ampProxy.registerApplicationMaster(null,
                ((RegisterApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(
                        RegisterApplicationMasterRequest.class)).getProto()));
    Assert.assertEquals("dummyQueue", regResp.getQueue());
    FinishApplicationMasterResponse finishResp =
        new FinishApplicationMasterResponsePBImpl(
            ampProxy.finishApplicationMaster(null,
                ((FinishApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(
                        FinishApplicationMasterRequest.class)).getProto()
            ));
    Assert.assertEquals(false, finishResp.getIsUnregistered());
    AllocateResponse allocResp =
        new AllocateResponsePBImpl(
            ampProxy.allocate(null,
                ((AllocateRequestPBImpl)factory
                    .newRecordInstance(AllocateRequest.class)).getProto())
        );
    List<Container> allocatedContainers = allocResp.getAllocatedContainers();
    Assert.assertEquals(1, allocatedContainers.size());
    Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
        allocatedContainers.get(0).getExecutionType());
    Assert.assertEquals(12345, allocResp.getNumClusterNodes());


    // Verify that the DistrubutedSchedulingService can handle the
    // DistributedSchedulingAMProtocol clients as well
    RPC.setProtocolEngine(conf, DistributedSchedulingAMProtocolPB.class,
        ProtobufRpcEngine.class);
    DistributedSchedulingAMProtocolPB dsProxy =
        RPC.getProxy(DistributedSchedulingAMProtocolPB
            .class, 1, NetUtils.getConnectAddress(server), conf);

    RegisterDistributedSchedulingAMResponse dsRegResp =
        new RegisterDistributedSchedulingAMResponsePBImpl(
            dsProxy.registerApplicationMasterForDistributedScheduling(null,
                ((RegisterApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(RegisterApplicationMasterRequest.class))
                    .getProto()));
    Assert.assertEquals(54321l, dsRegResp.getContainerIdStart());
    Assert.assertEquals(4,
        dsRegResp.getMaxContainerResource().getVirtualCores());
    Assert.assertEquals(1024,
        dsRegResp.getMinContainerResource().getMemorySize());
    Assert.assertEquals(2,
        dsRegResp.getIncrContainerResource().getVirtualCores());

    DistributedSchedulingAllocateRequestPBImpl distAllReq =
        (DistributedSchedulingAllocateRequestPBImpl)factory.newRecordInstance(
            DistributedSchedulingAllocateRequest.class);
    distAllReq.setAllocateRequest(allReq);
    distAllReq.setAllocatedContainers(Arrays.asList(c));
    DistributedSchedulingAllocateResponse dsAllocResp =
        new DistributedSchedulingAllocateResponsePBImpl(
            dsProxy.allocateForDistributedScheduling(null,
                distAllReq.getProto()));
    Assert.assertEquals(
        "h1", dsAllocResp.getNodesForScheduling().get(0).getNodeId().getHost());

    FinishApplicationMasterResponse dsfinishResp =
        new FinishApplicationMasterResponsePBImpl(
            dsProxy.finishApplicationMaster(null,
                ((FinishApplicationMasterRequestPBImpl) factory
                    .newRecordInstance(FinishApplicationMasterRequest.class))
                    .getProto()));
    Assert.assertEquals(
        false, dsfinishResp.getIsUnregistered());
  }

  private OpportunisticContainerAllocatorAMService createService(
      final RecordFactory factory, final RMContext rmContext,
      final Container c) {
    return new OpportunisticContainerAllocatorAMService(rmContext, null) {
      @Override
      public RegisterApplicationMasterResponse registerApplicationMaster(
          RegisterApplicationMasterRequest request) throws
          YarnException, IOException {
        RegisterApplicationMasterResponse resp = factory.newRecordInstance(
            RegisterApplicationMasterResponse.class);
        // Dummy Entry to Assert that we get this object back
        resp.setQueue("dummyQueue");
        return resp;
      }

      @Override
      public FinishApplicationMasterResponse finishApplicationMaster(
          FinishApplicationMasterRequest request) throws YarnException,
          IOException {
        FinishApplicationMasterResponse resp = factory.newRecordInstance(
            FinishApplicationMasterResponse.class);
        // Dummy Entry to Assert that we get this object back
        resp.setIsUnregistered(false);
        return resp;
      }

      @Override
      public AllocateResponse allocate(AllocateRequest request) throws
          YarnException, IOException {
        AllocateResponse response = factory.newRecordInstance(
            AllocateResponse.class);
        response.setNumClusterNodes(12345);
        response.setAllocatedContainers(Arrays.asList(c));
        return response;
      }

      @Override
      public RegisterDistributedSchedulingAMResponse
          registerApplicationMasterForDistributedScheduling(
          RegisterApplicationMasterRequest request)
          throws YarnException, IOException {
        RegisterDistributedSchedulingAMResponse resp = factory
            .newRecordInstance(RegisterDistributedSchedulingAMResponse.class);
        resp.setContainerIdStart(54321L);
        resp.setMaxContainerResource(Resource.newInstance(4096, 4));
        resp.setMinContainerResource(Resource.newInstance(1024, 1));
        resp.setIncrContainerResource(Resource.newInstance(2048, 2));
        return resp;
      }

      @Override
      public DistributedSchedulingAllocateResponse
          allocateForDistributedScheduling(
          DistributedSchedulingAllocateRequest request)
          throws YarnException, IOException {
        List<ResourceRequest> askList =
            request.getAllocateRequest().getAskList();
        List<Container> allocatedContainers = request.getAllocatedContainers();
        Assert.assertEquals(1, allocatedContainers.size());
        Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
            allocatedContainers.get(0).getExecutionType());
        Assert.assertEquals(1, askList.size());
        Assert.assertTrue(askList.get(0)
            .getExecutionTypeRequest().getEnforceExecutionType());
        DistributedSchedulingAllocateResponse resp = factory
            .newRecordInstance(DistributedSchedulingAllocateResponse.class);
        resp.setNodesForScheduling(
            Arrays.asList(RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "http://h1:4321")));
        return resp;
      }
    };
  }
}
