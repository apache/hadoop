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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeReconnectEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestRMNodeTransitions {

  RMNodeImpl node;
  
  private RMContext rmContext;
  private YarnScheduler scheduler;

  private SchedulerEventType eventType;
  private List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
  
  private final class TestSchedulerEventDispatcher implements
  EventHandler<SchedulerEvent> {
    @Override
    public void handle(SchedulerEvent event) {
      scheduler.handle(event);
    }
  }

  private NodesListManagerEvent nodesListManagerEvent = null;
  
  private class TestNodeListManagerEventDispatcher implements
      EventHandler<NodesListManagerEvent> {
    
    @Override
    public void handle(NodesListManagerEvent event) {
      nodesListManagerEvent = event;
    }

  }

  @Before
  public void setUp() throws Exception {
    InlineDispatcher rmDispatcher = new InlineDispatcher();
    
    rmContext =
        new RMContextImpl(rmDispatcher, null, null, null,
            mock(DelegationTokenRenewer.class), null, null, null, null, null);
    NodesListManager nodesListManager = mock(NodesListManager.class);
    HostsFileReader reader = mock(HostsFileReader.class);
    when(nodesListManager.getHostsReader()).thenReturn(reader);
    ((RMContextImpl) rmContext).setNodesListManager(nodesListManager);
    scheduler = mock(YarnScheduler.class);
    doAnswer(
        new Answer<Void>() {

          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            final SchedulerEvent event = (SchedulerEvent)(invocation.getArguments()[0]);
            eventType = event.getType();
            if (eventType == SchedulerEventType.NODE_UPDATE) {
              List<UpdatedContainerInfo> lastestContainersInfoList = 
                  ((NodeUpdateSchedulerEvent)event).getRMNode().pullContainerUpdates();
              for(UpdatedContainerInfo lastestContainersInfo : lastestContainersInfoList) {
            	  completedContainers.addAll(lastestContainersInfo.getCompletedContainers()); 
              }
            }
            return null;
          }
        }
        ).when(scheduler).handle(any(SchedulerEvent.class));
    
    rmDispatcher.register(SchedulerEventType.class, 
        new TestSchedulerEventDispatcher());
    
    rmDispatcher.register(NodesListManagerEventType.class,
        new TestNodeListManagerEventDispatcher());

    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null, null);
    nodesListManagerEvent =  null;

  }
  
  @After
  public void tearDown() throws Exception {
  }
  
  private RMNodeStatusEvent getMockRMNodeStatusEvent() {
    NodeHeartbeatResponse response = mock(NodeHeartbeatResponse.class);

    NodeHealthStatus healthStatus = mock(NodeHealthStatus.class);
    Boolean yes = new Boolean(true);
    doReturn(yes).when(healthStatus).getIsNodeHealthy();
    
    RMNodeStatusEvent event = mock(RMNodeStatusEvent.class);
    doReturn(healthStatus).when(event).getNodeHealthStatus();
    doReturn(response).when(event).getLatestResponse();
    doReturn(RMNodeEventType.STATUS_UPDATE).when(event).getType();
    return event;
  }
  
  @Test (timeout = 5000)
  public void testExpiredContainer() {
    // Start the node
    node.handle(new RMNodeStartedEvent(null, null, null));
    verify(scheduler).handle(any(NodeAddedSchedulerEvent.class));
    
    // Expire a container
    ContainerId completedContainerId = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(0, 0), 0), 0);
    node.handle(new RMNodeCleanContainerEvent(null, completedContainerId));
    Assert.assertEquals(1, node.getContainersToCleanUp().size());
    
    // Now verify that scheduler isn't notified of an expired container
    // by checking number of 'completedContainers' it got in the previous event
    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent();
    ContainerStatus containerStatus = mock(ContainerStatus.class);
    doReturn(completedContainerId).when(containerStatus).getContainerId();
    doReturn(Collections.singletonList(containerStatus)).
        when(statusEvent).getContainers();
    node.handle(statusEvent);
    /* Expect the scheduler call handle function 2 times
     * 1. RMNode status from new to Running, handle the add_node event
     * 2. handle the node update event
     */
    verify(scheduler,times(2)).handle(any(NodeUpdateSchedulerEvent.class));     
  }
  
  @Test (timeout = 5000)
  public void testContainerUpdate() throws InterruptedException{
    //Start the node
    node.handle(new RMNodeStartedEvent(null, null, null));
    
    NodeId nodeId = BuilderUtils.newNodeId("localhost:1", 1);
    RMNodeImpl node2 = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null, null);
    node2.handle(new RMNodeStartedEvent(null, null, null));
    
    ContainerId completedContainerIdFromNode1 = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(0, 0), 0), 0);
    ContainerId completedContainerIdFromNode2_1 = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(1, 1), 1), 1);
    ContainerId completedContainerIdFromNode2_2 = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(1, 1), 1), 2);
 
    RMNodeStatusEvent statusEventFromNode1 = getMockRMNodeStatusEvent();
    RMNodeStatusEvent statusEventFromNode2_1 = getMockRMNodeStatusEvent();
    RMNodeStatusEvent statusEventFromNode2_2 = getMockRMNodeStatusEvent();
    
    ContainerStatus containerStatusFromNode1 = mock(ContainerStatus.class);
    ContainerStatus containerStatusFromNode2_1 = mock(ContainerStatus.class);
    ContainerStatus containerStatusFromNode2_2 = mock(ContainerStatus.class);

    doReturn(completedContainerIdFromNode1).when(containerStatusFromNode1)
        .getContainerId();
    doReturn(Collections.singletonList(containerStatusFromNode1))
        .when(statusEventFromNode1).getContainers();
    node.handle(statusEventFromNode1);
    Assert.assertEquals(1, completedContainers.size());
    Assert.assertEquals(completedContainerIdFromNode1,
        completedContainers.get(0).getContainerId());

    completedContainers.clear();

    doReturn(completedContainerIdFromNode2_1).when(containerStatusFromNode2_1)
        .getContainerId();
    doReturn(Collections.singletonList(containerStatusFromNode2_1))
        .when(statusEventFromNode2_1).getContainers();

    doReturn(completedContainerIdFromNode2_2).when(containerStatusFromNode2_2)
        .getContainerId();
    doReturn(Collections.singletonList(containerStatusFromNode2_2))
        .when(statusEventFromNode2_2).getContainers();

    node2.setNextHeartBeat(false);
    node2.handle(statusEventFromNode2_1);
    node2.setNextHeartBeat(true);
    node2.handle(statusEventFromNode2_2);

    Assert.assertEquals(2, completedContainers.size());
    Assert.assertEquals(completedContainerIdFromNode2_1,completedContainers.get(0)
        .getContainerId()); 
    Assert.assertEquals(completedContainerIdFromNode2_2,completedContainers.get(1)
        .getContainerId());   
  }
  
  @Test (timeout = 5000)
  public void testStatusChange(){
    //Start the node
    node.handle(new RMNodeStartedEvent(null, null, null));
    //Add info to the queue first
    node.setNextHeartBeat(false);

    ContainerId completedContainerId1 = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(0, 0), 0), 0);
    ContainerId completedContainerId2 = BuilderUtils.newContainerId(
        BuilderUtils.newApplicationAttemptId(
            BuilderUtils.newApplicationId(1, 1), 1), 1);
        
    RMNodeStatusEvent statusEvent1 = getMockRMNodeStatusEvent();
    RMNodeStatusEvent statusEvent2 = getMockRMNodeStatusEvent();

    ContainerStatus containerStatus1 = mock(ContainerStatus.class);
    ContainerStatus containerStatus2 = mock(ContainerStatus.class);

    doReturn(completedContainerId1).when(containerStatus1).getContainerId();
    doReturn(Collections.singletonList(containerStatus1))
        .when(statusEvent1).getContainers();
     
    doReturn(completedContainerId2).when(containerStatus2).getContainerId();
    doReturn(Collections.singletonList(containerStatus2))
        .when(statusEvent2).getContainers();

    verify(scheduler,times(1)).handle(any(NodeUpdateSchedulerEvent.class)); 
    node.handle(statusEvent1);
    node.handle(statusEvent2);
    verify(scheduler,times(1)).handle(any(NodeUpdateSchedulerEvent.class));
    Assert.assertEquals(2, node.getQueueSize());
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE));
    Assert.assertEquals(0, node.getQueueSize());
  }
  
  @Test
  public void testRunningExpire() {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE));
    Assert.assertEquals("Active Nodes", initialActive - 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost + 1, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test
  public void testUnhealthyExpire() {
    RMNodeImpl node = getUnhealthyNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost + 1, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy - 1, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.LOST, node.getState());
  }
  
  @Test
  public void testUnhealthyExpireForSchedulerRemove() {
    RMNodeImpl node = getUnhealthyNode();
    verify(scheduler,times(2)).handle(any(NodeRemovedSchedulerEvent.class));
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE));
    verify(scheduler,times(2)).handle(any(NodeRemovedSchedulerEvent.class));
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test
  public void testRunningDecommission() {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.DECOMMISSION));
    Assert.assertEquals("Active Nodes", initialActive - 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned + 1, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.DECOMMISSIONED, node.getState());
  }

  @Test
  public void testUnhealthyDecommission() {
    RMNodeImpl node = getUnhealthyNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.DECOMMISSION));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy - 1, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned + 1, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.DECOMMISSIONED, node.getState());
  }

  @Test
  public void testRunningRebooting() {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.REBOOTING));
    Assert.assertEquals("Active Nodes", initialActive - 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted + 1, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  @Test
  public void testUnhealthyRebooting() {
    RMNodeImpl node = getUnhealthyNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.REBOOTING));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy - 1, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted + 1, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  @Test(timeout=20000)
  public void testUpdateHeartbeatResponseForCleanup() {
    RMNodeImpl node = getRunningNode();
    NodeId nodeId = node.getNodeID();

    // Expire a container
		ContainerId completedContainerId = BuilderUtils.newContainerId(
				BuilderUtils.newApplicationAttemptId(
						BuilderUtils.newApplicationId(0, 0), 0), 0);
    node.handle(new RMNodeCleanContainerEvent(nodeId, completedContainerId));
    Assert.assertEquals(1, node.getContainersToCleanUp().size());

    // Finish an application
    ApplicationId finishedAppId = BuilderUtils.newApplicationId(0, 1);
    node.handle(new RMNodeCleanAppEvent(nodeId, finishedAppId));
    Assert.assertEquals(1, node.getAppsToCleanup().size());

    // Verify status update does not clear containers/apps to cleanup
    // but updating heartbeat response for cleanup does
    RMNodeStatusEvent statusEvent = getMockRMNodeStatusEvent();
    node.handle(statusEvent);
    Assert.assertEquals(1, node.getContainersToCleanUp().size());
    Assert.assertEquals(1, node.getAppsToCleanup().size());
    NodeHeartbeatResponse hbrsp = Records.newRecord(NodeHeartbeatResponse.class);
    node.updateNodeHeartbeatResponseForCleanup(hbrsp);
    Assert.assertEquals(0, node.getContainersToCleanUp().size());
    Assert.assertEquals(0, node.getAppsToCleanup().size());
    Assert.assertEquals(1, hbrsp.getContainersToCleanup().size());
    Assert.assertEquals(completedContainerId, hbrsp.getContainersToCleanup().get(0));
    Assert.assertEquals(1, hbrsp.getApplicationsToCleanup().size());
    Assert.assertEquals(finishedAppId, hbrsp.getApplicationsToCleanup().get(0));
  }

  private RMNodeImpl getRunningNode() {
    return getRunningNode(null);
  }

  private RMNodeImpl getRunningNode(String nmVersion) {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    Resource capability = Resource.newInstance(4096, 4);
    RMNodeImpl node = new RMNodeImpl(nodeId, rmContext,null, 0, 0,
        null, capability, nmVersion);
    node.handle(new RMNodeStartedEvent(node.getNodeID(), null, null));
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    return node;
  }

  private RMNodeImpl getUnhealthyNode() {
    RMNodeImpl node = getRunningNode();
    NodeHealthStatus status = NodeHealthStatus.newInstance(false, "sick",
        System.currentTimeMillis());
    node.handle(new RMNodeStatusEvent(node.getNodeID(), status,
        new ArrayList<ContainerStatus>(), null, null));
    Assert.assertEquals(NodeState.UNHEALTHY, node.getState());
    return node;
  }


  private RMNodeImpl getNewNode() {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    RMNodeImpl node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null, null);
    return node;
  }
  
  private RMNodeImpl getNewNode(Resource capability) {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    RMNodeImpl node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, 
        capability, null);
    return node;
  }
  
  private RMNodeImpl getRebootedNode() {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    Resource capability = Resource.newInstance(4096, 4);
    RMNodeImpl node = new RMNodeImpl(nodeId, rmContext,null, 0, 0,
        null, capability, null);
    node.handle(new RMNodeStartedEvent(node.getNodeID(), null, null));
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.REBOOTING));
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
    return node;
  }

  @Test
  public void testAdd() {
    RMNodeImpl node = getNewNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeStartedEvent(node.getNodeID(), null, null));
    Assert.assertEquals("Active Nodes", initialActive + 1, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    Assert.assertNotNull(nodesListManagerEvent);
    Assert.assertEquals(NodesListManagerEventType.NODE_USABLE, 
        nodesListManagerEvent.getType());
  }

  @Test
  public void testReconnect() {
    RMNodeImpl node = getRunningNode();
    ClusterMetrics cm = ClusterMetrics.getMetrics();
    int initialActive = cm.getNumActiveNMs();
    int initialLost = cm.getNumLostNMs();
    int initialUnhealthy = cm.getUnhealthyNMs();
    int initialDecommissioned = cm.getNumDecommisionedNMs();
    int initialRebooted = cm.getNumRebootedNMs();
    node.handle(new RMNodeReconnectEvent(node.getNodeID(), node, null));
    Assert.assertEquals("Active Nodes", initialActive, cm.getNumActiveNMs());
    Assert.assertEquals("Lost Nodes", initialLost, cm.getNumLostNMs());
    Assert.assertEquals("Unhealthy Nodes",
        initialUnhealthy, cm.getUnhealthyNMs());
    Assert.assertEquals("Decommissioned Nodes",
        initialDecommissioned, cm.getNumDecommisionedNMs());
    Assert.assertEquals("Rebooted Nodes",
        initialRebooted, cm.getNumRebootedNMs());
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    Assert.assertNotNull(nodesListManagerEvent);
    Assert.assertEquals(NodesListManagerEventType.NODE_USABLE,
        nodesListManagerEvent.getType());
  }
  
  @Test
  public void testResourceUpdateOnRunningNode() {
    RMNodeImpl node = getRunningNode();
    Resource oldCapacity = node.getTotalCapability();
    assertEquals("Memory resource is not match.", oldCapacity.getMemory(), 4096);
    assertEquals("CPU resource is not match.", oldCapacity.getVirtualCores(), 4);
    node.handle(new RMNodeResourceUpdateEvent(node.getNodeID(),
        ResourceOption.newInstance(Resource.newInstance(2048, 2), 
            RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT)));
    Resource newCapacity = node.getTotalCapability();
    assertEquals("Memory resource is not match.", newCapacity.getMemory(), 2048);
    assertEquals("CPU resource is not match.", newCapacity.getVirtualCores(), 2);
    
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    Assert.assertNotNull(nodesListManagerEvent);
    Assert.assertEquals(NodesListManagerEventType.NODE_USABLE,
        nodesListManagerEvent.getType());
  }
  
  @Test
  public void testResourceUpdateOnNewNode() {
    RMNodeImpl node = getNewNode(Resource.newInstance(4096, 4));
    Resource oldCapacity = node.getTotalCapability();
    assertEquals("Memory resource is not match.", oldCapacity.getMemory(), 4096);
    assertEquals("CPU resource is not match.", oldCapacity.getVirtualCores(), 4);
    node.handle(new RMNodeResourceUpdateEvent(node.getNodeID(),
        ResourceOption.newInstance(Resource.newInstance(2048, 2), 
            RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT)));
    Resource newCapacity = node.getTotalCapability();
    assertEquals("Memory resource is not match.", newCapacity.getMemory(), 2048);
    assertEquals("CPU resource is not match.", newCapacity.getVirtualCores(), 2);
    
    Assert.assertEquals(NodeState.NEW, node.getState());
  }
  
  @Test
  public void testResourceUpdateOnRebootedNode() {
    RMNodeImpl node = getRebootedNode();
    Resource oldCapacity = node.getTotalCapability();
    assertEquals("Memory resource is not match.", oldCapacity.getMemory(), 4096);
    assertEquals("CPU resource is not match.", oldCapacity.getVirtualCores(), 4);
    node.handle(new RMNodeResourceUpdateEvent(node.getNodeID(),
        ResourceOption.newInstance(Resource.newInstance(2048, 2), 
            RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT)));
    Resource newCapacity = node.getTotalCapability();
    assertEquals("Memory resource is not match.", newCapacity.getMemory(), 2048);
    assertEquals("CPU resource is not match.", newCapacity.getVirtualCores(), 2);
    
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  @Test
  public void testReconnnectUpdate() {
    final String nmVersion1 = "nm version 1";
    final String nmVersion2 = "nm version 2";
    RMNodeImpl node = getRunningNode(nmVersion1);
    Assert.assertEquals(nmVersion1, node.getNodeManagerVersion());
    RMNodeImpl reconnectingNode = getRunningNode(nmVersion2);
    node.handle(new RMNodeReconnectEvent(node.getNodeID(), reconnectingNode,
        null));
    Assert.assertEquals(nmVersion2, node.getNodeManagerVersion());
  }
}
