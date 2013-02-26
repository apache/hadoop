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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
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

  @Before
  public void setUp() throws Exception {
    InlineDispatcher rmDispatcher = new InlineDispatcher();
    
    rmContext =
        new RMContextImpl(rmDispatcher, null, null, null,
            mock(DelegationTokenRenewer.class), null, null, null);
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
    
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null);

  }
  
  @After
  public void tearDown() throws Exception {
  }
  
  private RMNodeStatusEvent getMockRMNodeStatusEvent() {
    HeartbeatResponse response = mock(HeartbeatResponse.class);

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
    node.handle(new RMNodeEvent(null, RMNodeEventType.STARTED));
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
    node.handle(new RMNodeEvent(null,RMNodeEventType.STARTED));
    
    NodeId nodeId = BuilderUtils.newNodeId("localhost:1", 1);
    RMNodeImpl node2 = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null);
    node2.handle(new RMNodeEvent(null,RMNodeEventType.STARTED));
    
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
    node.handle(new RMNodeEvent(null,RMNodeEventType.STARTED));
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
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE));
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test
  public void testUnhealthyExpire() {
    RMNodeImpl node = getUnhealthyNode();
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE));
    Assert.assertEquals(NodeState.LOST, node.getState());
  }

  @Test
  public void testRunningDecommission() {
    RMNodeImpl node = getRunningNode();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.DECOMMISSION));
    Assert.assertEquals(NodeState.DECOMMISSIONED, node.getState());
  }

  @Test
  public void testUnhealthyDecommission() {
    RMNodeImpl node = getUnhealthyNode();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.DECOMMISSION));
    Assert.assertEquals(NodeState.DECOMMISSIONED, node.getState());
  }

  @Test
  public void testRunningRebooting() {
    RMNodeImpl node = getRunningNode();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.REBOOTING));
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  @Test
  public void testUnhealthyRebooting() {
    RMNodeImpl node = getUnhealthyNode();
    node.handle(new RMNodeEvent(node.getNodeID(),
        RMNodeEventType.REBOOTING));
    Assert.assertEquals(NodeState.REBOOTED, node.getState());
  }

  private RMNodeImpl getRunningNode() {
    NodeId nodeId = BuilderUtils.newNodeId("localhost", 0);
    RMNodeImpl node = new RMNodeImpl(nodeId, rmContext,null, 0, 0,
        null, null);
    node.handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.STARTED));
    Assert.assertEquals(NodeState.RUNNING, node.getState());
    return node;
  }

  private RMNodeImpl getUnhealthyNode() {
    RMNodeImpl node = getRunningNode();
    NodeHealthStatus status = node.getNodeHealthStatus();
    status.setHealthReport("sick");
    status.setIsNodeHealthy(false);
    node.handle(new RMNodeStatusEvent(node.getNodeID(), status,
        new ArrayList<ContainerStatus>(), null, null));
    Assert.assertEquals(NodeState.UNHEALTHY, node.getState());
    return node;
  }
}
