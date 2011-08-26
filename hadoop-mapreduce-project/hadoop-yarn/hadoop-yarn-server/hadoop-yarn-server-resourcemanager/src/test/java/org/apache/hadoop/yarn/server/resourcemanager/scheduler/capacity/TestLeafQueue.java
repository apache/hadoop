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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestLeafQueue {
  private static final Log LOG = LogFactory.getLog(TestLeafQueue.class);
  
  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  RMContext rmContext;
  CapacityScheduler cs;
  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;
  
  Queue root;
  Map<String, Queue> queues = new HashMap<String, Queue>();
  
  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  @Before
  public void setUp() throws Exception {
    cs = new CapacityScheduler();
    rmContext = TestUtils.getMockRMContext();
    
    csConf = 
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    
    
    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(Resources.createResource(GB));
    when(csContext.getMaximumResourceCapability()).thenReturn(Resources.createResource(16*GB));
    root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, 
            CapacityScheduler.queueComparator, 
            CapacityScheduler.applicationComparator, 
            TestUtils.spyHook);

    cs.reinitialize(csConf, null, rmContext);
  }
  
  private static final String A = "a";
  private static final String B = "b";
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacityScheduler.ROOT, new String[] {A, B});
    conf.setCapacity(CapacityScheduler.ROOT, 100);
    
    final String Q_A = CapacityScheduler.ROOT + "." + A;
    conf.setCapacity(Q_A, 10);
    
    final String Q_B = CapacityScheduler.ROOT + "." + B;
    conf.setCapacity(Q_B, 90);
    
    LOG.info("Setup top-level queues a and b");
  }

  private LeafQueue stubLeafQueue(LeafQueue queue) {
    
    // Mock some methods for ease in these unit tests
    
    // 1. LeafQueue.createContainer to return dummy containers
    doAnswer(
        new Answer<Container>() {
          @Override
          public Container answer(InvocationOnMock invocation) 
              throws Throwable {
            final SchedulerApp application = 
                (SchedulerApp)(invocation.getArguments()[0]);
            final ContainerId containerId =                 
                TestUtils.getMockContainerId(application);

            Container container = TestUtils.getMockContainer(
                containerId,
                ((SchedulerNode)(invocation.getArguments()[1])).getNodeID(), 
                (Resource)(invocation.getArguments()[2]));
            return container;
          }
        }
      ).
      when(queue).createContainer(
              any(SchedulerApp.class), 
              any(SchedulerNode.class), 
              any(Resource.class));
    
    // 2. Stub out LeafQueue.parent.completedContainer
    Queue parent = queue.getParent();
    doNothing().when(parent).completedContainer(
        any(Resource.class), any(SchedulerApp.class), any(SchedulerNode.class), 
        any(RMContainer.class), any(RMContainerEventType.class));
    
    return queue;
  }
  
  @Test
  public void testSingleQueueWithOneUser() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    SchedulerApp app_0 = 
        new SchedulerApp(appAttemptId_0, user_0, a, rmContext, null);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    SchedulerApp app_1 = 
        new SchedulerApp(appAttemptId_1, user_0, a, rmContext, null);
    a.submitApplication(app_1, user_0, A);  // same user

    // Setup some nodes
    String host_0 = "host_0";
    SchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 1;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 3, priority, 
                recordFactory))); 

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, priority,
            recordFactory))); 

    // Start testing...
    
    // Only 1 container
    a.assignContainers(clusterResource, node_0);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    // Can't allocate 3rd due to user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    // Bump up user-limit-factor, now allocate should work
    a.setUserLimitFactor(10);
    a.assignContainers(clusterResource, node_0);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    // One more should work, for app_1, due to user-limit-factor
    a.assignContainers(clusterResource, node_0);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());

    // Test max-capacity
    // Now - no more allocs since we are at max-cap
    a.setMaxCapacity(0.5f);
    a.assignContainers(clusterResource, node_0);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    
    // Release each container from app_0
    for (RMContainer rmContainer : app_0.getLiveContainers()) {
      a.completedContainer(clusterResource, app_0, node_0, rmContainer, 
          RMContainerEventType.KILL);
    }
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    
    // Release each container from app_1
    for (RMContainer rmContainer : app_1.getLiveContainers()) {
      a.completedContainer(clusterResource, app_1, node_0, rmContainer, 
          RMContainerEventType.KILL);
    }
    assertEquals(0*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
  }
  
  @Test
  public void testSingleQueueWithMultipleUsers() throws Exception {
    
    // Mock the queue
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    
    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    final String user_2 = "user_2";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    SchedulerApp app_0 = 
        new SchedulerApp(appAttemptId_0, user_0, a, rmContext, null);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    SchedulerApp app_1 = 
        new SchedulerApp(appAttemptId_1, user_0, a, rmContext, null);
    a.submitApplication(app_1, user_0, A);  // same user

    // Setup some nodes
    String host_0 = "host_0";
    SchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 1;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 10, priority,
                recordFactory))); 

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 10, priority,
            recordFactory))); 

    /** 
     * Start testing... 
     */
    
    // Only 1 container
    a.assignContainers(clusterResource, node_0);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    // Can't allocate 3rd due to user-limit
    a.setUserLimit(25);
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    // Submit more apps
    final ApplicationAttemptId appAttemptId_2 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    SchedulerApp app_2 = 
        new SchedulerApp(appAttemptId_2, user_1, a, rmContext, null);
    a.submitApplication(app_2, user_1, A);

    final ApplicationAttemptId appAttemptId_3 = 
        TestUtils.getMockApplicationAttemptId(3, 0); 
    SchedulerApp app_3 = 
        new SchedulerApp(appAttemptId_3, user_2, a, rmContext, null);
    a.submitApplication(app_3, user_2, A);
    
    app_2.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 3*GB, 1, priority,
            recordFactory))); 

    app_3.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, priority,
            recordFactory))); 

    // Now allocations should goto app_2 since 
    // user_0 is at limit inspite of high user-limit-factor
    a.setUserLimitFactor(10);
    a.assignContainers(clusterResource, node_0);
    assertEquals(5*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemory());

    // Now allocations should goto app_0 since 
    // user_0 is at user-limit not above it
    a.assignContainers(clusterResource, node_0);
    assertEquals(6*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemory());
    
    // Test max-capacity
    // Now - no more allocs since we are at max-cap
    a.setMaxCapacity(0.5f);
    a.assignContainers(clusterResource, node_0);
    assertEquals(6*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemory());
    
    // Revert max-capacity and user-limit-factor
    // Now, allocations should goto app_3 since it's under user-limit 
    a.setMaxCapacity(-1);
    a.setUserLimitFactor(1);
    a.assignContainers(clusterResource, node_0);
    assertEquals(7*GB, a.getUsedResources().getMemory()); 
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_3.getCurrentConsumption().getMemory());

    // Now we should assign to app_3 again since user_2 is under user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(8*GB, a.getUsedResources().getMemory()); 
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemory());

    // 8. Release each container from app_0
    for (RMContainer rmContainer : app_0.getLiveContainers()) {
      a.completedContainer(clusterResource, app_0, node_0, rmContainer, 
          RMContainerEventType.KILL);
    }
    assertEquals(5*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemory());
    
    // 9. Release each container from app_2
    for (RMContainer rmContainer : app_2.getLiveContainers()) {
      a.completedContainer(clusterResource, app_2, node_0, rmContainer, 
          RMContainerEventType.KILL);
    }
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemory());

    // 10. Release each container from app_3
    for (RMContainer rmContainer : app_3.getLiveContainers()) {
      a.completedContainer(clusterResource, app_3, node_0, rmContainer, 
          RMContainerEventType.KILL);
    }
    assertEquals(0*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemory());
  }
  
  @Test
  public void testReservation() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    SchedulerApp app_0 = 
        new SchedulerApp(appAttemptId_0, user_0, a, rmContext, null);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    SchedulerApp app_1 = 
        new SchedulerApp(appAttemptId_1, user_1, a, rmContext, null);
    a.submitApplication(app_1, user_1, A);  

    // Setup some nodes
    String host_0 = "host_0";
    SchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 4*GB);
    
    final int numNodes = 1;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, priority,
                recordFactory))); 

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 4*GB, 1, priority,
            recordFactory))); 

    // Start testing...
    
    // Only 1 container
    a.assignContainers(clusterResource, node_0);
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    
    // Now, reservation should kick in for app_1
    a.assignContainers(clusterResource, node_0);
    assertEquals(6*GB, a.getUsedResources().getMemory()); 
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(2*GB, node_0.getUsedResource().getMemory());
    
    // Now free 1 container from app_0 i.e. 1G
    a.completedContainer(clusterResource, app_0, node_0, 
        app_0.getLiveContainers().iterator().next(), RMContainerEventType.KILL);
    a.assignContainers(clusterResource, node_0);
    assertEquals(5*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(1*GB, node_0.getUsedResource().getMemory());

    // Now finish another container from app_0 and fulfill the reservation
    a.completedContainer(clusterResource, app_0, node_0, 
        app_0.getLiveContainers().iterator().next(), RMContainerEventType.KILL);
    a.assignContainers(clusterResource, node_0);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(4*GB, node_0.getUsedResource().getMemory());
  }
  
  
  @Test
  public void testLocalityScheduling() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    // User
    String user_0 = "user_0";
    
    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    SchedulerApp app_0 = 
        spy(new SchedulerApp(appAttemptId_0, user_0, a, rmContext, null));
    a.submitApplication(app_0, user_0, A);
    
    // Setup some nodes and racks
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    SchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 8*GB);
    
    String host_1 = "host_1";
    String rack_1 = "rack_1";
    SchedulerNode node_1 = TestUtils.getMockNode(host_1, rack_1, 0, 8*GB);
    
    String host_2 = "host_2";
    String rack_2 = "rack_2";
    SchedulerNode node_2 = TestUtils.getMockNode(host_2, rack_2, 0, 8*GB);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests and submit
    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 3, // one extra 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // Start testing...
    
    // Start with off switch, shouldn't allocate due to delay scheduling
    a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(1, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));

    // Another off switch, shouldn't allocate due to delay scheduling
    a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(2, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    
    // Another off switch, shouldn't allocate due to delay scheduling
    a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(3, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    
    // Another off switch, now we should allocate 
    // since missedOpportunities=3 and reqdContainers=3
    a.assignContainers(clusterResource, node_2);
    verify(app_0).allocate(eq(NodeType.OFF_SWITCH), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(2, app_0.getTotalRequiredResources(priority));
    
    // NODE_LOCAL - node_0
    a.assignContainers(clusterResource, node_0);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    
    // NODE_LOCAL - node_1
    a.assignContainers(clusterResource, node_1);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));
    
    // Add 1 more request to check for RACK_LOCAL
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 1, // one extra 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    
    String host_3 = "host_3"; // on rack_1
    SchedulerNode node_3 = TestUtils.getMockNode(host_3, rack_1, 0, 8*GB);
    
    a.assignContainers(clusterResource, node_3);
    verify(app_0).allocate(eq(NodeType.RACK_LOCAL), eq(node_3), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));
  }
  
  @Test
  public void testApplicationPriorityScheduling() throws Exception {
    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    // User
    String user_0 = "user_0";
    
    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    SchedulerApp app_0 = 
        spy(new SchedulerApp(appAttemptId_0, user_0, a, rmContext, null));
    a.submitApplication(app_0, user_0, A);
    
    // Setup some nodes and racks
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    SchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 8*GB);
    
    String host_1 = "host_1";
    String rack_1 = "rack_1";
    SchedulerNode node_1 = TestUtils.getMockNode(host_1, rack_1, 0, 8*GB);
    
    String host_2 = "host_2";
    String rack_2 = "rack_2";
    SchedulerNode node_2 = TestUtils.getMockNode(host_2, rack_2, 0, 8*GB);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests and submit
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    
    // P1
    Priority priority_1 = TestUtils.createMockPriority(1);
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0, 1*GB, 1, 
            priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1, 
            priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, 
            priority_1, recordFactory));
    
    // P2
    Priority priority_2 = TestUtils.createMockPriority(2);
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_2, 2*GB, 1, 
            priority_2, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_2, 2*GB, 1, 
            priority_2, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 2*GB, 1, 
            priority_2, recordFactory));
    
    app_0.updateResourceRequests(app_0_requests_0);

    // Start testing...
    
    // Start with off switch, shouldn't allocate P1 due to delay scheduling
    // thus, no P2 either!
    a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(1, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(2, app_0.getTotalRequiredResources(priority_1));
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        eq(priority_2), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    // Another off-switch, shouldn't allocate P1 due to delay scheduling
    // thus, no P2 either!
    a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(2, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(2, app_0.getTotalRequiredResources(priority_1));
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        eq(priority_2), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    // Another off-switch, shouldn allocate OFF_SWITCH P1
    a.assignContainers(clusterResource, node_2);
    verify(app_0).allocate(eq(NodeType.OFF_SWITCH), eq(node_2), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(1, app_0.getTotalRequiredResources(priority_1));
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        eq(priority_2), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    // Now, DATA_LOCAL for P1
    a.assignContainers(clusterResource, node_0);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(0, app_0.getTotalRequiredResources(priority_1));
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_0), 
        eq(priority_2), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(1, app_0.getTotalRequiredResources(priority_2));

    // Now, OFF_SWITCH for P2
    a.assignContainers(clusterResource, node_1);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_1));
    assertEquals(0, app_0.getTotalRequiredResources(priority_1));
    verify(app_0).allocate(eq(NodeType.OFF_SWITCH), eq(node_1), 
        eq(priority_2), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(0, app_0.getTotalRequiredResources(priority_2));

  }
  
  @After
  public void tearDown() throws Exception {
  }
}
