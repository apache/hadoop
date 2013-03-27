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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
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
  
  CSQueue root;
  Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
  
  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
  
  @Before
  public void setUp() throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    cs = spy(spyCs);
    rmContext = TestUtils.getMockRMContext();
    
    csConf = 
        new CapacitySchedulerConfiguration();
    csConf.setBoolean("yarn.scheduler.capacity.user-metrics.enable", true);
    final String newRoot = "root" + System.currentTimeMillis();
    setupQueueConfiguration(csConf, newRoot);
    YarnConfiguration conf = new YarnConfiguration();
    cs.setConf(conf);
    
    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB, 32));
    when(csContext.getClusterResources()).
        thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getApplicationComparator()).
    thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).
        thenReturn(CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).
        thenReturn(resourceCalculator);

    root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, 
            CapacitySchedulerConfiguration.ROOT, 
            queues, queues, 
            TestUtils.spyHook);

    cs.reinitialize(csConf, rmContext);
  }
  
  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";
  private static final String C1 = "c1";
  private static final String D = "d";
  private void setupQueueConfiguration(
      CapacitySchedulerConfiguration conf, 
      final String newRoot) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {newRoot});
    conf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100);
    conf.setAcl(CapacitySchedulerConfiguration.ROOT, QueueACL.SUBMIT_APPLICATIONS, " ");
    
    final String Q_newRoot = CapacitySchedulerConfiguration.ROOT + "." + newRoot;
    conf.setQueues(Q_newRoot, new String[] {A, B, C, D});
    conf.setCapacity(Q_newRoot, 100);
    conf.setMaximumCapacity(Q_newRoot, 100);
    conf.setAcl(Q_newRoot, QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_A = Q_newRoot + "." + A;
    conf.setCapacity(Q_A, 8.5f);
    conf.setMaximumCapacity(Q_A, 20);
    conf.setAcl(Q_A, QueueACL.SUBMIT_APPLICATIONS, "*");
    
    final String Q_B = Q_newRoot + "." + B;
    conf.setCapacity(Q_B, 80);
    conf.setMaximumCapacity(Q_B, 99);
    conf.setAcl(Q_B, QueueACL.SUBMIT_APPLICATIONS, "*");

    final String Q_C = Q_newRoot + "." + C;
    conf.setCapacity(Q_C, 1.5f);
    conf.setMaximumCapacity(Q_C, 10);
    conf.setAcl(Q_C, QueueACL.SUBMIT_APPLICATIONS, " ");
    
    conf.setQueues(Q_C, new String[] {C1});

    final String Q_C1 = Q_C + "." + C1;
    conf.setCapacity(Q_C1, 100);

    final String Q_D = Q_newRoot + "." + D;
    conf.setCapacity(Q_D, 10);
    conf.setMaximumCapacity(Q_D, 11);
    conf.setAcl(Q_D, QueueACL.SUBMIT_APPLICATIONS, "user_d");
    
  }

  static LeafQueue stubLeafQueue(LeafQueue queue) {
    
    // Mock some methods for ease in these unit tests
    
    // 1. LeafQueue.createContainer to return dummy containers
    doAnswer(
        new Answer<Container>() {
          @Override
          public Container answer(InvocationOnMock invocation) 
              throws Throwable {
            final FiCaSchedulerApp application = 
                (FiCaSchedulerApp)(invocation.getArguments()[0]);
            final ContainerId containerId =                 
                TestUtils.getMockContainerId(application);

            Container container = TestUtils.getMockContainer(
                containerId,
                ((FiCaSchedulerNode)(invocation.getArguments()[1])).getNodeID(), 
                (Resource)(invocation.getArguments()[2]),
                ((Priority)invocation.getArguments()[3]));
            return container;
          }
        }
      ).
      when(queue).createContainer(
              any(FiCaSchedulerApp.class), 
              any(FiCaSchedulerNode.class), 
              any(Resource.class),
              any(Priority.class)
              );
    
    // 2. Stub out LeafQueue.parent.completedContainer
    CSQueue parent = queue.getParent();
    doNothing().when(parent).completedContainer(
        any(Resource.class), any(FiCaSchedulerApp.class), any(FiCaSchedulerNode.class), 
        any(RMContainer.class), any(ContainerStatus.class), 
        any(RMContainerEventType.class));
    
    return queue;
  }
  
  @Test
  public void testInitializeQueue() throws Exception {
	  final float epsilon = 1e-5f;
	  //can add more sturdy test with 3-layer queues 
	  //once MAPREDUCE:3410 is resolved
	  LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
	  assertEquals(0.085, a.getCapacity(), epsilon);
	  assertEquals(0.085, a.getAbsoluteCapacity(), epsilon);
	  assertEquals(0.2, a.getMaximumCapacity(), epsilon);
	  assertEquals(0.2, a.getAbsoluteMaximumCapacity(), epsilon);
	  
	  LeafQueue b = stubLeafQueue((LeafQueue)queues.get(B));
	  assertEquals(0.80, b.getCapacity(), epsilon);
	  assertEquals(0.80, b.getAbsoluteCapacity(), epsilon);
	  assertEquals(0.99, b.getMaximumCapacity(), epsilon);
	  assertEquals(0.99, b.getAbsoluteMaximumCapacity(), epsilon);

	  ParentQueue c = (ParentQueue)queues.get(C);
	  assertEquals(0.015, c.getCapacity(), epsilon);
	  assertEquals(0.015, c.getAbsoluteCapacity(), epsilon);
	  assertEquals(0.1, c.getMaximumCapacity(), epsilon);
	  assertEquals(0.1, c.getAbsoluteMaximumCapacity(), epsilon);
  }
 
  @Test
  public void testSingleQueueOneUserMetrics() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(B));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_0, user_0, B);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_1, user_0, B);  // same user

    
    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 1;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 3, priority, 
                recordFactory))); 

    // Start testing...
    
    // Only 1 container
    a.assignContainers(clusterResource, node_0);
    assertEquals(6*GB, a.getMetrics().getAvailableMB());
  }

  @Test
  public void testUserQueueAcl() throws Exception {

    // Manipulate queue 'a'
    LeafQueue d = stubLeafQueue((LeafQueue) queues.get(D));

    // Users
    final String user_d = "user_d";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 1);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_d, d, null,
        rmContext);
    d.submitApplication(app_0, user_d, D);

    // Attempt the same application again
    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(0, 2);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_d, d, null,
        rmContext);
    d.submitApplication(app_1, user_d, D); // same user
  }


  @Test
  public void testAppAttemptMetrics() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(B));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 1);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, null,
        rmContext);
    a.submitApplication(app_0, user_0, B);
    
    when(cs.getApplication(appAttemptId_0)).thenReturn(app_0);
    AppRemovedSchedulerEvent event = new AppRemovedSchedulerEvent(
        appAttemptId_0, RMAppAttemptState.FAILED);
    cs.handle(event);
    
    assertEquals(0, a.getMetrics().getAppsPending());
    assertEquals(1, a.getMetrics().getAppsFailed());

    // Attempt the same application again
    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(0, 2);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, null,
        rmContext);
    a.submitApplication(app_1, user_0, B); // same user

    assertEquals(1, a.getMetrics().getAppsSubmitted());
    assertEquals(1, a.getMetrics().getAppsPending());
    
    when(cs.getApplication(appAttemptId_1)).thenReturn(app_0);
    event = new AppRemovedSchedulerEvent(appAttemptId_0,
        RMAppAttemptState.FINISHED);
    cs.handle(event);
    
    assertEquals(1, a.getMetrics().getAppsSubmitted());
    assertEquals(0, a.getMetrics().getAppsPending());
    assertEquals(0, a.getMetrics().getAppsFailed());
    assertEquals(1, a.getMetrics().getAppsCompleted());

    QueueMetrics userMetrics = a.getMetrics().getUserMetrics(user_0);
    assertEquals(1, userMetrics.getAppsSubmitted());
  }

  @Test
  public void testSingleQueueWithOneUser() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_1, user_0, A);  // same user

    
    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 1;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
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
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());
    assertEquals(0*GB, a.getMetrics().getAvailableMB());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Can't allocate 3rd due to user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Bump up user-limit-factor, now allocate should work
    a.setUserLimitFactor(10);
    a.assignContainers(clusterResource, node_0);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(3*GB, a.getMetrics().getAllocatedMB());

    // One more should work, for app_1, due to user-limit-factor
    a.assignContainers(clusterResource, node_0);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(4*GB, a.getMetrics().getAllocatedMB());

    // Test max-capacity
    // Now - no more allocs since we are at max-cap
    a.setMaxCapacity(0.5f);
    a.assignContainers(clusterResource, node_0);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(4*GB, a.getMetrics().getAllocatedMB());
    
    // Release each container from app_0
    for (RMContainer rmContainer : app_0.getLiveContainers()) {
      a.completedContainer(clusterResource, app_0, node_0, rmContainer, 
          null, RMContainerEventType.KILL);
    }
    assertEquals(1*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());
    
    // Release each container from app_1
    for (RMContainer rmContainer : app_1.getLiveContainers()) {
      a.completedContainer(clusterResource, app_1, node_0, rmContainer, 
          null, RMContainerEventType.KILL);
    }
    assertEquals(0*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(0*GB, a.getMetrics().getAllocatedMB());
    assertEquals(1*GB, a.getMetrics().getAvailableMB());
  }
  
  @Test
  public void testUserLimits() throws Exception {
    // Mock the queue
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);
    
    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_1, user_0, A);  // same user

    final ApplicationAttemptId appAttemptId_2 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_2 = 
        new FiCaSchedulerApp(appAttemptId_2, user_1, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_2, user_1, A);

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 2;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
 
    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(RMNodeImpl.ANY, 2*GB, 1, priority,
                recordFactory))); 

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, priority,
            recordFactory))); 

    /**
     * Start testing...
     */
    
    // Set user-limit
    a.setUserLimit(50);
    a.setUserLimitFactor(2);
    
    // Now, only user_0 should be active since he is the only one with
    // outstanding requests
    assertEquals("There should only be 1 active user!", 
        1, a.getActiveUsersManager().getNumActiveUsers());

    // This commented code is key to test 'activeUsers'. 
    // It should fail the test if uncommented since
    // it would increase 'activeUsers' to 2 and stop user_2
    // Pre MAPREDUCE-3732 this test should fail without this block too
//    app_2.updateResourceRequests(Collections.singletonList(
//        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 1, priority,
//            recordFactory))); 

    // 1 container to user_0
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());

    // Again one to user_0 since he hasn't exceeded user limit yet
    a.assignContainers(clusterResource, node_0);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());

    // One more to user_0 since he is the only active user
    a.assignContainers(clusterResource, node_1);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(2*GB, app_1.getCurrentConsumption().getMemory());
  }
  
  @Test
  public void testHeadroomWithMaxCap() throws Exception {
    // Mock the queue
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);
    
    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_1, user_0, A);  // same user

    final ApplicationAttemptId appAttemptId_2 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_2 = 
        new FiCaSchedulerApp(appAttemptId_2, user_1, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_2, user_1, A);

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB), 1);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
 
    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(RMNodeImpl.ANY, 2*GB, 1, priority,
                recordFactory))); 

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, priority,
            recordFactory))); 

    /**
     * Start testing...
     */
    
    // Set user-limit
    a.setUserLimit(50);
    a.setUserLimitFactor(2);
    
    // Now, only user_0 should be active since he is the only one with
    // outstanding requests
    assertEquals("There should only be 1 active user!", 
        1, a.getActiveUsersManager().getNumActiveUsers());

    // 1 container to user_0
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_0.getHeadroom().getMemory()); // User limit = 2G
    assertEquals(0*GB, app_0.getHeadroom().getMemory()); // User limit = 2G

    // Again one to user_0 since he hasn't exceeded user limit yet
    a.assignContainers(clusterResource, node_0);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_0.getHeadroom().getMemory()); // 3G - 2G
    assertEquals(0*GB, app_0.getHeadroom().getMemory()); // 3G - 2G
    
    // Submit requests for app_1 and set max-cap
    a.setMaxCapacity(.1f);
    app_2.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 1, priority,
            recordFactory))); 
    assertEquals(2, a.getActiveUsersManager().getNumActiveUsers());

    // No more to user_0 since he is already over user-limit
    // and no more containers to queue since it's already at max-cap
    a.assignContainers(clusterResource, node_1);
    assertEquals(3*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_0.getHeadroom().getMemory());
    assertEquals(0*GB, app_1.getHeadroom().getMemory());
    
    // Check headroom for app_2 
    LOG.info("here");
    app_1.updateResourceRequests(Collections.singletonList(     // unset
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 0, priority,
            recordFactory))); 
    assertEquals(1, a.getActiveUsersManager().getNumActiveUsers());
    a.assignContainers(clusterResource, node_1);
    assertEquals(1*GB, app_2.getHeadroom().getMemory());   // hit queue max-cap 
  }

  @Test
  public void testSingleQueueWithMultipleUsers() throws Exception {
    
    // Mock the queue
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);
    
    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    final String user_2 = "user_2";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_1, user_0, A);  // same user

    final ApplicationAttemptId appAttemptId_2 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_2 = 
        new FiCaSchedulerApp(appAttemptId_2, user_1, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_2, user_1, A);

    final ApplicationAttemptId appAttemptId_3 = 
        TestUtils.getMockApplicationAttemptId(3, 0); 
    FiCaSchedulerApp app_3 = 
        new FiCaSchedulerApp(appAttemptId_3, user_2, a, 
            a.getActiveUsersManager(), rmContext);
    a.submitApplication(app_3, user_2, A);
    
    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 1;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
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
    
    // Submit resource requests for other apps now to 'activate' them
    
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
    a.setMaxCapacity(1.0f);
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
          null, RMContainerEventType.KILL);
    }
    assertEquals(5*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemory());
    
    // 9. Release each container from app_2
    for (RMContainer rmContainer : app_2.getLiveContainers()) {
      a.completedContainer(clusterResource, app_2, node_0, rmContainer, 
          null, RMContainerEventType.KILL);
    }
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemory());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemory());

    // 10. Release each container from app_3
    for (RMContainer rmContainer : app_3.getLiveContainers()) {
      a.completedContainer(clusterResource, app_3, node_0, rmContainer, 
          null, RMContainerEventType.KILL);
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
    //unset maxCapacity
    a.setMaxCapacity(1.0f);

    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_1, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_1, user_1, A);  

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 4*GB);
    
    final int numNodes = 2;
    Resource clusterResource = 
        Resources.createResource(numNodes * (4*GB), numNodes * 16);
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
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());
    assertEquals(0*GB, a.getMetrics().getAvailableMB());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Now, reservation should kick in for app_1
    a.assignContainers(clusterResource, node_0);
    assertEquals(6*GB, a.getUsedResources().getMemory()); 
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(2*GB, node_0.getUsedResource().getMemory());
    assertEquals(4*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Now free 1 container from app_0 i.e. 1G
    a.completedContainer(clusterResource, app_0, node_0, 
        app_0.getLiveContainers().iterator().next(), null, RMContainerEventType.KILL);
    a.assignContainers(clusterResource, node_0);
    assertEquals(5*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(1*GB, node_0.getUsedResource().getMemory());
    assertEquals(4*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());

    // Now finish another container from app_0 and fulfill the reservation
    a.completedContainer(clusterResource, app_0, node_0, 
        app_0.getLiveContainers().iterator().next(), null, RMContainerEventType.KILL);
    a.assignContainers(clusterResource, node_0);
    assertEquals(4*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(4*GB, node_0.getUsedResource().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(4*GB, a.getMetrics().getAllocatedMB());
  }
  
  @Test
  public void testStolenReservedContainer() throws Exception {
    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);

    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_1, a,
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_1, user_1, A);

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 4*GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 4*GB);

    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (4*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(RMNodeImpl.ANY, 2*GB, 1, priority,
                recordFactory)));

    // Setup app_1 to request a 4GB container on host_0 and
    // another 4GB container anywhere.
    ArrayList<ResourceRequest> appRequests_1 =
        new ArrayList<ResourceRequest>(4);
    appRequests_1.add(TestUtils.createResourceRequest(host_0, 4*GB, 1,
        priority, recordFactory));
    appRequests_1.add(TestUtils.createResourceRequest(DEFAULT_RACK, 4*GB, 1,
        priority, recordFactory));
    appRequests_1.add(TestUtils.createResourceRequest(RMNodeImpl.ANY, 4*GB, 2,
        priority, recordFactory));
    app_1.updateResourceRequests(appRequests_1);

    // Start testing...

    a.assignContainers(clusterResource, node_0);
    assertEquals(2*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    assertEquals(0*GB, a.getMetrics().getAvailableMB());

    // Now, reservation should kick in for app_1
    a.assignContainers(clusterResource, node_0);
    assertEquals(6*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(2*GB, node_0.getUsedResource().getMemory());
    assertEquals(4*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());

    // node_1 heartbeats in and gets the DEFAULT_RACK request for app_1
    a.assignContainers(clusterResource, node_1);
    assertEquals(10*GB, a.getUsedResources().getMemory());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(4*GB, node_1.getUsedResource().getMemory());
    assertEquals(4*GB, a.getMetrics().getReservedMB());
    assertEquals(6*GB, a.getMetrics().getAllocatedMB());

    // Now free 1 container from app_0 and try to assign to node_0
    a.completedContainer(clusterResource, app_0, node_0,
        app_0.getLiveContainers().iterator().next(), null, RMContainerEventType.KILL);
    a.assignContainers(clusterResource, node_0);
    assertEquals(8*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(8*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(4*GB, node_0.getUsedResource().getMemory());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(8*GB, a.getMetrics().getAllocatedMB());
  }

  @Test
  public void testReservationExchange() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);
    a.setUserLimitFactor(10);

    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_0, user_0, A);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_1, a, 
            mock(ActiveUsersManager.class), rmContext);
    a.submitApplication(app_1, user_1, A);  

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 4*GB);
    
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 4*GB);
    
    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (4*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(4*GB, 16));
    when(a.getMaximumAllocation()).thenReturn(
        Resources.createResource(4*GB, 16));
    when(a.getMinimumAllocationFactor()).thenReturn(0.25f); // 1G / 4G 
    
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
    
    // Now free 1 container from app_0 i.e. 1G, and re-reserve it
    a.completedContainer(clusterResource, app_0, node_0, 
        app_0.getLiveContainers().iterator().next(), null, RMContainerEventType.KILL);
    a.assignContainers(clusterResource, node_0);
    assertEquals(5*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(1*GB, node_0.getUsedResource().getMemory());
    assertEquals(1, app_1.getReReservations(priority));

    // Re-reserve
    a.assignContainers(clusterResource, node_0);
    assertEquals(5*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(1*GB, node_0.getUsedResource().getMemory());
    assertEquals(2, app_1.getReReservations(priority));
    
    // Try to schedule on node_1 now, should *move* the reservation
    a.assignContainers(clusterResource, node_1);
    assertEquals(9*GB, a.getUsedResources().getMemory()); 
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(4*GB, node_1.getUsedResource().getMemory());
    // Doesn't change yet... only when reservation is cancelled or a different
    // container is reserved
    assertEquals(2, app_1.getReReservations(priority)); 
    
    // Now finish another container from app_0 and see the reservation cancelled
    a.completedContainer(clusterResource, app_0, node_0, 
        app_0.getLiveContainers().iterator().next(), null, RMContainerEventType.KILL);
    CSAssignment assignment = a.assignContainers(clusterResource, node_0);
    assertEquals(8*GB, a.getUsedResources().getMemory());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemory());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemory());
    assertEquals(0*GB, node_0.getUsedResource().getMemory());
    assertEquals(4*GB, 
        assignment.getExcessReservation().getContainer().getResource().getMemory());
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
    FiCaSchedulerApp app_0 = 
        spy(new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), rmContext));
    a.submitApplication(app_0, user_0, A);
    
    // Setup some nodes and racks
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 8*GB);
    
    String host_1 = "host_1";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, rack_1, 0, 8*GB);
    
    String host_2 = "host_2";
    String rack_2 = "rack_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, rack_2, 0, 8*GB);

    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
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
    CSAssignment assignment = null;
    
    // Start with off switch, shouldn't allocate due to delay scheduling
    assignment = a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(1, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL

    // Another off switch, shouldn't allocate due to delay scheduling
    assignment = a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(2, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    
    // Another off switch, shouldn't allocate due to delay scheduling
    assignment = a.assignContainers(clusterResource, node_2);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(3, app_0.getSchedulingOpportunities(priority));
    assertEquals(3, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    
    // Another off switch, now we should allocate 
    // since missedOpportunities=3 and reqdContainers=3
    assignment = a.assignContainers(clusterResource, node_2);
    verify(app_0).allocate(eq(NodeType.OFF_SWITCH), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(4, app_0.getSchedulingOpportunities(priority)); // should NOT reset
    assertEquals(2, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.OFF_SWITCH, assignment.getType());
    
    // NODE_LOCAL - node_0
    assignment = a.assignContainers(clusterResource, node_0);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType());
    
    // NODE_LOCAL - node_1
    assignment = a.assignContainers(clusterResource, node_1);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType());
    
    // Add 1 more request to check for RACK_LOCAL
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, // one extra 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    assertEquals(2, app_0.getTotalRequiredResources(priority));
    
    String host_3 = "host_3"; // on rack_1
    FiCaSchedulerNode node_3 = TestUtils.getMockNode(host_3, rack_1, 0, 8*GB);
    
    // Rack-delay
    doReturn(1).when(a).getNodeLocalityDelay();
    
    // Shouldn't assign RACK_LOCAL yet
    assignment = a.assignContainers(clusterResource, node_3);
    assertEquals(1, app_0.getSchedulingOpportunities(priority));
    assertEquals(2, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL

    // Should assign RACK_LOCAL now
    assignment = a.assignContainers(clusterResource, node_3);
    verify(app_0).allocate(eq(NodeType.RACK_LOCAL), eq(node_3), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.RACK_LOCAL, assignment.getType());
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
    FiCaSchedulerApp app_0 = 
        spy(new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), rmContext));
    a.submitApplication(app_0, user_0, A);
    
    // Setup some nodes and racks
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 8*GB);
    
    String host_1 = "host_1";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, rack_1, 0, 8*GB);
    
    String host_2 = "host_2";
    String rack_2 = "rack_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, rack_2, 0, 8*GB);

    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), 1);
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

    // Another off-switch, shouldn't allocate OFF_SWITCH P1
    a.assignContainers(clusterResource, node_2);
    verify(app_0).allocate(eq(NodeType.OFF_SWITCH), eq(node_2), 
        eq(priority_1), any(ResourceRequest.class), any(Container.class));
    assertEquals(3, app_0.getSchedulingOpportunities(priority_1));
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
    assertEquals(1, app_0.getSchedulingOpportunities(priority_2));
    assertEquals(0, app_0.getTotalRequiredResources(priority_2));

  }
  
  @Test
  public void testSchedulingConstraints() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    // User
    String user_0 = "user_0";
    
    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        spy(new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), rmContext));
    a.submitApplication(app_0, user_0, A);
    
    // Setup some nodes and racks
    String host_0_0 = "host_0_0";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0_0 = TestUtils.getMockNode(host_0_0, rack_0, 0, 8*GB);
    String host_0_1 = "host_0_1";
    FiCaSchedulerNode node_0_1 = TestUtils.getMockNode(host_0_1, rack_0, 0, 8*GB);
    
    
    String host_1_0 = "host_1_0";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1_0 = TestUtils.getMockNode(host_1_0, rack_1, 0, 8*GB);
    
    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(
        numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests and submit
    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0_0, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0_1, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1_0, 1*GB, 1, 
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // Start testing...
    
    // Add one request
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 1, // only one 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    
    // NODE_LOCAL - node_0_1
    a.assignContainers(clusterResource, node_0_0);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));

    // No allocation on node_1_0 even though it's node/rack local since
    // required(ANY) == 0
    a.assignContainers(clusterResource, node_1_0);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // Still zero
                                                               // since #req=0
    assertEquals(0, app_0.getTotalRequiredResources(priority));
    
    // Add one request
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 1, // only one 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // No allocation on node_0_1 even though it's node/rack local since
    // required(rack_1) == 0
    a.assignContainers(clusterResource, node_0_1);
    verify(app_0, never()).allocate(any(NodeType.class), eq(node_1_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(1, app_0.getSchedulingOpportunities(priority)); 
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    
    // NODE_LOCAL - node_1
    a.assignContainers(clusterResource, node_1_0);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_1_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));

  }

  public boolean hasQueueACL(List<QueueUserACLInfo> aclInfos, QueueACL acl) {
    for (QueueUserACLInfo aclInfo : aclInfos) {
      if (aclInfo.getUserAcls().contains(acl)) {
        return true;
      }
    }    
    return false;
  }

  @Test
  public void testInheritedQueueAcls() throws IOException {
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    LeafQueue b = stubLeafQueue((LeafQueue)queues.get(B));
    ParentQueue c = (ParentQueue)queues.get(C);
    LeafQueue c1 = stubLeafQueue((LeafQueue)queues.get(C1));

    assertFalse(root.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertTrue(a.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertTrue(b.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertFalse(c.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));
    assertFalse(c1.hasAccess(QueueACL.SUBMIT_APPLICATIONS, user));

    assertTrue(hasQueueACL(
          a.getQueueUserAclInfo(user), QueueACL.SUBMIT_APPLICATIONS));
    assertTrue(hasQueueACL(
          b.getQueueUserAclInfo(user), QueueACL.SUBMIT_APPLICATIONS));
    assertFalse(hasQueueACL(
          c.getQueueUserAclInfo(user), QueueACL.SUBMIT_APPLICATIONS));
    assertFalse(hasQueueACL(
          c1.getQueueUserAclInfo(user), QueueACL.SUBMIT_APPLICATIONS));

  }
  
  @After
  public void tearDown() throws Exception {
  }
}
