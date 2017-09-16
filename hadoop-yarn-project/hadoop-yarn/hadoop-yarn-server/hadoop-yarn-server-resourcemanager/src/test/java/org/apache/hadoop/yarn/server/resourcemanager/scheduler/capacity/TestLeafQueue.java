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
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueStateManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager.User;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils.toSchedulerKey;

public class TestLeafQueue {
  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  private static final Log LOG = LogFactory.getLog(TestLeafQueue.class);

  RMContext rmContext;
  RMContext spyRMContext;
  ResourceRequest amResourceRequest;
  CapacityScheduler cs;
  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;
  
  CSQueue root;
  Map<String, CSQueue> queues;
  
  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  private final ResourceCalculator resourceCalculator =
      new DefaultResourceCalculator();
  
  private final ResourceCalculator dominantResourceCalculator =
      new DominantResourceCalculator();

  @Before
  public void setUp() throws Exception {
    setUpInternal(resourceCalculator);
  }

  private void setUpWithDominantResourceCalculator() throws Exception {
    setUpInternal(dominantResourceCalculator);
  }

  private void setUpInternal(ResourceCalculator rC) throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    queues = new HashMap<String, CSQueue>();
    cs = spy(spyCs);
    rmContext = TestUtils.getMockRMContext();
    spyRMContext = spy(rmContext);

    ConcurrentMap<ApplicationId, RMApp> spyApps = 
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    when(rmApp.getRMAppAttempt((ApplicationAttemptId)Matchers.any())).thenReturn(null);
    amResourceRequest = mock(ResourceRequest.class);
    when(amResourceRequest.getCapability()).thenReturn(
      Resources.createResource(0, 0));
    when(rmApp.getAMResourceRequests()).thenReturn(
        Collections.singletonList(amResourceRequest));
    Mockito.doReturn(rmApp).when(spyApps).get((ApplicationId)Matchers.any());
    when(spyRMContext.getRMApps()).thenReturn(spyApps);
    
    csConf = 
        new CapacitySchedulerConfiguration();
    csConf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    csConf.setBoolean(CapacitySchedulerConfiguration.RESERVE_CONT_LOOK_ALL_NODES,
        false);
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
    when(csContext.getClusterResource()).
        thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getResourceCalculator()).
        thenReturn(resourceCalculator);
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
    when(csContext.getResourceCalculator()).thenReturn(rC);
    when(csContext.getRMContext()).thenReturn(rmContext);
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

    root = 
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT, 
            queues, queues, 
            TestUtils.spyHook);

    ResourceUsage queueResUsage = root.getQueueResourceUsage();
    when(csContext.getClusterResourceUsage())
        .thenReturn(queueResUsage);

    cs.setRMContext(spyRMContext);
    cs.init(csConf);
    cs.setResourceCalculator(rC);
    cs.start();

    when(spyRMContext.getScheduler()).thenReturn(cs);
    when(spyRMContext.getYarnConfiguration())
        .thenReturn(new YarnConfiguration());
    when(cs.getNumClusterNodes()).thenReturn(3);
  }


  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";
  private static final String C1 = "c1";
  private static final String D = "d";
  private static final String E = "e";
  private void setupQueueConfiguration(
      CapacitySchedulerConfiguration conf, 
      final String newRoot) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {newRoot});
    conf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100);
    conf.setAcl(CapacitySchedulerConfiguration.ROOT,
      QueueACL.SUBMIT_APPLICATIONS, " ");
    
    final String Q_newRoot = CapacitySchedulerConfiguration.ROOT + "." + newRoot;
    conf.setQueues(Q_newRoot, new String[] {A, B, C, D, E});
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
    conf.setCapacity(Q_D, 9);
    conf.setMaximumCapacity(Q_D, 11);
    conf.setAcl(Q_D, QueueACL.SUBMIT_APPLICATIONS, "user_d");
    
    final String Q_E = Q_newRoot + "." + E;
    conf.setCapacity(Q_E, 1);
    conf.setMaximumCapacity(Q_E, 1);
    conf.setAcl(Q_E, QueueACL.SUBMIT_APPLICATIONS, "user_e");

  }

  static LeafQueue stubLeafQueue(LeafQueue queue) {
    // Mock some methods for ease in these unit tests
    
    // 1. Stub out LeafQueue.parent.completedContainer
    CSQueue parent = queue.getParent();
    doNothing().when(parent).completedContainer(
        any(Resource.class), any(FiCaSchedulerApp.class), any(FiCaSchedulerNode.class), 
        any(RMContainer.class), any(ContainerStatus.class), 
        any(RMContainerEventType.class), any(CSQueue.class), anyBoolean());

    // Stub out parent queue's accept and apply.
    doReturn(true).when(parent).accept(any(Resource.class),
        any(ResourceCommitRequest.class));
    doNothing().when(parent).apply(any(Resource.class),
        any(ResourceCommitRequest.class));
    
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

    // Verify the value for getAMResourceLimit for queues with < .1 maxcap
    Resource clusterResource = Resource.newInstance(50 * GB, 50);

    a.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));
    assertEquals(Resource.newInstance(1 * GB, 1),
        a.calculateAndGetAMResourceLimit());

    b.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));
    assertEquals(Resource.newInstance(5 * GB, 1),
        b.calculateAndGetAMResourceLimit());
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
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);  // same user

    
    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0);

    final int numNodes = 1;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 3, true,
                priority, recordFactory)));

    // Start testing...
    
    // Only 1 container
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(
        (int)(node_0.getTotalResource().getMemorySize() * a.getCapacity()) - (1*GB),
        a.getMetrics().getAvailableMB());
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
        spyRMContext);
    d.submitApplicationAttempt(app_0, user_d);

    // Attempt the same application again
    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(0, 2);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_d, d, null,
        spyRMContext);
    d.submitApplicationAttempt(app_1, user_d); // same user
  }

  @Test
  public void testPolicyConfiguration() throws Exception {
    
    CapacitySchedulerConfiguration testConf = 
        new CapacitySchedulerConfiguration();
    
    String tproot = CapacitySchedulerConfiguration.ROOT + "." + 
      "testPolicyRoot" + System.currentTimeMillis();

    OrderingPolicy<FiCaSchedulerApp> comPol =    
      testConf.<FiCaSchedulerApp>getAppOrderingPolicy(tproot);
    
    
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

    AppAddedSchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appAttemptId_0.getApplicationId(),
          a.getQueueName(), user_0);
    cs.handle(addAppEvent);
    AppAttemptAddedSchedulerEvent addAttemptEvent = 
        new AppAttemptAddedSchedulerEvent(appAttemptId_0, false);
    cs.handle(addAttemptEvent);

    AppAttemptRemovedSchedulerEvent event = new AppAttemptRemovedSchedulerEvent(
        appAttemptId_0, RMAppAttemptState.FAILED, false);
    cs.handle(event);
    
    assertEquals(0, a.getMetrics().getAppsPending());
    assertEquals(0, a.getMetrics().getAppsFailed());

    // Attempt the same application again
    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(0, 2);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, null,
        spyRMContext);
    app_1.setAMResource(Resource.newInstance(100, 1));
    a.submitApplicationAttempt(app_1, user_0); // same user

    assertEquals(1, a.getMetrics().getAppsSubmitted());
    assertEquals(1, a.getMetrics().getAppsPending());
    assertEquals(1, a.getUser(user_0).getActiveApplications());
    assertEquals(app_1.getAMResource().getMemorySize(), a.getMetrics()
        .getUsedAMResourceMB());
    assertEquals(app_1.getAMResource().getVirtualCores(), a.getMetrics()
        .getUsedAMResourceVCores());
    
    event = new AppAttemptRemovedSchedulerEvent(appAttemptId_0,
        RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        appAttemptId_0.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);
    
    assertEquals(1, a.getMetrics().getAppsSubmitted());
    assertEquals(0, a.getMetrics().getAppsPending());
    assertEquals(0, a.getMetrics().getAppsFailed());
    assertEquals(1, a.getMetrics().getAppsCompleted());

    QueueMetrics userMetrics = a.getMetrics().getUserMetrics(user_0);
    assertEquals(1, userMetrics.getAppsSubmitted());
  }

  @Test
  public void testFairConfiguration() throws Exception {

    CapacitySchedulerConfiguration testConf =
        new CapacitySchedulerConfiguration();

    String tproot = CapacitySchedulerConfiguration.ROOT + "." +
      "testPolicyRoot" + System.currentTimeMillis();

    OrderingPolicy<FiCaSchedulerApp> schedOrder =
      testConf.<FiCaSchedulerApp>getAppOrderingPolicy(tproot);

    //override default to fair
    String policyType = CapacitySchedulerConfiguration.PREFIX + tproot +
      "." + CapacitySchedulerConfiguration.ORDERING_POLICY;

    testConf.set(policyType,
      CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY);
    schedOrder =
      testConf.<FiCaSchedulerApp>getAppOrderingPolicy(tproot);
    FairOrderingPolicy fop = (FairOrderingPolicy<FiCaSchedulerApp>) schedOrder;
    assertFalse(fop.getSizeBasedWeight());

    //Now with sizeBasedWeight
    String sbwConfig = CapacitySchedulerConfiguration.PREFIX + tproot +
      "." + CapacitySchedulerConfiguration.ORDERING_POLICY + "." +
      FairOrderingPolicy.ENABLE_SIZE_BASED_WEIGHT;
    testConf.set(sbwConfig, "true");
    schedOrder =
      testConf.<FiCaSchedulerApp>getAppOrderingPolicy(tproot);
    fop = (FairOrderingPolicy<FiCaSchedulerApp>) schedOrder;
    assertTrue(fop.getSizeBasedWeight());

  }

  @Test
  public void testSingleQueueWithOneUser() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);

    // Users
    final String user_0 = "user_0";

    // Active Users Manager
    AbstractUsersManager activeUserManager = a.getAbstractUsersManager();

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            activeUserManager, spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            activeUserManager, spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);  // same user

    
    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0);

    final int numNodes = 1;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 3, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    // Start testing...
    
    // Only 1 container
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(1*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());
    assertEquals(0*GB, a.getMetrics().getAvailableMB());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Can't allocate 3rd due to user-limit
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Bump up user-limit-factor, now allocate should work
    a.setUserLimitFactor(10);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(3*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(3*GB, a.getMetrics().getAllocatedMB());

    // One more should work, for app_1, due to user-limit-factor
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(4*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(4*GB, a.getMetrics().getAllocatedMB());

    // Test max-capacity
    // Now - no more allocs since we are at max-cap
    a.setMaxCapacity(0.5f);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(4*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(4*GB, a.getMetrics().getAllocatedMB());
    
    // Release each container from app_0
    for (RMContainer rmContainer : app_0.getLiveContainers()) {
      a.completedContainer(clusterResource, app_0, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }
    assertEquals(1*GB, a.getUsedResources().getMemorySize());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());
    
    // Release each container from app_1
    for (RMContainer rmContainer : app_1.getLiveContainers()) {
      a.completedContainer(clusterResource, app_1, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }

    assertEquals(0*GB, a.getUsedResources().getMemorySize());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(0*GB, a.getMetrics().getAllocatedMB());
    assertEquals((int)(a.getCapacity() * node_0.getTotalResource().getMemorySize()),
        a.getMetrics().getAvailableMB());
  }
  @Test
  public void testDRFUsageRatioRounding() throws Exception {
    CSAssignment assign;
    setUpWithDominantResourceCalculator();
    // Mock the queue
    LeafQueue b = stubLeafQueue((LeafQueue) queues.get(E));

    // Users
    final String user0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app0 =
        new FiCaSchedulerApp(appAttemptId0, user0, b,
            b.getAbstractUsersManager(), spyRMContext);
    b.submitApplicationAttempt(app0, user0);

    // Setup some nodes
    String host0 = "127.0.0.1";
    FiCaSchedulerNode node0 =
        TestUtils.getMockNode(host0, DEFAULT_RACK, 0, 80 * GB, 100);

    // Make cluster relatively large so usageRatios are small
    int numNodes = 1000;
    Resource clusterResource =
        Resources.createResource(numNodes * (80 * GB), numNodes * 100);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Set user-limit. Need a small queue within a large cluster.
    b.setUserLimit(50);
    b.setUserLimitFactor(1000000);
    b.setMaxCapacity(1.0f);
    b.setAbsoluteCapacity(0.00001f);

    // First allocation is larger than second but is still vcore dominant
    // so usage ratio will be based on vcores. If consumedRatio doesn't round
    // in our favor then new limit calculation will actually be less than
    // what is currently consumed and we will fail to allocate
    Priority priority = TestUtils.createMockPriority(1);
    app0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 20 * GB, 29, 1, true,
            priority, recordFactory, RMNodeLabelsManager.NO_LABEL)));
    assign = b.assignContainers(clusterResource, node0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    app0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 10 * GB, 29, 2, true,
            priority, recordFactory, RMNodeLabelsManager.NO_LABEL)));
    assign = b.assignContainers(clusterResource, node0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertTrue("Still within limits, should assign",
        assign.getResource().getMemorySize() > 0);
  }

  private void applyCSAssignment(Resource clusterResource, CSAssignment assign,
      LeafQueue q, final Map<NodeId, FiCaSchedulerNode> nodes,
      final Map<ApplicationAttemptId, FiCaSchedulerApp> apps)
      throws IOException {
    TestUtils.applyResourceCommitRequest(clusterResource, assign, nodes, apps);
  }

  @Test
  public void testDRFUserLimits() throws Exception {
    setUpWithDominantResourceCalculator();

    // Mock the queue
    LeafQueue b = stubLeafQueue((LeafQueue) queues.get(B));
    // unset maxCapacity
    b.setMaxCapacity(1.0f);

    // Users
    final String user0 = "user_0";
    final String user1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app0 =
        new FiCaSchedulerApp(appAttemptId0, user0, b,
            b.getAbstractUsersManager(), spyRMContext);
    b.submitApplicationAttempt(app0, user0);

    final ApplicationAttemptId appAttemptId2 =
        TestUtils.getMockApplicationAttemptId(2, 0);
    FiCaSchedulerApp app2 =
        new FiCaSchedulerApp(appAttemptId2, user1, b,
            b.getAbstractUsersManager(), spyRMContext);
    b.submitApplicationAttempt(app2, user1);

    // Setup some nodes
    String host0 = "127.0.0.1";
    FiCaSchedulerNode node0 =
        TestUtils.getMockNode(host0, DEFAULT_RACK, 0, 8 * GB, 100);
    String host1 = "127.0.0.2";
    FiCaSchedulerNode node1 =
        TestUtils.getMockNode(host1, DEFAULT_RACK, 0, 8 * GB, 100);

    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node0.getNodeID(),
        node0, node1.getNodeID(), node1);
    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app0.getApplicationAttemptId(), app0, app2.getApplicationAttemptId(),
        app2);

    int numNodes = 2;
    Resource clusterResource =
        Resources.createResource(numNodes * (8 * GB), numNodes * 100);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    when(csContext.getClusterResource()).thenReturn(clusterResource);

    // Setup resource-requests so that one application is memory dominant
    // and other application is vcores dominant
    Priority priority = TestUtils.createMockPriority(1);
    app0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 1 * GB, 40, 10, true,
            priority, recordFactory, RMNodeLabelsManager.NO_LABEL)));

    app2.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 10, 10, true,
            priority, recordFactory, RMNodeLabelsManager.NO_LABEL)));

    /**
     * Start testing...
     */

    // Set user-limit
    b.setUserLimit(50);
    b.setUserLimitFactor(2);
    User queueUser0 = b.getUser(user0);
    User queueUser1 = b.getUser(user1);

    assertEquals("There should 2 active users!", 2, b
        .getAbstractUsersManager().getNumActiveUsers());
    // Fill both Nodes as far as we can
    CSAssignment assign;
    do {
      assign =
          b.assignContainers(clusterResource, node0, new ResourceLimits(
              clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      LOG.info(assign.toString());
      applyCSAssignment(clusterResource, assign, b, nodes, apps);
    } while (assign.getResource().getMemorySize() > 0 &&
        assign.getAssignmentInformation().getNumReservations() == 0);
    do {
      assign =
          b.assignContainers(clusterResource, node1, new ResourceLimits(
              clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      applyCSAssignment(clusterResource, assign, b, nodes, apps);
    } while (assign.getResource().getMemorySize() > 0 &&
        assign.getAssignmentInformation().getNumReservations() == 0);

    assertTrue("Verify user_0 got resources ", queueUser0.getUsed()
        .getMemorySize() > 0);
    assertTrue("Verify user_1 got resources ", queueUser1.getUsed()
        .getMemorySize() > 0);
    assertTrue(
        "Expected AbsoluteUsedCapacity > 0.95, got: "
            + b.getAbsoluteUsedCapacity(), b.getAbsoluteUsedCapacity() > 0.95);

    // Verify consumedRatio is based on dominant resources
    float expectedRatio =
        queueUser0.getUsed().getVirtualCores()
            / (numNodes * 100.0f)
            + queueUser1.getUsed().getMemorySize()
            / (numNodes * 8.0f * GB);
    assertEquals(expectedRatio, b.getUsersManager().getUsageRatio(""), 0.001);
    // Add another node and make sure consumedRatio is adjusted
    // accordingly.
    numNodes = 3;
    clusterResource =
        Resources.createResource(numNodes * (8 * GB), numNodes * 100);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    expectedRatio =
        queueUser0.getUsed().getVirtualCores()
            / (numNodes * 100.0f)
            + queueUser1.getUsed().getMemorySize()
            / (numNodes * 8.0f * GB);
    assertEquals(expectedRatio, b.getUsersManager().getUsageRatio(""), 0.001);
  }

  @Test
  public void testUserLimits() throws Exception {
    // Mock the queue
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    //unset maxCapacity
    a.setMaxCapacity(1.0f);

    when(csContext.getClusterResource())
        .thenReturn(Resources.createResource(16 * GB, 32));

    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_1, a, 
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_1, user_1); // different user

    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    String host_1 = "127.0.0.2";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 8*GB);
    
    final int numNodes = 2;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
 
    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 3*GB, 2, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1);

    /**
     * Start testing...
     */
    
    // Set user-limit
    a.setUserLimit(50);
    a.setUserLimitFactor(2);
    
    // There're two active users
    assertEquals(2, a.getAbstractUsersManager().getNumActiveUsers());

    // 1 container to user_0
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(3*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());

    // Allocate one container to app_1. Even if app_0
    // submit earlier, it cannot get this container assigned since user_0
    // exceeded user-limit already. 
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(4*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());

    // Allocate one container to app_0, before allocating this container,
    // user-limit = ceil((4 + 1) / 2) = 3G. app_0's used resource (3G) <=
    // user-limit.
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(7*GB, a.getUsedResources().getMemorySize());
    assertEquals(6*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());

    // app_0 doesn't have outstanding resources, there's only one active user.
    assertEquals("There should only be 1 active user!", 
        1, a.getAbstractUsersManager().getNumActiveUsers());
  }

  @Test
  public void testUserSpecificUserLimits() throws Exception {
    // Mock the queue
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    // Set minimum-user-limit-percent for queue "a" in the configs.
    csConf.setUserLimit(a.getQueuePath(), 50);
    // Set weight for "user_0" to be 1.5 for the a queue in the configs.
    csConf.setFloat("yarn.scheduler.capacity." + a.getQueuePath()
        + ".user-settings.user_0." + CapacitySchedulerConfiguration.USER_WEIGHT,
        1.5f);

    when(csContext.getClusterResource())
        .thenReturn(Resources.createResource(16 * GB, 32));
    // Verify that configs were updated and parsed correctly.
    Assert.assertNull(a.getUserWeights().get("user_0"));
    a.reinitialize(a, csContext.getClusterResource());
    assertEquals(1.5, a.getUserWeights().get("user_0").floatValue(), 0.0);

    // set maxCapacity
    a.setMaxCapacity(1.0f);

    // Set minimum user-limit-percent
    a.setUserLimit(50);
    a.setUserLimitFactor(2);

    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Set user_0's weight to 1.5 in the a queue's object.
    a.getUsersManager().getUserAndAddIfAbsent(user_0).setWeight(1.5f);

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_1, a,
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_1, user_1); // different user

    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    String host_1 = "127.0.0.2";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 8*GB);

    final int numNodes = 2;
    Resource clusterResource =
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    // app_0 asks for 3 3-GB containers
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 4*GB, 3, true,
                priority, recordFactory)));

    // app_1 asks for 2 1-GB containers
    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1);

    /**
     * Start testing...
     */

    // There're two active users
    assertEquals(2, a.getAbstractUsersManager().getNumActiveUsers());

    // 1 container to user_0. Since queue starts out empty, user limit would
    // normally be calculated to be the minumum container size (1024GB).
    // However, in this case, user_0 has a weight of 1.5, so the UL is 2048GB
    // because 1024 * 1.5 rounded up to container size is 2048GB.
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(4*GB, a.getUsedResources().getMemorySize());
    assertEquals(4*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());

    // At this point the queue-wide user limit is 3072GB, but since user_0 has a
    // weight of 1.5, its user limit is 5120GB. So, even though user_0 already
    // has 4096GB, it is under its user limit, so it gets another container.
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(8*GB, a.getUsedResources().getMemorySize());
    assertEquals(8*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());

    // Queue-wide user limit at this point is 4069GB and user_0's user limit is
    // 6144GB. user_0 has 8192GB.
    // Now that user_0 is above its user limit, the next container should go to user_1
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(9*GB, a.getUsedResources().getMemorySize());
    assertEquals(8*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());

    assertEquals(4*GB,
        app_0.getTotalPendingRequestsPerPartition().get("").getMemorySize());

    assertEquals(1*GB,
        app_1.getTotalPendingRequestsPerPartition().get("").getMemorySize());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testComputeUserLimitAndSetHeadroom() throws IOException {
    LeafQueue qb = stubLeafQueue((LeafQueue)queues.get(B));
    qb.setMaxCapacity(1.0f);
    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    //create nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    String host_1 = "127.0.0.2";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 8*GB);

    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB), 1);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    CapacitySchedulerQueueManager mockCapacitySchedulerQueueManager
        = mock(CapacitySchedulerQueueManager.class);
    QueueStateManager mockQueueStateManager = mock(QueueStateManager.class);
    when(mockCapacitySchedulerQueueManager.getQueueStateManager()).thenReturn(
        mockQueueStateManager);
    when(csContext.getCapacitySchedulerQueueManager()).thenReturn(
        mockCapacitySchedulerQueueManager);

    //our test plan contains three cases
    //1. single user dominate the queue, we test the headroom
    //2. two users, but user_0 is assigned 100% of the queue resource,
    //   submit user_1's application, check headroom correctness
    //3. two users, each is assigned 50% of the queue resource
    //   each user submit one application and check their headrooms
    //4. similarly to 3. but user_0 has no quote left and there are
    //   free resources left, check headroom

    //test case 1
    qb.setUserLimit(100);
    qb.setUserLimitFactor(1);

    final ApplicationAttemptId appAttemptId_0 =
              TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, qb,
            qb.getAbstractUsersManager(), spyRMContext);
    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = new HashMap<>();
    apps.put(app_0.getApplicationAttemptId(), app_0);
    qb.submitApplicationAttempt(app_0, user_0);
    Priority u0Priority = TestUtils.createMockPriority(1);
    SchedulerRequestKey u0SchedKey = toSchedulerKey(u0Priority);
    app_0.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 4*GB, 1, true,
            u0Priority, recordFactory)));

    assertEquals("There should only be 1 active user!",
        1, qb.getAbstractUsersManager().getNumActiveUsers());
    //get headroom
    applyCSAssignment(clusterResource,
        qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), qb, nodes, apps);
    qb.computeUserLimitAndSetHeadroom(app_0, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, null);

    //maxqueue 16G, userlimit 13G, - 4G used = 9G
    assertEquals(9*GB,app_0.getHeadroom().getMemorySize());

    //test case 2
    final ApplicationAttemptId appAttemptId_2 =
        TestUtils.getMockApplicationAttemptId(2, 0);
    FiCaSchedulerApp app_2 =
        new FiCaSchedulerApp(appAttemptId_2, user_1, qb,
            qb.getAbstractUsersManager(), spyRMContext);
    apps.put(app_2.getApplicationAttemptId(), app_2);
    Priority u1Priority = TestUtils.createMockPriority(2);
    SchedulerRequestKey u1SchedKey = toSchedulerKey(u1Priority);
    app_2.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 4*GB, 1, true,
            u1Priority, recordFactory)));
    qb.submitApplicationAttempt(app_2, user_1);
    applyCSAssignment(clusterResource,
        qb.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), qb, nodes, apps);
    qb.computeUserLimitAndSetHeadroom(app_0, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, null);

    assertEquals(8*GB, qb.getUsedResources().getMemorySize());
    assertEquals(4*GB, app_0.getCurrentConsumption().getMemorySize());
    //maxqueue 16G, userlimit 13G, - 4G used = 9G BUT
    //maxqueue 16G - used 8G (4 each app/user) = 8G max headroom (the new logic)
    assertEquals(8*GB, app_0.getHeadroom().getMemorySize());
    assertEquals(4*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(8*GB, app_2.getHeadroom().getMemorySize());

    //test case 3
    qb.finishApplication(app_0.getApplicationId(), user_0);
    qb.finishApplication(app_2.getApplicationId(), user_1);
    qb.releaseResource(clusterResource, app_0, Resource.newInstance(4*GB, 1),
        null, null);
    qb.releaseResource(clusterResource, app_2, Resource.newInstance(4*GB, 1),
        null, null);

    qb.setUserLimit(50);
    qb.setUserLimitFactor(1);
    
    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, qb,
            qb.getAbstractUsersManager(), spyRMContext);
    apps.put(app_1.getApplicationAttemptId(), app_1);
    final ApplicationAttemptId appAttemptId_3 =
        TestUtils.getMockApplicationAttemptId(3, 0);
    FiCaSchedulerApp app_3 =
        new FiCaSchedulerApp(appAttemptId_3, user_1, qb,
            qb.getAbstractUsersManager(), spyRMContext);
    apps.put(app_3.getApplicationAttemptId(), app_3);
    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 2*GB, 1, true,
            u0Priority, recordFactory)));
    app_3.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 2*GB, 1, true,
             u1Priority, recordFactory)));
    qb.submitApplicationAttempt(app_1, user_0);
    qb.submitApplicationAttempt(app_3, user_1);
    applyCSAssignment(clusterResource,
        qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), qb, nodes, apps);
    applyCSAssignment(clusterResource,
        qb.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), qb, nodes, apps);
    qb.computeUserLimitAndSetHeadroom(app_3, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, null);
    assertEquals(4*GB, qb.getUsedResources().getMemorySize());
    //maxqueue 16G, userlimit 7G, used (by each user) 2G, headroom 5G (both)
    assertEquals(5*GB, app_3.getHeadroom().getMemorySize());
    assertEquals(5*GB, app_1.getHeadroom().getMemorySize());
    //test case 4
    final ApplicationAttemptId appAttemptId_4 =
              TestUtils.getMockApplicationAttemptId(4, 0);
    FiCaSchedulerApp app_4 =
              new FiCaSchedulerApp(appAttemptId_4, user_0, qb,
                      qb.getAbstractUsersManager(), spyRMContext);
    apps.put(app_4.getApplicationAttemptId(), app_4);
    qb.submitApplicationAttempt(app_4, user_0);
    app_4.updateResourceRequests(Collections.singletonList(
              TestUtils.createResourceRequest(ResourceRequest.ANY, 6*GB, 1, true,
                      u0Priority, recordFactory)));
    applyCSAssignment(clusterResource,
        qb.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), qb, nodes, apps);
    qb.computeUserLimitAndSetHeadroom(app_4, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, null);
    qb.computeUserLimitAndSetHeadroom(app_3, clusterResource,
        "", SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, null);
    
    
    //app3 is user1, active from last test case
    //maxqueue 16G, userlimit 13G, used 2G, would be headroom 10G BUT
    //10G in use, so max possible headroom is 6G (new logic)
    assertEquals(6*GB, app_3.getHeadroom().getMemorySize());
    //testcase3 still active - 2+2+6=10
    assertEquals(10*GB, qb.getUsedResources().getMemorySize());
    //app4 is user 0
    //maxqueue 16G, userlimit 7G, used 8G, headroom 5G
    //(8G used is 6G from this test case - app4, 2 from last test case, app_1)
    assertEquals(1*GB, app_4.getHeadroom().getMemorySize());
  }

  @Test
  public void testUserHeadroomMultiApp() throws Exception {
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
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, a,
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);  // same user

    final ApplicationAttemptId appAttemptId_2 =
        TestUtils.getMockApplicationAttemptId(2, 0);
    FiCaSchedulerApp app_2 =
        new FiCaSchedulerApp(appAttemptId_2, user_1, a,
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_2, user_1);

    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 
      0, 16*GB);
    String host_1 = "127.0.0.2";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 
      0, 16*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1, app_2.getApplicationAttemptId(), app_2);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (16*GB), 1);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    Priority priority = TestUtils.createMockPriority(1);

    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true,
                priority, recordFactory)));

    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(1*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    //Now, headroom is the same for all apps for a given user + queue combo
    //and a change to any app's headroom is reflected for all the user's apps
    //once those apps are active/have themselves calculated headroom for 
    //allocation at least one time
    assertEquals(2*GB, app_0.getHeadroom().getMemorySize());
    assertEquals(0*GB, app_1.getHeadroom().getMemorySize());//not yet active
    assertEquals(0*GB, app_2.getHeadroom().getMemorySize());//not yet active

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_0.getHeadroom().getMemorySize());
    assertEquals(1*GB, app_1.getHeadroom().getMemorySize());//now active
    assertEquals(0*GB, app_2.getHeadroom().getMemorySize());//not yet active

    //Complete container and verify that headroom is updated, for both apps 
    //for the user
    RMContainer rmContainer = app_0.getLiveContainers().iterator().next();
    a.completedContainer(clusterResource, app_0, node_0, rmContainer,
    ContainerStatus.newInstance(rmContainer.getContainerId(),
	ContainerState.COMPLETE, "",
	ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
    RMContainerEventType.KILL, null, true);

    assertEquals(2*GB, app_0.getHeadroom().getMemorySize());
    assertEquals(2*GB, app_1.getHeadroom().getMemorySize());
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
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);  // same user

    final ApplicationAttemptId appAttemptId_2 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_2 = 
        new FiCaSchedulerApp(appAttemptId_2, user_1, a, 
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_2, user_1);

    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    String host_1 = "127.0.0.2";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 8*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1, app_2.getApplicationAttemptId(), app_2);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1);
    
    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB), 1);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
 
    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 2*GB, 1, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    /**
     * Start testing...
     */
    
    // Set user-limit
    a.setUserLimit(50);
    a.setUserLimitFactor(2);

    // Now, only user_0 should be active since he is the only one with
    // outstanding requests
    assertEquals("There should only be 1 active user!", 
        1, a.getAbstractUsersManager().getNumActiveUsers());

    // 1 container to user_0
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    // TODO, fix headroom in the future patch
    assertEquals(1*GB, app_0.getHeadroom().getMemorySize());
      // User limit = 2G, 2 in use
    assertEquals(0*GB, app_1.getHeadroom().getMemorySize());
      // the application is not yet active

    // Again one to user_0 since he hasn't exceeded user limit yet
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(3*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_0.getHeadroom().getMemorySize()); // 4G - 3G
    assertEquals(1*GB, app_1.getHeadroom().getMemorySize()); // 4G - 3G
    
    // Submit requests for app_1 and set max-cap
    a.setMaxCapacity(.1f);
    app_2.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true,
            priority, recordFactory)));
    assertEquals(2, a.getAbstractUsersManager().getNumActiveUsers());

    // No more to user_0 since he is already over user-limit
    // and no more containers to queue since it's already at max-cap
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(3*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_0.getHeadroom().getMemorySize());
    assertEquals(0*GB, app_1.getHeadroom().getMemorySize());
    
    // Check headroom for app_2 
    app_1.updateResourceRequests(Collections.singletonList(     // unset
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 0, true,
            priority, recordFactory)));
    assertEquals(1, a.getAbstractUsersManager().getNumActiveUsers());
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(0*GB, app_2.getHeadroom().getMemorySize());   // hit queue max-cap
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
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_0, a, 
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);  // same user

    final ApplicationAttemptId appAttemptId_2 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_2 = 
        new FiCaSchedulerApp(appAttemptId_2, user_1, a, 
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_2, user_1);

    final ApplicationAttemptId appAttemptId_3 = 
        TestUtils.getMockApplicationAttemptId(3, 0); 
    FiCaSchedulerApp app_3 = 
        new FiCaSchedulerApp(appAttemptId_3, user_2, a, 
            a.getAbstractUsersManager(), spyRMContext);
    a.submitApplicationAttempt(app_3, user_2);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1, app_2.getApplicationAttemptId(), app_2,
        app_3.getApplicationAttemptId(), app_3);
    
    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8*GB);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0);
    
    final int numNodes = 1;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    when(csContext.getClusterResource()).thenReturn(clusterResource);

    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 10, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 10, true,
            priority, recordFactory)));

    /** 
     * Start testing... 
     */
    
    // Only 1 container
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(1*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    
    // Can't allocate 3rd due to user-limit
    a.setUserLimit(25);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    
    // Submit resource requests for other apps now to 'activate' them
    
    app_2.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 3*GB, 1, true,
            priority, recordFactory)));

    app_3.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    // Now allocations should goto app_2 since 
    // user_0 is at limit inspite of high user-limit-factor
    a.setUserLimitFactor(10);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(5*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());

    // Now allocations should goto app_0 since 
    // user_0 is at user-limit not above it
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(6*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());
    
    // Test max-capacity
    // Now - no more allocs since we are at max-cap
    a.setMaxCapacity(0.5f);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(6*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());
    
    // Revert max-capacity and user-limit-factor
    // Now, allocations should goto app_3 since it's under user-limit 
    a.setMaxCapacity(1.0f);
    a.setUserLimitFactor(1);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(7*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_3.getCurrentConsumption().getMemorySize());

    // Now we should assign to app_3 again since user_2 is under user-limit
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(8*GB, a.getUsedResources().getMemorySize());
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemorySize());

    // 8. Release each container from app_0
    for (RMContainer rmContainer : app_0.getLiveContainers()) {
      a.completedContainer(clusterResource, app_0, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }
    assertEquals(5*GB, a.getUsedResources().getMemorySize());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(3*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemorySize());
    
    // 9. Release each container from app_2
    for (RMContainer rmContainer : app_2.getLiveContainers()) {
      a.completedContainer(clusterResource, app_2, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(2*GB, app_3.getCurrentConsumption().getMemorySize());

    // 10. Release each container from app_3
    for (RMContainer rmContainer : app_3.getLiveContainers()) {
      a.completedContainer(clusterResource, app_3, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }
    assertEquals(0*GB, a.getUsedResources().getMemorySize());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());
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
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_1, a, 
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_1, user_1);  

    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 4*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0);
    
    final int numNodes = 2;
    Resource clusterResource = 
        Resources.createResource(numNodes * (4*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 4*GB, 1, true,
            priority, recordFactory)));

    // Start testing...
    
    // Only 1 container
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(1*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());
    assertEquals(0*GB, a.getMetrics().getAvailableMB());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Now, reservation should kick in for app_1
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(6*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(2*GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(4*GB, a.getMetrics().getReservedMB());
    assertEquals(2*GB, a.getMetrics().getAllocatedMB());
    
    // Now free 1 container from app_0 i.e. 1G
    RMContainer rmContainer = app_0.getLiveContainers().iterator().next();
    a.completedContainer(clusterResource, app_0, node_0, rmContainer,
        ContainerStatus.newInstance(rmContainer.getContainerId(),
            ContainerState.COMPLETE, "",
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(5*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(1*GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(4*GB, a.getMetrics().getReservedMB());
    assertEquals(1*GB, a.getMetrics().getAllocatedMB());

    // Now finish another container from app_0 and fulfill the reservation
    rmContainer = app_0.getLiveContainers().iterator().next();
    a.completedContainer(clusterResource, app_0, node_0, rmContainer,
        ContainerStatus.newInstance(rmContainer.getContainerId(),
            ContainerState.COMPLETE, "",
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(4*GB, a.getUsedResources().getMemorySize());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(4*GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0*GB, a.getMetrics().getReservedMB());
    assertEquals(4*GB, a.getMetrics().getAllocatedMB());
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
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 = 
        new FiCaSchedulerApp(appAttemptId_1, user_1, a, 
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_1, user_1);  

    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 4*GB);
    
    String host_1 = "127.0.0.2";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0, 4*GB);
    
    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getClusterResource()).thenReturn(Resource.newInstance(8, 1));

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1);
    
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
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 4*GB, 1, true,
            priority, recordFactory)));

    // Start testing...
    
    // Only 1 container
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(1*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());

    // Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
    // you can get one container more than user-limit
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(2*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    
    // Now, reservation should kick in for app_1
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(6*GB, a.getUsedResources().getMemorySize());
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(2*GB, node_0.getAllocatedResource().getMemorySize());
    
    // Now free 1 container from app_0 i.e. 1G, and re-reserve it
    RMContainer rmContainer = app_0.getLiveContainers().iterator().next();
    a.completedContainer(clusterResource, app_0, node_0, rmContainer,
        ContainerStatus.newInstance(rmContainer.getContainerId(),
            ContainerState.COMPLETE, "",
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(5*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(1*GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(1, app_1.getReReservations(toSchedulerKey(priority)));

    // Re-reserve
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(5*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(1*GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(2, app_1.getReReservations(toSchedulerKey(priority)));
    
    // Try to schedule on node_1 now, should *move* the reservation
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(9*GB, a.getUsedResources().getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(4*GB, node_1.getAllocatedResource().getMemorySize());
    // Doesn't change yet... only when reservation is cancelled or a different
    // container is reserved
    assertEquals(2, app_1.getReReservations(toSchedulerKey(priority)));
    
    // Now finish another container from app_0 and see the reservation cancelled
    rmContainer = app_0.getLiveContainers().iterator().next();
    a.completedContainer(clusterResource, app_0, node_0, rmContainer,
        ContainerStatus.newInstance(rmContainer.getContainerId(),
            ContainerState.COMPLETE, "",
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL, null, true);
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    assertEquals(4*GB, a.getUsedResources().getMemorySize());
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(4*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentReservation().getMemorySize());
    assertEquals(0*GB, node_0.getAllocatedResource().getMemorySize());
  }
  
  private void verifyContainerAllocated(CSAssignment assignment, NodeType nodeType) {
    Assert.assertTrue(Resources.greaterThan(resourceCalculator, null,
        assignment.getResource(), Resources.none()));
    Assert
        .assertTrue(assignment.getAssignmentInformation().getNumAllocations() > 0);
    Assert.assertEquals(nodeType, assignment.getType());
  }

  private void verifyNoContainerAllocated(CSAssignment assignment) {
    Assert.assertTrue(Resources.equals(assignment.getResource(),
        Resources.none()));
    Assert
        .assertTrue(assignment.getAssignmentInformation().getNumAllocations() == 0);
  }

  @Test
  public void testLocalityScheduling() throws Exception {

    // Manipulate queue 'b'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(B));

    // User
    String user_0 = "user_0";
    
    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    // Setup some nodes and racks
    String host_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 8*GB);
    
    String host_1 = "127.0.0.2";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, rack_1, 0, 8*GB);
    
    String host_2 = "127.0.0.3";
    String rack_2 = "rack_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, rack_2, 0, 8*GB);

    String host_3 = "127.0.0.4"; // on rack_1
    FiCaSchedulerNode node_3 = TestUtils.getMockNode(host_3, rack_1, 0, 8*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2,
        node_3.getNodeID(), node_3);

    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests and submit
    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 3, // one extra
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // Start testing...
    CSAssignment assignment = null;

    SchedulerRequestKey schedulerKey = toSchedulerKey(priority);
    // Start with off switch, shouldn't allocate due to delay scheduling
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(1, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app_0.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL

    // Another off switch, shouldn't allocate due to delay scheduling
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(2, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app_0.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    
    // Another off switch, shouldn't allocate due to delay scheduling
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(3, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app_0.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    
    // Another off switch, now we should allocate 
    // since missedOpportunities=3 and reqdContainers=3
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.OFF_SWITCH);
    // should NOT reset
    assertEquals(4, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(2, app_0.getOutstandingAsksCount(schedulerKey));
    
    // NODE_LOCAL - node_0
    assignment = a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    // should reset
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey));
    
    // NODE_LOCAL - node_1
    assignment = a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    // should reset
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType());
    
    // Add 1 more request to check for RACK_LOCAL
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 3, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 4, // one extra
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    assertEquals(4, app_0.getOutstandingAsksCount(schedulerKey));
    
    // Rack-delay
    doReturn(true).when(a).getRackLocalityFullReset();
    doReturn(1).when(a).getNodeLocalityDelay();
    
    // Shouldn't assign RACK_LOCAL yet
    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(1, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(4, app_0.getOutstandingAsksCount(schedulerKey));

    // Should assign RACK_LOCAL now
    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.RACK_LOCAL);
    // should reset
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app_0.getOutstandingAsksCount(schedulerKey));
    
    // Shouldn't assign RACK_LOCAL because schedulingOpportunities should have gotten reset.
    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(1, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app_0.getOutstandingAsksCount(schedulerKey));

    // Next time we schedule RACK_LOCAL, don't reset
    doReturn(false).when(a).getRackLocalityFullReset();

    // Should assign RACK_LOCAL now
    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.RACK_LOCAL);
    // should NOT reset
    assertEquals(2, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(2, app_0.getOutstandingAsksCount(schedulerKey));

    // Another RACK_LOCAL since schedulingOpportunities not reset
    assignment = a.assignContainers(clusterResource, node_3,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.RACK_LOCAL);
    // should NOT reset
    assertEquals(3, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey));
    
    // Add a request larger than cluster size to verify
    // OFF_SWITCH delay is capped by cluster size
    app_0.resetSchedulingOpportunities(schedulerKey);
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0, 1*GB, 100,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 100,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 100,
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // Start with off switch. 3 nodes in cluster so shouldn't allocate first 3
    for (int i = 0; i < numNodes; i++) {
      assignment =
          a.assignContainers(clusterResource, node_2, new ResourceLimits(
              clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      applyCSAssignment(clusterResource, assignment, a, nodes, apps);
      verifyNoContainerAllocated(assignment);
      assertEquals(i+1, app_0.getSchedulingOpportunities(schedulerKey));
    }
    // delay should be capped at numNodes so next one should allocate
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.OFF_SWITCH);
    assertEquals(numNodes+1, app_0.getSchedulingOpportunities(schedulerKey));
  }

  @Test
  public void testRackLocalityDelayScheduling() throws Exception {

    // Change parameter values for node locality and rack locality delay.
    csConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 2);
    csConf.setInt(
        CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY, 1);
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot = CapacitySchedulerQueueManager.parseQueue(csContext,
        csConf, null, CapacitySchedulerConfiguration.ROOT, newQueues, queues,
        TestUtils.spyHook);
    queues = newQueues;
    root.reinitialize(newRoot, cs.getClusterResource());

    // Manipulate queue 'b'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(B));

    // Check locality parameters.
    assertEquals(2, a.getNodeLocalityDelay());
    assertEquals(1, a.getRackLocalityAdditionalDelay());

    // User
    String user1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId1 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app1 = new FiCaSchedulerApp(appAttemptId1, user1, a,
        mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app1, user1);

    // Setup some nodes and racks
    String host1 = "127.0.0.1";
    String host2 = "127.0.0.2";
    String host3 = "127.0.0.3";
    String host4 = "127.0.0.4";
    String rack1 = "rack_1";
    String rack2 = "rack_2";
    String rack3 = "rack_3";
    FiCaSchedulerNode node2 = TestUtils.getMockNode(host3, rack2, 0, 8 * GB);
    FiCaSchedulerNode node3 = TestUtils.getMockNode(host4, rack3, 0, 8 * GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps =
        ImmutableMap.of(app1.getApplicationAttemptId(), app1);
    Map<NodeId, FiCaSchedulerNode> nodes =
        ImmutableMap.of(node2.getNodeID(), node2, node3.getNodeID(), node3);

    final int numNodes = 5;
    Resource clusterResource =
        Resources.createResource(numNodes * (8 * GB), numNodes * 16);
    when(spyRMContext.getScheduler().getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests and submit
    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app1Requests1 = new ArrayList<ResourceRequest>();
    app1Requests1.add(TestUtils.createResourceRequest(host1, 1 * GB, 1,
        true, priority, recordFactory));
    app1Requests1.add(TestUtils.createResourceRequest(rack1, 1 * GB, 1,
        true, priority, recordFactory));
    app1Requests1.add(TestUtils.createResourceRequest(host2, 1 * GB, 1,
        true, priority, recordFactory));
    app1Requests1.add(TestUtils.createResourceRequest(rack2, 1 * GB, 1,
        true, priority, recordFactory));
    // Adding one extra in the ANY.
    app1Requests1.add(TestUtils.createResourceRequest(ResourceRequest.ANY,
        1 * GB, 3, true, priority, recordFactory));
    app1.updateResourceRequests(app1Requests1);

    // Start testing...
    CSAssignment assignment = null;

    SchedulerRequestKey schedulerKey = toSchedulerKey(priority);
    assertEquals(3, app1.getOutstandingAsksCount(schedulerKey));

    // No rack-local yet.
    assignment = a.assignContainers(clusterResource, node2,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(1, app1.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app1.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL

    // Still no rack-local.
    assignment = a.assignContainers(clusterResource, node2,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(2, app1.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app1.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL

    // Rack local now.
    assignment = a.assignContainers(clusterResource, node2,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(0, app1.getSchedulingOpportunities(schedulerKey));
    assertEquals(2, app1.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.RACK_LOCAL, assignment.getType());

    // No off-switch until 3 missed opportunities.
    a.assignContainers(clusterResource, node3,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    a.assignContainers(clusterResource, node3,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assignment = a.assignContainers(clusterResource, node3,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(3, app1.getSchedulingOpportunities(schedulerKey));
    assertEquals(2, app1.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL

    // Now off-switch should succeed.
    assignment = a.assignContainers(clusterResource, node3,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(4, app1.getSchedulingOpportunities(schedulerKey));
    assertEquals(1, app1.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.OFF_SWITCH, assignment.getType());

    // Check capping by number of cluster nodes.
    doReturn(10).when(a).getRackLocalityAdditionalDelay();
    // Off-switch will happen at 6 missed opportunities now, since cluster size
    // is 5.
    assignment = a.assignContainers(clusterResource, node3,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(5, app1.getSchedulingOpportunities(schedulerKey));
    assertEquals(1, app1.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType()); // None->NODE_LOCAL
    assignment = a.assignContainers(clusterResource, node3,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    assertEquals(6, app1.getSchedulingOpportunities(schedulerKey));
    assertEquals(0, app1.getOutstandingAsksCount(schedulerKey));
    assertEquals(NodeType.OFF_SWITCH, assignment.getType());
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
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);
    
    // Setup some nodes and racks
    String host_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 8*GB);
    
    String host_1 = "127.0.0.2";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, rack_1, 0, 8*GB);
    
    String host_2 = "127.0.0.3";
    String rack_2 = "rack_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, rack_2, 0, 8*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), 1);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests and submit
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    
    // P1
    Priority priority_1 = TestUtils.createMockPriority(1);
    SchedulerRequestKey schedulerKey1 = toSchedulerKey(priority_1);
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0, 1*GB, 1, 
            true, priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1, 
            true, priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            true, priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            true, priority_1, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2,
            true, priority_1, recordFactory));
    
    // P2
    Priority priority_2 = TestUtils.createMockPriority(2);
    SchedulerRequestKey schedulerKey2 = toSchedulerKey(priority_2);
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_2, 2*GB, 1, 
            true, priority_2, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_2, 2*GB, 1, 
            true, priority_2, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 2*GB, 1,
            true, priority_2, recordFactory));
    
    app_0.updateResourceRequests(app_0_requests_0);

    // Start testing...
    
    // Start with off switch, shouldn't allocate P1 due to delay scheduling
    // thus, no P2 either!
    CSAssignment assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(1, app_0.getSchedulingOpportunities(schedulerKey1));
    assertEquals(2, app_0.getOutstandingAsksCount(schedulerKey1));
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey2));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey2));

    // Another off-switch, shouldn't allocate P1 due to delay scheduling
    // thus, no P2 either!
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(2, app_0.getSchedulingOpportunities(schedulerKey1));
    assertEquals(2, app_0.getOutstandingAsksCount(schedulerKey1));
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey2));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey2));

    // Another off-switch, shouldn't allocate OFF_SWITCH P1
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.OFF_SWITCH);
    assertEquals(3, app_0.getSchedulingOpportunities(schedulerKey1));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey1));
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey2));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey2));

    // Now, DATA_LOCAL for P1
    assignment = a.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey1));
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey1));
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey2));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey2));

    // Now, OFF_SWITCH for P2
    assignment = a.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.OFF_SWITCH);
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey1));
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey1));
    assertEquals(1, app_0.getSchedulingOpportunities(schedulerKey2));
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey2));

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
        new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);
    
    // Setup some nodes and racks
    String host_0_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0_0 = TestUtils.getMockNode(host_0_0, rack_0, 0, 8*GB);
    String host_0_1 = "127.0.0.2";
    FiCaSchedulerNode node_0_1 = TestUtils.getMockNode(host_0_1, rack_0, 0, 8*GB);
    
    
    String host_1_0 = "127.0.0.3";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1_0 = TestUtils.getMockNode(host_1_0, rack_1, 0, 8*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0_0.getNodeID(),
        node_0_0, node_0_1.getNodeID(), node_0_1, node_1_0.getNodeID(),
        node_1_0);
    
    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(
        numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests and submit
    Priority priority = TestUtils.createMockPriority(1);
    SchedulerRequestKey schedulerKey = toSchedulerKey(priority);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // Start testing...
    
    // Add one request
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, // only 1
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    
    // NODE_LOCAL - node_0_1
    CSAssignment assignment = a.assignContainers(clusterResource, node_0_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    // should reset
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey));

    // No allocation on node_1_0 even though it's node/rack local since
    // required(ANY) == 0
    assignment = a.assignContainers(clusterResource, node_1_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    // Still zero
    // since #req=0
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey));
    
    // Add one request
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, // only one
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // No allocation on node_0_1 even though it's node/rack local since
    // required(rack_1) == 0
    assignment = a.assignContainers(clusterResource, node_0_1,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(1, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey));
    
    // NODE_LOCAL - node_1
    assignment = a.assignContainers(clusterResource, node_1_0,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    // should reset
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey));
  }

  @Test (timeout = 30000)
  public void testActivateApplicationAfterQueueRefresh() throws Exception {

    // Manipulate queue 'e'
    LeafQueue e = stubLeafQueue((LeafQueue)queues.get(E));

    // Users
    final String user_e = "user_e";
    
    when(amResourceRequest.getCapability()).thenReturn(
      Resources.createResource(1 * GB, 0));

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_e, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_0, user_e);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_e, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_1, user_e);  // same user

    final ApplicationAttemptId appAttemptId_2 =
        TestUtils.getMockApplicationAttemptId(2, 0);
    FiCaSchedulerApp app_2 =
        new FiCaSchedulerApp(appAttemptId_2, user_e, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_2, user_e);  // same user

    // before reinitialization
    assertEquals(2, e.getNumActiveApplications());
    assertEquals(1, e.getNumPendingApplications());

    csConf.setDouble(CapacitySchedulerConfiguration
        .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
        CapacitySchedulerConfiguration
        .DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT * 2);
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot =
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT,
            newQueues, queues,
            TestUtils.spyHook);
    queues = newQueues;
    root.reinitialize(newRoot, csContext.getClusterResource());

    // after reinitialization
    assertEquals(3, e.getNumActiveApplications());
    assertEquals(0, e.getNumPendingApplications());
  }
  
  @Test (timeout = 30000)
  public void testLocalityDelaysAfterQueueRefresh() throws Exception {

    // Manipulate queue 'e'
    LeafQueue e = stubLeafQueue((LeafQueue)queues.get(E));

    // before reinitialization
    assertEquals(40, e.getNodeLocalityDelay());
    assertEquals(-1, e.getRackLocalityAdditionalDelay());

    csConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 60);
    csConf.setInt(
        CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY, 600);
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot =
        CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT,
            newQueues, queues,
            TestUtils.spyHook);
    queues = newQueues;
    root.reinitialize(newRoot, cs.getClusterResource());

    // after reinitialization
    assertEquals(60, e.getNodeLocalityDelay());
    assertEquals(600, e.getRackLocalityAdditionalDelay());
  }

  @Test (timeout = 30000)
  public void testActivateApplicationByUpdatingClusterResource()
      throws Exception {

    // Manipulate queue 'e'
    LeafQueue e = stubLeafQueue((LeafQueue)queues.get(E));

    // Users
    final String user_e = "user_e";
    
    when(amResourceRequest.getCapability()).thenReturn(
      Resources.createResource(1 * GB, 0));

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_e, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_0, user_e);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_e, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_1, user_e);  // same user

    final ApplicationAttemptId appAttemptId_2 =
        TestUtils.getMockApplicationAttemptId(2, 0);
    FiCaSchedulerApp app_2 =
        new FiCaSchedulerApp(appAttemptId_2, user_e, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_2, user_e);  // same user

    // before updating cluster resource
    assertEquals(2, e.getNumActiveApplications());
    assertEquals(1, e.getNumPendingApplications());

    Resource clusterResource = Resources.createResource(200 * 16 * GB, 100 * 32); 
    e.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // after updating cluster resource
    assertEquals(3, e.getNumActiveApplications());
    assertEquals(0, e.getNumPendingApplications());
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

  @Test
  public void testLocalityConstraints() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    // User
    String user_0 = "user_0";
    
    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);

    // Setup some nodes and racks
    String host_0_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    String host_0_1 = "127.0.0.2";
    FiCaSchedulerNode node_0_1 = TestUtils.getMockNode(host_0_1, rack_0, 0, 8*GB);

    String host_1_0 = "127.0.0.3";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1_0 = TestUtils.getMockNode(host_1_0, rack_1, 0, 8*GB);
    String host_1_1 = "127.0.0.4";
    FiCaSchedulerNode node_1_1 = TestUtils.getMockNode(host_1_1, rack_1, 0, 8*GB);

    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0_1.getNodeID(),
        node_0_1, node_1_0.getNodeID(), node_1_0, node_1_1.getNodeID(),
        node_1_1);
    
    final int numNodes = 4;
    Resource clusterResource = Resources.createResource(
        numNodes * (8*GB), numNodes * 1);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    // resourceName: <priority, memory, #containers, relaxLocality>
    // host_0_0: < 1, 1GB, 1, true >
    // host_0_1: < null >
    // rack_0:   < null >                     <----
    // host_1_0: < 1, 1GB, 1, true >
    // host_1_1: < null >
    // rack_1:   < 1, 1GB, 1, false >         <----
    // ANY:      < 1, 1GB, 1, false >         <----
    // Availability:
    // host_0_0: 8G
    // host_0_1: 8G
    // host_1_0: 8G
    // host_1_1: 8G
    // Blacklist: <host_0_0>
    Priority priority = TestUtils.createMockPriority(1);
    SchedulerRequestKey schedulerKey = toSchedulerKey(priority);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            false, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, // only one
            false, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    app_0.updateBlacklist(Collections.singletonList(host_0_0), null);
    app_0_requests_0.clear();

    //
    // Start testing...
    //
    
    // node_0_1  
    // Shouldn't allocate since RR(rack_0) = null && RR(ANY) = relax: false
    CSAssignment assignment =
        a.assignContainers(clusterResource, node_0_1, new ResourceLimits(
            clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    // should be 0
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    
    // resourceName: <priority, memory, #containers, relaxLocality>
    // host_0_0: < 1, 1GB, 1, true >
    // host_0_1: < null >
    // rack_0:   < null >                     <----
    // host_1_0: < 1, 1GB, 1, true >
    // host_1_1: < null >
    // rack_1:   < 1, 1GB, 1, false >         <----
    // ANY:      < 1, 1GB, 1, false >         <----
    // Availability:
    // host_0_0: 8G
    // host_0_1: 8G
    // host_1_0: 8G
    // host_1_1: 8G
    // Blacklist: <host_0_0>

    // node_1_1  
    // Shouldn't allocate since RR(rack_1) = relax: false
    assignment = a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    // should be 0
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    
    // Allow rack-locality for rack_1, but blacklist node_1_1
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    app_0.updateBlacklist(Collections.singletonList(host_1_1), null);
    app_0_requests_0.clear();

    // resourceName: <priority, memory, #containers, relaxLocality>
    // host_0_0: < 1, 1GB, 1, true >
    // host_0_1: < null >
    // rack_0:   < null >                     
    // host_1_0: < 1, 1GB, 1, true >
    // host_1_1: < null >
    // rack_1:   < 1, 1GB, 1, true >         
    // ANY:      < 1, 1GB, 1, false >         
    // Availability:
    // host_0_0: 8G
    // host_0_1: 8G
    // host_1_0: 8G
    // host_1_1: 8G
    // Blacklist: < host_0_0 , host_1_1 >       <----

    // node_1_1  
    // Shouldn't allocate since node_1_1 is blacklisted
    assignment = a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    // should be 0
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));

    // Now, remove node_1_1 from blacklist, but add rack_1 to blacklist
    app_0.updateResourceRequests(app_0_requests_0);
    app_0.updateBlacklist(
        Collections.singletonList(rack_1), Collections.singletonList(host_1_1));
    app_0_requests_0.clear();

    // resourceName: <priority, memory, #containers, relaxLocality>
    // host_0_0: < 1, 1GB, 1, true >
    // host_0_1: < null >
    // rack_0:   < null >                     
    // host_1_0: < 1, 1GB, 1, true >
    // host_1_1: < null >
    // rack_1:   < 1, 1GB, 1, true >         
    // ANY:      < 1, 1GB, 1, false >         
    // Availability:
    // host_0_0: 8G
    // host_0_1: 8G
    // host_1_0: 8G
    // host_1_1: 8G
    // Blacklist: < host_0_0 , rack_1 >       <----

    // node_1_1  
    // Shouldn't allocate since rack_1 is blacklisted
    assignment = a.assignContainers(clusterResource, node_1_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    // should be 0
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    
    // Now remove rack_1 from blacklist
    app_0.updateResourceRequests(app_0_requests_0);
    app_0.updateBlacklist(null, Collections.singletonList(rack_1));
    app_0_requests_0.clear();
    
    // resourceName: <priority, memory, #containers, relaxLocality>
    // host_0_0: < 1, 1GB, 1, true >
    // host_0_1: < null >
    // rack_0:   < null >                     
    // host_1_0: < 1, 1GB, 1, true >
    // host_1_1: < null >
    // rack_1:   < 1, 1GB, 1, true >         
    // ANY:      < 1, 1GB, 1, false >         
    // Availability:
    // host_0_0: 8G
    // host_0_1: 8G
    // host_1_0: 8G
    // host_1_1: 8G
    // Blacklist: < host_0_0 >       <----

    // Now, should allocate since RR(rack_1) = relax: true
    assignment = a.assignContainers(clusterResource, node_1_1, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyNoContainerAllocated(assignment);
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(1, app_0.getOutstandingAsksCount(schedulerKey));

    // Now sanity-check node_local
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            false, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, // only one
            false, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    app_0_requests_0.clear();
    
    // resourceName: <priority, memory, #containers, relaxLocality>
    // host_0_0: < 1, 1GB, 1, true >
    // host_0_1: < null >
    // rack_0:   < null >                     
    // host_1_0: < 1, 1GB, 1, true >
    // host_1_1: < null >
    // rack_1:   < 1, 1GB, 1, false >          <----
    // ANY:      < 1, 1GB, 1, false >          <----
    // Availability:
    // host_0_0: 8G
    // host_0_1: 8G
    // host_1_0: 8G
    // host_1_1: 7G

    assignment = a.assignContainers(clusterResource, node_1_0, 
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(0, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(0, app_0.getOutstandingAsksCount(schedulerKey));

  }
  
  @Test
  public void testMaxAMResourcePerQueuePercentAfterQueueRefresh()
      throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    Resource clusterResource = Resources
        .createResource(100 * 16 * GB, 100 * 32);
    CapacitySchedulerContext csContext = mockCSContext(csConf, clusterResource);
    when(csContext.getRMContext()).thenReturn(rmContext);
    csConf.setFloat(CapacitySchedulerConfiguration.
        MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 0.1f);
    ParentQueue root = new ParentQueue(csContext, 
        CapacitySchedulerConfiguration.ROOT, null, null);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + A, 80);
    LeafQueue a = new LeafQueue(csContext, A, root, null);
    assertEquals(0.1f, a.getMaxAMResourcePerQueuePercent(), 1e-3f);
    assertEquals(a.calculateAndGetAMResourceLimit(),
        Resources.createResource(160 * GB, 1));
    
    csConf.setFloat(CapacitySchedulerConfiguration.
        MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 0.2f);
    LeafQueue newA = new LeafQueue(csContext, A, root, null);
    a.reinitialize(newA, clusterResource);
    assertEquals(0.2f, a.getMaxAMResourcePerQueuePercent(), 1e-3f);
    assertEquals(a.calculateAndGetAMResourceLimit(),
        Resources.createResource(320 * GB, 1));

    Resource newClusterResource = Resources.createResource(100 * 20 * GB,
        100 * 32);
    a.updateClusterResource(newClusterResource, 
        new ResourceLimits(newClusterResource));
    //  100 * 20 * 0.2 = 400
    assertEquals(a.calculateAndGetAMResourceLimit(),
        Resources.createResource(400 * GB, 1));
  }
  
  @Test
  public void testAllocateContainerOnNodeWithoutOffSwitchSpecified()
      throws Exception {
    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(B));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0); // same user

    // Setup some nodes
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 =
        TestUtils.getMockNode(host_0, DEFAULT_RACK, 0, 8 * GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0);

    final int numNodes = 1;
    Resource clusterResource =
        Resources.createResource(numNodes * (8 * GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Arrays.asList(TestUtils.createResourceRequest(
        "127.0.0.1", 1 * GB, 3, true, priority, recordFactory), TestUtils
        .createResourceRequest(DEFAULT_RACK, 1 * GB, 3, true, priority,
            recordFactory)));

    try {
      applyCSAssignment(clusterResource,
          a.assignContainers(clusterResource, node_0,
          new ResourceLimits(clusterResource),
          SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    } catch (NullPointerException e) {
      Assert.fail("NPE when allocating container on node but "
          + "forget to set off-switch request should be handled");
    }
  }

  @Test
  public void testFifoAssignment() throws Exception {

    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    a.setOrderingPolicy(new FifoOrderingPolicy<FiCaSchedulerApp>());

    String host_0_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0_0 = TestUtils.getMockNode(host_0_0, rack_0, 0,
        16 * GB);

    final int numNodes = 4;
    Resource clusterResource = Resources.createResource(numNodes * (16 * GB),
        numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    String user_0 = "user_0";

    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = spy(new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext, Priority.newInstance(3),
        false));
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = spy(new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext, Priority.newInstance(5),
        false));
    a.submitApplicationAttempt(app_1, user_0);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0_0.getNodeID(),
        node_0_0);

    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    List<ResourceRequest> app_1_requests_0 = new ArrayList<ResourceRequest>();

    app_0_requests_0.clear();
    app_0_requests_0.add(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true, priority,
            recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    app_1_requests_0.clear();
    app_1_requests_0.add(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 1 * GB, 1, true, priority,
            recordFactory));
    app_1.updateResourceRequests(app_1_requests_0);

    // app_1 will get containers as it has high priority
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(1 * GB, app_1.getCurrentConsumption().getMemorySize());
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());

    app_0_requests_0.clear();
    app_0_requests_0.add(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 1 * GB, 1, true, priority,
            recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    app_1_requests_0.clear();
    app_1_requests_0.add(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 1 * GB, 1, true, priority,
            recordFactory));
    app_1.updateResourceRequests(app_1_requests_0);

    //app_1 will still get assigned first as priority is more.
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(2 * GB, app_1.getCurrentConsumption().getMemorySize());
    Assert.assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());

    //and only then will app_2
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(3 * GB, app_0.getCurrentConsumption().getMemorySize());
  }

  @Test
  public void testConcurrentAccess() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM();
    rm.init(conf);
    rm.start();

    final String queue = "default";
    final String user = "user";
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    final LeafQueue defaultQueue = (LeafQueue) cs.getQueue(queue);

    final List<FiCaSchedulerApp> listOfApps =
        createListOfApps(10000, user, defaultQueue);

    final CyclicBarrier cb = new CyclicBarrier(2);
    final List<ConcurrentModificationException> conException =
        new ArrayList<ConcurrentModificationException>();

    Thread submitAndRemove = new Thread(new Runnable() {

      @Override
      public void run() {

        for (FiCaSchedulerApp fiCaSchedulerApp : listOfApps) {
          defaultQueue.submitApplicationAttempt(fiCaSchedulerApp, user);
        }
        try {
          cb.await();
        } catch (Exception e) {
          // Ignore
        }
        for (FiCaSchedulerApp fiCaSchedulerApp : listOfApps) {
          defaultQueue.finishApplicationAttempt(fiCaSchedulerApp, queue);
        }
      }
    }, "SubmitAndRemoveApplicationAttempt Thread");

    Thread getAppsInQueue = new Thread(new Runnable() {
      List<ApplicationAttemptId> apps = new ArrayList<ApplicationAttemptId>();

      @Override
      public void run() {
        try {
          try {
            cb.await();
          } catch (Exception e) {
            // Ignore
          }
          defaultQueue.collectSchedulerApplications(apps);
        } catch (ConcurrentModificationException e) {
          conException.add(e);
        }
      }

    }, "GetAppsInQueue Thread");

    submitAndRemove.start();
    getAppsInQueue.start();

    submitAndRemove.join();
    getAppsInQueue.join();

    assertTrue("ConcurrentModificationException is thrown",
        conException.isEmpty());
    rm.stop();

  }

  @Test
  public void testFairAssignment() throws Exception {

    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    OrderingPolicy<FiCaSchedulerApp> schedulingOrder =
      new FairOrderingPolicy<FiCaSchedulerApp>();

    a.setOrderingPolicy(schedulingOrder);

    String host_0_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0_0 = TestUtils.getMockNode(host_0_0, rack_0, 0, 16*GB);

    final int numNodes = 4;
    Resource clusterResource = Resources.createResource(
        numNodes * (16*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    String user_0 = "user_0";

    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        spy(new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext));
    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        spy(new FiCaSchedulerApp(appAttemptId_1, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext));
    a.submitApplicationAttempt(app_1, user_0);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0_0.getNodeID(),
        node_0_0);

    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    List<ResourceRequest> app_1_requests_0 = new ArrayList<ResourceRequest>();

    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 2*GB, 1,
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    app_1_requests_0.clear();
    app_1_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1,
            true, priority, recordFactory));
    app_1.updateResourceRequests(app_1_requests_0);

    // app_0 will get containers as its submitted first.
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());

    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1,
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    app_1_requests_0.clear();
    app_1_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1,
            true, priority, recordFactory));
    app_1.updateResourceRequests(app_1_requests_0);

    //Since it already has more resources, app_0 will not get
    //assigned first, but app_1 will
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    Assert.assertEquals(2*GB, app_1.getCurrentConsumption().getMemorySize());

    //and only then will app_0
    applyCSAssignment(clusterResource,
        a.assignContainers(clusterResource, node_0_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), a, nodes, apps);
    Assert.assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());

  }
  
  @Test
  public void testLocalityDelaySkipsApplication() throws Exception {

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));

    // User
    String user_0 = "user_0";
    
    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_0, user_0);
    final ApplicationAttemptId appAttemptId_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, a,
            mock(ActiveUsersManager.class), spyRMContext);
    a.submitApplicationAttempt(app_1, user_0);

    // Setup some nodes and racks
    String host_0 = "127.0.0.1";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 8*GB);
    
    String host_1 = "127.0.0.2";
    String rack_1 = "rack_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, rack_1, 0, 8*GB);
    
    String host_2 = "127.0.0.3";
    String rack_2 = "rack_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, rack_2, 0, 8*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    final int numNodes = 3;
    Resource clusterResource = 
        Resources.createResource(numNodes * (8*GB), numNodes * 16);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    
    // Setup resource-requests and submit
    // App0 has node local request for host_0/host_1, and app1 has node local
    // request for host2.
    Priority priority = TestUtils.createMockPriority(1);
    SchedulerRequestKey schedulerKey = toSchedulerKey(priority);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, 
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 3, // one extra
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    List<ResourceRequest> app_1_requests_0 = new ArrayList<ResourceRequest>();
    app_1_requests_0.add(
        TestUtils.createResourceRequest(host_2, 1*GB, 1, 
            true, priority, recordFactory));
    app_1_requests_0.add(
        TestUtils.createResourceRequest(rack_2, 1*GB, 1, 
            true, priority, recordFactory));
    app_1_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, // one extra
            true, priority, recordFactory));
    app_1.updateResourceRequests(app_1_requests_0);

    // Start testing...
    // When doing allocation, even if app_0 submit earlier than app_1, app_1 can
    // still get allocated because app_0 is waiting for node-locality-delay
    CSAssignment assignment = null;
    
    // Check app_0's scheduling opportunities increased and app_1 get allocated
    assignment = a.assignContainers(clusterResource, node_2,
        new ResourceLimits(clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    applyCSAssignment(clusterResource, assignment, a, nodes, apps);
    verifyContainerAllocated(assignment, NodeType.NODE_LOCAL);
    assertEquals(1, app_0.getSchedulingOpportunities(schedulerKey));
    assertEquals(3, app_0.getOutstandingAsksCount(schedulerKey));
    assertEquals(0, app_0.getLiveContainers().size());
    assertEquals(1, app_1.getLiveContainers().size());
  }

  @Test
  public void testGetTotalPendingResourcesConsideringUserLimitOneUser()
      throws Exception {
    // Manipulate queue 'e'
    LeafQueue e = stubLeafQueue((LeafQueue)queues.get(E));
    // Allow queue 'e' to use 100% of cluster resources (max capacity).
    e.setMaxCapacity(1.0f);
    // When used queue resources goes above capacity (in this case, 1%), user
    // resource limit (used in calculating headroom) is calculated in small
    // increments to ensure that user-limit-percent can be met for all users in
    // a queue. Take user-limit-percent out of the equation so that user
    // resource limit will always be calculated to its max possible value.
    e.setUserLimit(1000);

    final String user_0 = "user_0";

    // Submit 2 applications for user_0
    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_1, user_0);  // same user

    // Setup 1 node with 100GB of memory resources.
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        100*GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0);

    final int numNodes = 1;
    Resource clusterResource =
        Resources.createResource(numNodes * (100*GB), numNodes * 128);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Pending resource requests for app_0 and app_1 total 5GB.
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 3, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    // Start testing...

    // Assign 1st Container of 1GB
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // With queue capacity set at 1% of 100GB and user-limit-factor set to 1.0,
    // all users (only user_0) queue 'e' should be able to consume 1GB.
    // The first container should be assigned to app_0 with no headroom left
    // even though user_0's apps are still asking for a total of 4GB.
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB,
        e.getTotalPendingResourcesConsideringUserLimit(clusterResource,
            RMNodeLabelsManager.NO_LABEL, false).getMemorySize());

    // Assign 2nd container of 1GB
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // user_0 has no headroom due to user-limit-factor of 1.0. However capacity
    // scheduler will assign one container more than user-limit-factor.
    // This container also went to app_0. Still with no neadroom even though
    // app_0 and app_1 are asking for a cumulative 3GB.
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());

    // Can't allocate 3rd container due to user-limit. Headroom still 0.
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    assertEquals(2*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());

    // Increase user-limit-factor from 1GB to 10GB (1% * 10 * 100GB = 10GB).
    // Pending for both app_0 and app_1 are still 3GB, so user-limit-factor
    // is no longer limiting the return value of
    // getTotalPendingResourcesConsideringUserLimit()
    e.setUserLimitFactor(10.0f);
    assertEquals(3*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());

    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // app_0 is now satisified, app_1 is still asking for 2GB.
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(2*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());

    // Get the last 2 containers for app_1, no more pending requests.
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    assertEquals(3*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(2*GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());

    // Release each container from app_0
    for (RMContainer rmContainer : app_0.getLiveContainers()) {
      e.completedContainer(clusterResource, app_0, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }

    // Release each container from app_1
    for (RMContainer rmContainer : app_1.getLiveContainers()) {
      e.completedContainer(clusterResource, app_1, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }
  }

  @Test
  public void testGetTotalPendingResourcesConsideringUserLimitTwoUsers()
      throws Exception {
    // Manipulate queue 'e'
    LeafQueue e = stubLeafQueue((LeafQueue)queues.get(E));
    // Allow queue 'e' to use 100% of cluster resources (max capacity).
    e.setMaxCapacity(1.0f);
    // When used queue resources goes above capacity (in this case, 1%), user
    // resource limit (used in calculating headroom) is calculated in small
    // increments to ensure that user-limit-percent can be met for all users in
    // a queue. Take user-limit-percent out of the equation so that user
    // resource limit will always be calculated to its max possible value.
    e.setUserLimit(1000);

    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit 2 applications for user_0
    final ApplicationAttemptId appAttemptId_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 =
        new FiCaSchedulerApp(appAttemptId_0, user_0, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 =
        new FiCaSchedulerApp(appAttemptId_1, user_0, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_1, user_0);

    // Submit 2 applications for user_1
    final ApplicationAttemptId appAttemptId_2 =
        TestUtils.getMockApplicationAttemptId(2, 0);
    FiCaSchedulerApp app_2 =
        new FiCaSchedulerApp(appAttemptId_2, user_1, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_2, user_1);

    final ApplicationAttemptId appAttemptId_3 =
        TestUtils.getMockApplicationAttemptId(3, 0);
    FiCaSchedulerApp app_3 =
        new FiCaSchedulerApp(appAttemptId_3, user_1, e,
            mock(ActiveUsersManager.class), spyRMContext);
    e.submitApplicationAttempt(app_3, user_1);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1, app_2.getApplicationAttemptId(), app_2,
        app_3.getApplicationAttemptId(), app_3);

    // Setup 1 node with 100GB of memory resources.
    String host_0 = "127.0.0.1";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        100*GB);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0);

    final int numNodes = 1;
    Resource clusterResource =
        Resources.createResource(numNodes * (100*GB), numNodes * 128);
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Pending resource requests for user_0: app_0 and app_1 total 3GB.
    Priority priority = TestUtils.createMockPriority(1);
    app_0.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true,
                priority, recordFactory)));

    app_1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
            priority, recordFactory)));

    // Pending resource requests for user_1: app_2 and app_3 total 1GB.
    priority = TestUtils.createMockPriority(1);
    app_2.updateResourceRequests(Collections.singletonList(
            TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2, true,
                priority, recordFactory)));

    app_3.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true,
            priority, recordFactory)));

    // Start testing...
    // With queue capacity set at 1% of 100GB and user-limit-factor set to 1.0,
    // queue 'e' should be able to consume 1GB per user.
    assertEquals(2*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    // None of the apps have assigned resources
    // user_0's apps:
    assertEquals(0*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    // user_1's apps:
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());

    // Assign 1st Container of 1GB
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
   // The first container was assigned to user_0's app_0. Queues total headroom
    // has 1GB left for user_1.
    assertEquals(1*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    // user_0's apps:
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_1.getCurrentConsumption().getMemorySize());
    // user_1's apps:
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());

    // Assign 2nd container of 1GB
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // user_0 has no headroom due to user-limit-factor of 1.0. However capacity
    // scheduler will assign one container more than user-limit-factor. So,
    // this container went to user_0's app_1. so, headroom for queue 'e'e is
    // still 1GB for user_1
    assertEquals(1*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    // user_0's apps:
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    // user_1's apps:
    assertEquals(0*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());

    // Assign 3rd container.
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // Container was allocated to user_1's app_2 since user_1, Now, no headroom
    // is left.
    assertEquals(0*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    // user_0's apps:
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_1.getCurrentConsumption().getMemorySize());
    // user_1's apps:
    assertEquals(1*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());

    // Assign 4th container.
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // Allocated to user_1's app_2 since scheduler allocates 1 container
    // above user resource limit. Available headroom still 0.
    assertEquals(0*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    // user_0's apps:
    long app_0_consumption = app_0.getCurrentConsumption().getMemorySize();
    assertEquals(1*GB, app_0_consumption);
    long app_1_consumption = app_1.getCurrentConsumption().getMemorySize();
    assertEquals(1*GB, app_1_consumption);
    // user_1's apps:
    long app_2_consumption = app_2.getCurrentConsumption().getMemorySize();
    assertEquals(2*GB, app_2_consumption);
    long app_3_consumption = app_3.getCurrentConsumption().getMemorySize();
    assertEquals(0*GB, app_3_consumption);

    // Attempt to assign 5th container. Will be a no-op.
    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // Cannot allocate 5th container because both users are above their allowed
    // user resource limit. Values should be the same as previously.
    assertEquals(0*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    // user_0's apps:
    assertEquals(app_0_consumption, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(app_1_consumption, app_1.getCurrentConsumption().getMemorySize());
    // user_1's apps:
    assertEquals(app_2_consumption, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(app_3_consumption, app_3.getCurrentConsumption().getMemorySize());

    // Increase user-limit-factor from 1GB to 10GB (1% * 10 * 100GB = 10GB).
    // Pending for both user_0 and user_1 are still 1GB each, so user-limit-
    // factor is no longer the limiting factor.
    e.setUserLimitFactor(10.0f);

    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // Next container goes to user_0's app_1, since it still wanted 1GB.
    assertEquals(1*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    // user_0's apps:
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(2*GB, app_1.getCurrentConsumption().getMemorySize());
    // user_1's apps:
    assertEquals(2*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(0*GB, app_3.getCurrentConsumption().getMemorySize());

    applyCSAssignment(clusterResource,
        e.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), e, nodes, apps);
    // Last container goes to user_1's app_3, since it still wanted 1GB.
    // user_0's apps:
    assertEquals(0*GB, e.getTotalPendingResourcesConsideringUserLimit(
        clusterResource, RMNodeLabelsManager.NO_LABEL, false).getMemorySize());
    assertEquals(1*GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(2*GB, app_1.getCurrentConsumption().getMemorySize());
    // user_1's apps:
    assertEquals(2*GB, app_2.getCurrentConsumption().getMemorySize());
    assertEquals(1*GB, app_3.getCurrentConsumption().getMemorySize());

    // Release each container from app_0
    for (RMContainer rmContainer : app_0.getLiveContainers()) {
      e.completedContainer(clusterResource, app_0, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }

    // Release each container from app_1
    for (RMContainer rmContainer : app_1.getLiveContainers()) {
      e.completedContainer(clusterResource, app_1, node_0, rmContainer,
          ContainerStatus.newInstance(rmContainer.getContainerId(),
              ContainerState.COMPLETE, "",
              ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
          RMContainerEventType.KILL, null, true);
    }
  }

  private List<FiCaSchedulerApp> createListOfApps(int noOfApps, String user,
      LeafQueue defaultQueue) {
    List<FiCaSchedulerApp> appsLists = new ArrayList<FiCaSchedulerApp>();
    for (int i = 0; i < noOfApps; i++) {
      ApplicationAttemptId appAttemptId_0 =
          TestUtils.getMockApplicationAttemptId(i, 0);
      FiCaSchedulerApp app_0 =
          new FiCaSchedulerApp(appAttemptId_0, user, defaultQueue,
              mock(ActiveUsersManager.class), spyRMContext);
      appsLists.add(app_0);
    }
    return appsLists;
  }

  private CapacitySchedulerContext mockCSContext(
      CapacitySchedulerConfiguration csConf, Resource clusterResource) {
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(new YarnConfiguration());
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getClusterResource()).thenReturn(clusterResource);
    when(csContext.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(2 * GB, 2));
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
    return csContext;
  }

  @Test
  public void testApplicationQueuePercent()
      throws Exception {
    Resource res = Resource.newInstance(10 * 1024, 10);
    CapacityScheduler scheduler = mock(CapacityScheduler.class);
    when(scheduler.getClusterResource()).thenReturn(res);
    when(scheduler.getResourceCalculator())
        .thenReturn(new DefaultResourceCalculator());

    ApplicationAttemptId appAttId = createAppAttemptId(0, 0);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getEpoch()).thenReturn(3L);
    when(rmContext.getScheduler()).thenReturn(scheduler);
    when(rmContext.getRMApps())
      .thenReturn(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMNodeLabelsManager nlm = mock(RMNodeLabelsManager.class);
    when(nlm.getResourceByLabel(any(), any())).thenReturn(res);
    when(rmContext.getNodeLabelManager()).thenReturn(nlm);

    // Queue "test" consumes 100% of the cluster, so its capacity and absolute
    // capacity are both 1.0f.
    Queue queue = createQueue("test", null, 1.0f, 1.0f);
    final String user = "user1";
    FiCaSchedulerApp app =
        new FiCaSchedulerApp(appAttId, user, queue,
            queue.getAbstractUsersManager(), rmContext);

    // Resource request
    Resource requestedResource = Resource.newInstance(1536, 2);
    app.getAppAttemptResourceUsage().incUsed(requestedResource);
    // In "test" queue, 1536 used is 15% of both the queue and the cluster
    assertEquals(15.0f, app.getResourceUsageReport().getQueueUsagePercentage(),
        0.01f);
    assertEquals(15.0f,
        app.getResourceUsageReport().getClusterUsagePercentage(), 0.01f);

    // Queue "test2" is a child of root and its capacity is 50% of root. As a
    // child of root, its absolute capaicty is also 50%.
    queue = createQueue("test2", null, 0.5f, 0.5f);
    app = new FiCaSchedulerApp(appAttId, user, queue,
        queue.getAbstractUsersManager(), rmContext);
    app.getAppAttemptResourceUsage().incUsed(requestedResource);
    // In "test2" queue, 1536 used is 30% of "test2" and 15% of the cluster.
    assertEquals(30.0f, app.getResourceUsageReport().getQueueUsagePercentage(),
        0.01f);
    assertEquals(15.0f,
        app.getResourceUsageReport().getClusterUsagePercentage(), 0.01f);

    // Queue "test2.1" is 50% of queue "test2", which is 50% of the cluster.
    // Therefore, "test2.1" capacity is 50% and absolute capacity is 25%.
    AbstractCSQueue qChild = createQueue("test2.1", queue, 0.5f, 0.25f);
    app = new FiCaSchedulerApp(appAttId, user, qChild,
        qChild.getAbstractUsersManager(), rmContext);
    app.getAppAttemptResourceUsage().incUsed(requestedResource);
    // In "test2.1" queue, 1536 used is 60% of "test2.1" and 15% of the cluster.
    assertEquals(60.0f, app.getResourceUsageReport().getQueueUsagePercentage(),
        0.01f);
    assertEquals(15.0f,
        app.getResourceUsageReport().getClusterUsagePercentage(), 0.01f);
  }

  private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, appId);
    ApplicationAttemptId attId =
        ApplicationAttemptId.newInstance(appIdImpl, attemptId);
    return attId;
  }

  private AbstractCSQueue createQueue(String name, Queue parent, float capacity,
      float absCap) {
    CSQueueMetrics metrics = CSQueueMetrics.forQueue(name, parent, false, cs.getConf());
    QueueInfo queueInfo = QueueInfo.newInstance(name, capacity, 1.0f, 0, null,
        null, QueueState.RUNNING, null, "", null, false);
    ActiveUsersManager activeUsersManager = new ActiveUsersManager(metrics);
    AbstractCSQueue queue = mock(AbstractCSQueue.class);
    when(queue.getMetrics()).thenReturn(metrics);
    when(queue.getAbstractUsersManager()).thenReturn(activeUsersManager);
    when(queue.getQueueInfo(false, false)).thenReturn(queueInfo);
    QueueCapacities qCaps = mock(QueueCapacities.class);
    when(qCaps.getAbsoluteCapacity(any())).thenReturn(absCap);
    when(queue.getQueueCapacities()).thenReturn(qCaps);
    return queue;
  }

  @After
  public void tearDown() throws Exception {
    if (cs != null) {
      cs.stop();
    }
  }
}
