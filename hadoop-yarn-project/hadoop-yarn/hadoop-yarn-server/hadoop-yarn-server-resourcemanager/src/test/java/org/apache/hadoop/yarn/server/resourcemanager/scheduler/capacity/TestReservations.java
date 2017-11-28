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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;

import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils.toSchedulerKey;

public class TestReservations {

  private static final Log LOG = LogFactory.getLog(TestReservations.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  RMContext rmContext;
  RMContext spyRMContext;
  CapacityScheduler cs;
  // CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;

  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  CSQueue root;
  Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
  Map<String, CSQueue> oldQueues = new HashMap<String, CSQueue>();

  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  @Before
  public void setUp() throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    cs = spy(spyCs);
    rmContext = TestUtils.getMockRMContext();

  }

  private void setup(CapacitySchedulerConfiguration csConf) throws Exception {
    setup(csConf, false);
  }

  private void setup(CapacitySchedulerConfiguration csConf,
      boolean addUserLimits) throws Exception {

    csConf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    final String newRoot = "root" + System.currentTimeMillis();
    // final String newRoot = "root";

    setupQueueConfiguration(csConf, newRoot, addUserLimits);
    YarnConfiguration conf = new YarnConfiguration();
    cs.setConf(conf);

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(16 * GB, 12));
    when(csContext.getClusterResource()).thenReturn(
        Resources.createResource(100 * 16 * GB, 100 * 12));
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
    when(csContext.getRMContext()).thenReturn(rmContext);
    RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager(
        conf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

    root = CapacitySchedulerQueueManager.parseQueue(csContext, csConf, null,
        CapacitySchedulerConfiguration.ROOT, queues, queues, TestUtils.spyHook);

    ResourceUsage queueResUsage = root.getQueueResourceUsage();
    when(csContext.getClusterResourceUsage())
        .thenReturn(queueResUsage);

    spyRMContext = spy(rmContext);
    when(spyRMContext.getScheduler()).thenReturn(cs);
    when(spyRMContext.getYarnConfiguration())
        .thenReturn(new YarnConfiguration());

    cs.setRMContext(spyRMContext);
    cs.init(csConf);
    cs.start();

    when(cs.getNumClusterNodes()).thenReturn(3);
  }

  private static final String A = "a";

  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf,
      final String newRoot, boolean addUserLimits) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { newRoot });
    conf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100);
    conf.setAcl(CapacitySchedulerConfiguration.ROOT,
        QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_newRoot = CapacitySchedulerConfiguration.ROOT + "."
        + newRoot;
    conf.setQueues(Q_newRoot, new String[] { A });
    conf.setCapacity(Q_newRoot, 100);
    conf.setMaximumCapacity(Q_newRoot, 100);
    conf.setAcl(Q_newRoot, QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_A = Q_newRoot + "." + A;
    conf.setCapacity(Q_A, 100f);
    conf.setMaximumCapacity(Q_A, 100);
    conf.setAcl(Q_A, QueueACL.SUBMIT_APPLICATIONS, "*");

    if (addUserLimits) {
      conf.setUserLimit(Q_A, 25);
      conf.setUserLimitFactor(Q_A, 0.25f);
    }
  }

  static LeafQueue stubLeafQueue(LeafQueue queue) {
    ParentQueue parent = (ParentQueue) queue.getParent();

    if (parent != null) {
      // Stub out parent queue's accept and apply.
      doReturn(true).when(parent).accept(any(Resource.class),
          any(ResourceCommitRequest.class));
      doNothing().when(parent).apply(any(Resource.class),
          any(ResourceCommitRequest.class));
    }
    return queue;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReservation() throws Exception {
    // Test that we now unreserve and use a node that has space

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_0 = spy(app_0);
    Mockito.doNothing().when(app_0).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_0.getApplicationId(), mock(RMApp.class));

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_1 = spy(app_1);
    Mockito.doNothing().when(app_1).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    cs.getNodeTracker().addNode(node_0);
    cs.getNodeTracker().addNode(node_1);
    cs.getNodeTracker().addNode(node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(2 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(22 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // Only 1 map - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(5 * GB, a.getUsedResources().getMemorySize());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(19 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // Only 1 map to other node - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(2, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // try to assign reducer (5G on node 0 and should reserve)
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(13 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(11 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(2, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // assign reducer to node 2
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_2,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(18 * GB, a.getUsedResources().getMemorySize());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(5 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(1, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // node_1 heartbeat and unreserves from node_0 in order to allocate
    // on node_1
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(18 * GB, a.getUsedResources().getMemorySize());
    assertEquals(18 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(18 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(8 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(5 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(0, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));
  }

  // Test that hitting a reservation limit and needing to unreserve
  // does not affect assigning containers for other users
  @Test
  @SuppressWarnings("unchecked")
  public void testReservationLimitOtherUsers() throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf, true);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";
    final String user_1 = "user_1";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_0 = spy(app_0);
    Mockito.doNothing().when(app_0).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_0.getApplicationId(), mock(RMApp.class));

    a.submitApplicationAttempt(app_0, user_0);

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_1, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_1 = spy(app_1);
    Mockito.doNothing().when(app_1).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_1.getApplicationId(), mock(RMApp.class));

    a.submitApplicationAttempt(app_1, user_1);

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    cs.getNodeTracker().addNode(node_0);
    cs.getNodeTracker().addNode(node_1);
    cs.getNodeTracker().addNode(node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_1.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));

    // Start testing...
    // Only AM
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(2 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(22 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(4 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(2 * GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(4 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(20 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(2 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // Add a few requests to each app
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 8 * GB, 2, true,
            priorityMap, recordFactory)));
    app_1.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 2, true,
            priorityMap, recordFactory)));

    // add a reservation for app_0
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(12 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(2 * GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());
    assertEquals(4 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(12 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(2 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // next assignment is beyond user limit for user_0 but it should assign to
    // app_1 for user_1
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(14 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(4 * GB, app_1.getCurrentConsumption().getMemorySize());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());
    assertEquals(6 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(10 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(4 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());
  }

  @Test
  public void testReservationNoContinueLook() throws Exception {
    // Test that with reservations-continue-look-all-nodes feature off
    // we don't unreserve and show we could get stuck

    queues = new HashMap<String, CSQueue>();
    // test that the deadlock occurs when turned off
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setBoolean(CapacitySchedulerConfiguration.RESERVE_CONT_LOOK_ALL_NODES,
        false);
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_0 = spy(app_0);
    Mockito.doNothing().when(app_0).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_0.getApplicationId(), mock(RMApp.class));

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_1 = spy(app_1);
    Mockito.doNothing().when(app_1).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(2 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(22 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // Only 1 map - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(5 * GB, a.getUsedResources().getMemorySize());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(19 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // Only 1 map to other node - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(2, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // try to assign reducer (5G on node 0 and should reserve)
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(13 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(11 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(2, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // assign reducer to node 2
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_2,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(18 * GB, a.getUsedResources().getMemorySize());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(5 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(1, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // node_1 heartbeat and won't unreserve from node_0, potentially stuck
    // if AM doesn't handle
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(18 * GB, a.getUsedResources().getMemorySize());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(5 * GB, node_2.getAllocatedResource().getMemorySize());
    assertEquals(1, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAssignContainersNeedToUnreserve() throws Exception {
    // Test that we now unreserve and use a node that has space
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_0 = spy(app_0);
    Mockito.doNothing().when(app_0).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_0.getApplicationId(), mock(RMApp.class));

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_1 = spy(app_1);
    Mockito.doNothing().when(app_1).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1);

    cs.getNodeTracker().addNode(node_0);
    cs.getNodeTracker().addNode(node_1);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(2 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(14 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());

    // Only 1 map - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(5 * GB, a.getUsedResources().getMemorySize());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());

    // Only 1 map to other node - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(8 * GB, a.getMetrics().getAvailableMB());
    assertEquals(8 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(2, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // try to assign reducer (5G on node 0 and should reserve)
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(13 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(2, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));

    // could allocate but told need to unreserve first
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(13 * GB, a.getUsedResources().getMemorySize());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(8 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(1, app_0.getOutstandingAsksCount(
        toSchedulerKey(priorityReduce)));
  }

  @Test
  public void testGetAppToUnreserve() throws Exception {

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);
    final String user_0 = "user_0";
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);

    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);

    Resource clusterResource = Resources.createResource(2 * 8 * GB);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Setup resource-requests
    Priority p = TestUtils.createMockPriority(5);
    SchedulerRequestKey priorityMap = toSchedulerKey(p);
    Resource capability = Resources.createResource(2*GB, 0);

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    ContainerAllocationExpirer expirer =
      mock(ContainerAllocationExpirer.class);
    DrainDispatcher drainDispatcher = new DrainDispatcher();
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    Container container = TestUtils.getMockContainer(containerId,
        node_1.getNodeID(), Resources.createResource(2*GB),
        priorityMap.getPriority());
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        node_1.getNodeID(), "user", rmContext);

    Container container_1 = TestUtils.getMockContainer(containerId,
        node_0.getNodeID(), Resources.createResource(1*GB),
        priorityMap.getPriority());
    RMContainer rmContainer_1 = new RMContainerImpl(container_1,
        SchedulerRequestKey.extractFrom(container_1), appAttemptId,
        node_0.getNodeID(), "user", rmContext);

    // no reserved containers
    NodeId unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability,
            cs.getResourceCalculator());
    assertEquals(null, unreserveId);

    // no reserved containers - reserve then unreserve
    app_0.reserve(node_0, priorityMap, rmContainer_1, container_1);
    app_0.unreserve(priorityMap, node_0, rmContainer_1);
    unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability,
        cs.getResourceCalculator());
    assertEquals(null, unreserveId);

    // no container large enough is reserved
    app_0.reserve(node_0, priorityMap, rmContainer_1, container_1);
    unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability,
        cs.getResourceCalculator());
    assertEquals(null, unreserveId);

    // reserve one that is now large enough
    app_0.reserve(node_1, priorityMap, rmContainer, container);
    unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability,
        cs.getResourceCalculator());
    assertEquals(node_1.getNodeID(), unreserveId);
  }

  @Test
  public void testFindNodeToUnreserve() throws Exception {

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);
    final String user_0 = "user_0";
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);

    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);

    // Setup resource-requests
    Priority p = TestUtils.createMockPriority(5);
    SchedulerRequestKey priorityMap = toSchedulerKey(p);
    Resource capability = Resources.createResource(2 * GB, 0);

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    ContainerAllocationExpirer expirer =
      mock(ContainerAllocationExpirer.class);
    DrainDispatcher drainDispatcher = new DrainDispatcher();
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    Container container = TestUtils.getMockContainer(containerId,
        node_1.getNodeID(), Resources.createResource(2*GB),
        priorityMap.getPriority());
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), appAttemptId,
        node_1.getNodeID(), "user", rmContext);

    // nothing reserved
    RMContainer toUnreserveContainer = app_0.findNodeToUnreserve(node_1,
            priorityMap, capability);
    assertTrue(toUnreserveContainer == null);

    // reserved but scheduler doesn't know about that node.
    app_0.reserve(node_1, priorityMap, rmContainer, container);
    node_1.reserveResource(app_0, priorityMap, rmContainer);
    toUnreserveContainer = app_0.findNodeToUnreserve(node_1,
            priorityMap, capability);
    assertTrue(toUnreserveContainer == null);
  }

  @Test
  public void testAssignToQueue() throws Exception {

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_0 = spy(app_0);
    Mockito.doNothing().when(app_0).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_0.getApplicationId(), mock(RMApp.class));

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_1 = spy(app_1);
    Mockito.doNothing().when(app_1).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(2 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(14 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());

    // Only 1 map - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(5 * GB, a.getUsedResources().getMemorySize());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());

    // Only 1 map to other node - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(8 * GB, a.getMetrics().getAvailableMB());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());

    // now add in reservations and make sure it continues if config set
    // allocate to queue so that the potential new capacity is greater then
    // absoluteMaxCapacity
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(13 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());

    ResourceLimits limits =
        new ResourceLimits(Resources.createResource(13 * GB));
    boolean res =
        a.canAssignToThisQueue(Resources.createResource(13 * GB),
            RMNodeLabelsManager.NO_LABEL, limits,
            Resources.createResource(3 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertTrue(res);
    // 16GB total, 13GB consumed (8 allocated, 5 reserved). asking for 5GB so we would have to
    // unreserve 2GB to get the total 5GB needed.
    // also note vcore checks not enabled
    assertEquals(0, limits.getHeadroom().getMemorySize());

    refreshQueuesTurnOffReservationsContLook(a, csConf);

    // should return false since reservations continue look is off.
    limits =
        new ResourceLimits(Resources.createResource(13 * GB));
    res =
        a.canAssignToThisQueue(Resources.createResource(13 * GB),
            RMNodeLabelsManager.NO_LABEL, limits,
            Resources.createResource(3 * GB),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    assertFalse(res);
  }

  public void refreshQueuesTurnOffReservationsContLook(LeafQueue a,
      CapacitySchedulerConfiguration csConf) throws Exception {
    // before reinitialization
    assertEquals(true, a.getReservationContinueLooking());
    assertEquals(true,
        ((ParentQueue) a.getParent()).getReservationContinueLooking());

    csConf.setBoolean(
        CapacitySchedulerConfiguration.RESERVE_CONT_LOOK_ALL_NODES, false);
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot = CapacitySchedulerQueueManager.parseQueue(csContext,
        csConf, null, CapacitySchedulerConfiguration.ROOT, newQueues, queues,
        TestUtils.spyHook);
    queues = newQueues;
    root.reinitialize(newRoot, cs.getClusterResource());

    // after reinitialization
    assertEquals(false, a.getReservationContinueLooking());
    assertEquals(false,
        ((ParentQueue) a.getParent()).getReservationContinueLooking());
  }

  @Test
  public void testContinueLookingReservationsAfterQueueRefresh()
      throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'e'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    refreshQueuesTurnOffReservationsContLook(a, csConf);
  }

  @Test
  public void testAssignToUser() throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_0 = spy(app_0);
    Mockito.doNothing().when(app_0).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_0.getApplicationId(), mock(RMApp.class));
    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_1 = spy(app_1);
    Mockito.doNothing().when(app_1).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));

    // Start testing...
    // Only AM
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(2 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(14 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());

    // Only 1 map - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(5 * GB, a.getUsedResources().getMemorySize());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());

    // Only 1 map to other node - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(8 * GB, a.getMetrics().getAvailableMB());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());

    // now add in reservations and make sure it continues if config set
    // allocate to queue so that the potential new capacity is greater then
    // absoluteMaxCapacity
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(13 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(5 * GB, app_0.getCurrentReservation().getMemorySize());

    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());

    // not over the limit
    Resource limit = Resources.createResource(14 * GB, 0);
    ResourceLimits userResourceLimits = new ResourceLimits(clusterResource);
    boolean res = a.canAssignToUser(clusterResource, user_0, limit, app_0, "", userResourceLimits);
    assertTrue(res);
    assertEquals(Resources.none(), userResourceLimits.getAmountNeededUnreserve());


    // set limit so it subtracts reservations and it can continue
    limit = Resources.createResource(12 * GB, 0);
    userResourceLimits = new ResourceLimits(clusterResource);
    res = a.canAssignToUser(clusterResource, user_0, limit, app_0,
             "", userResourceLimits);
    assertTrue(res);
    // limit set to 12GB, we are using 13GB (8 allocated,  5 reserved), to get under limit
    // we need to unreserve 1GB
    // also note vcore checks not enabled
    assertEquals(Resources.createResource(1 * GB, 4),
        userResourceLimits.getAmountNeededUnreserve());

    refreshQueuesTurnOffReservationsContLook(a, csConf);
    userResourceLimits = new ResourceLimits(clusterResource);

    // should now return false since feature off
    res = a.canAssignToUser(clusterResource, user_0, limit, app_0, "", userResourceLimits);
    assertFalse(res);
    assertEquals(Resources.none(), userResourceLimits.getAmountNeededUnreserve());
  }

  @Test
  public void testReservationsNoneAvailable() throws Exception {
    // Test that we now unreserve and use a node that has space

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_0 = spy(app_0);
    Mockito.doNothing().when(app_0).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    rmContext.getRMApps().put(app_0.getApplicationId(), mock(RMApp.class));

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), spyRMContext);
    app_1 = spy(app_1);
    Mockito.doNothing().when(app_1).updateAMContainerDiagnostics(any(AMState.class),
        any(String.class));
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    Map<ApplicationAttemptId, FiCaSchedulerApp> apps = ImmutableMap.of(
        app_0.getApplicationAttemptId(), app_0, app_1.getApplicationAttemptId(),
        app_1);
    Map<NodeId, FiCaSchedulerNode> nodes = ImmutableMap.of(node_0.getNodeID(),
        node_0, node_1.getNodeID(), node_1, node_2.getNodeID(), node_2);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));


    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);
    Priority priorityLast = TestUtils.createMockPriority(12);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 1, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 8 * GB, 2, true,
            priorityLast, recordFactory)));

    // Start testing...
    // Only AM
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(2 * GB, a.getUsedResources().getMemorySize());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(22 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // Only 1 map - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(5 * GB, a.getUsedResources().getMemorySize());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(19 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // Only 1 map to other node - simulating reduce
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_1,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // try to assign reducer (5G on node 0), but tell it's resource limits <
    // used (8G) + required (5G). It will not reserved since it has to unreserve
    // some resource. Even with continous reservation looking, we don't allow 
    // unreserve resource to reserve container.
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(Resources.createResource(10 * GB)),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    // app_0's headroom = limit (10G) - used (8G) = 2G 
    assertEquals(2 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // try to assign reducer (5G on node 0), but tell it's resource limits <
    // used (8G) + required (5G). It will not reserved since it has to unreserve
    // some resource. Unfortunately, there's nothing to unreserve.
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_2,
            new ResourceLimits(Resources.createResource(10 * GB)),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(8 * GB, a.getUsedResources().getMemorySize());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    // app_0's headroom = limit (10G) - used (8G) = 2G 
    assertEquals(2 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(0 * GB, node_2.getAllocatedResource().getMemorySize());

    // let it assign 5G to node_2
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_2,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(13 * GB, a.getUsedResources().getMemorySize());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(11 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(5 * GB, node_2.getAllocatedResource().getMemorySize());

    // reserve 8G node_0
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_0,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(21 * GB, a.getUsedResources().getMemorySize());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(5 * GB, node_2.getAllocatedResource().getMemorySize());

    // try to assign (8G on node 2). No room to allocate,
    // continued to try due to having reservation above,
    // but hits queue limits so can't reserve anymore.
    TestUtils.applyResourceCommitRequest(clusterResource,
        a.assignContainers(clusterResource, node_2,
            new ResourceLimits(clusterResource),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), nodes, apps);
    assertEquals(21 * GB, a.getUsedResources().getMemorySize());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemorySize());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemorySize());
    assertEquals(5 * GB, node_0.getAllocatedResource().getMemorySize());
    assertEquals(3 * GB, node_1.getAllocatedResource().getMemorySize());
    assertEquals(5 * GB, node_2.getAllocatedResource().getMemorySize());
  }
}
