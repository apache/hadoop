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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.google.common.collect.Sets;

public class TestFairScheduler {

  static class MockClock implements Clock {
    private long time = 0;
    @Override
    public long getTime() {
      return time;
    }

    public void tick(int seconds) {
      time = time + seconds * 1000;
    }

  }

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "/tmp")).getAbsolutePath();

  final static String ALLOC_FILE = new File(TEST_DIR,
      "test-queues").getAbsolutePath();

  private FairScheduler scheduler;
  private ResourceManager resourceManager;
  private Configuration conf;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private int APP_ID = 1; // Incrementing counter for schedling apps
  private int ATTEMPT_ID = 1; // Incrementing counter for scheduling attempts

  // HELPER METHODS
  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    conf = createConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB,
      1024);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 10240);
    // All tests assume only one assignment per node update
    conf.set(FairSchedulerConfiguration.ASSIGN_MULTIPLE, "false");
    resourceManager = new ResourceManager();
    resourceManager.init(conf);

    // TODO: This test should really be using MockRM. For now starting stuff
    // that is needed at a bare minimum.
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();

    // to initialize the master key
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
  }

  @After
  public void tearDown() {
    scheduler = null;
    resourceManager = null;
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }


  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
    ResourceScheduler scheduler = new FairScheduler();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2048);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    try {
      scheduler.reinitialize(conf, null);
      fail("Exception is expected because the min memory allocation is" +
        " larger than the max memory allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue("The thrown exception is not the expected one.",
        e.getMessage().startsWith(
          "Invalid resource scheduler memory"));
    }

    conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 2);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 1);
    try {
      scheduler.reinitialize(conf, null);
      fail("Exception is expected because the min vcores allocation is" +
        " larger than the max vcores allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue("The thrown exception is not the expected one.",
        e.getMessage().startsWith(
          "Invalid resource scheduler vcores"));
    }
  }

  private Configuration createConfiguration() {
    Configuration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    return conf;
  }

  private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, appId);
    ApplicationAttemptId attId =
        ApplicationAttemptId.newInstance(appIdImpl, attemptId);
    return attId;
  }
  
  private ResourceRequest createResourceRequest(int memory, String host,
      int priority, int numContainers, boolean relaxLocality) {
    return createResourceRequest(memory, 1, host, priority, numContainers,
        relaxLocality);
  }

  private ResourceRequest createResourceRequest(int memory, int vcores, String host,
      int priority, int numContainers, boolean relaxLocality) {
    ResourceRequest request = recordFactory.newRecordInstance(ResourceRequest.class);
    request.setCapability(BuilderUtils.newResource(memory, vcores));
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    request.setRelaxLocality(relaxLocality);
    return request;
  }

  /**
   * Creates a single container priority-1 request and submits to
   * scheduler.
   */
  private ApplicationAttemptId createSchedulingRequest(int memory, String queueId,
      String userId) {
    return createSchedulingRequest(memory, queueId, userId, 1);
  }
  
  private ApplicationAttemptId createSchedulingRequest(int memory, int vcores,
      String queueId, String userId) {
    return createSchedulingRequest(memory, vcores, queueId, userId, 1);
  }

  private ApplicationAttemptId createSchedulingRequest(int memory, String queueId,
      String userId, int numContainers) {
    return createSchedulingRequest(memory, queueId, userId, numContainers, 1);
  }
  
  private ApplicationAttemptId createSchedulingRequest(int memory, int vcores,
      String queueId, String userId, int numContainers) {
    return createSchedulingRequest(memory, vcores, queueId, userId, numContainers, 1);
  }

  private ApplicationAttemptId createSchedulingRequest(int memory, String queueId,
      String userId, int numContainers, int priority) {
    return createSchedulingRequest(memory, 1, queueId, userId, numContainers,
        priority);
  }
  
  private ApplicationAttemptId createSchedulingRequest(int memory, int vcores,
      String queueId, String userId, int numContainers, int priority) {
    ApplicationAttemptId id = createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    scheduler.addApplication(id.getApplicationId(), queueId, userId);
    // This conditional is for testAclSubmitApplication where app is rejected
    // and no app is added.
    if (scheduler.getSchedulerApplications().containsKey(id.getApplicationId())) {
      scheduler.addApplicationAttempt(id, false);
    }
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest request = createResourceRequest(memory, vcores, ResourceRequest.ANY,
        priority, numContainers, true);
    ask.add(request);
    scheduler.allocate(id, ask,  new ArrayList<ContainerId>(), null, null);
    return id;
  }
  
  private void createSchedulingRequestExistingApplication(int memory, int priority,
      ApplicationAttemptId attId) {
    ResourceRequest request = createResourceRequest(memory, ResourceRequest.ANY,
        priority, 1, true);
    createSchedulingRequestExistingApplication(request, attId);
  }
  
  private void createSchedulingRequestExistingApplication(int memory, int vcores,
      int priority, ApplicationAttemptId attId) {
	ResourceRequest request = createResourceRequest(memory, vcores, ResourceRequest.ANY,
		priority, 1, true);
	createSchedulingRequestExistingApplication(request, attId);
  }
  
  private void createSchedulingRequestExistingApplication(ResourceRequest request,
      ApplicationAttemptId attId) {
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.add(request);
    scheduler.allocate(attId, ask,  new ArrayList<ContainerId>(), null, null);
  }

  // TESTS

  @Test(timeout=2000)
  public void testLoadConfigurationOnInitialize() throws IOException {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setInt(FairSchedulerConfiguration.MAX_ASSIGN, 3);
    conf.setBoolean(FairSchedulerConfiguration.SIZE_BASED_WEIGHT, true);
    conf.setFloat(FairSchedulerConfiguration.LOCALITY_THRESHOLD_NODE, .5f);
    conf.setFloat(FairSchedulerConfiguration.LOCALITY_THRESHOLD_RACK, .7f);
    conf.setBoolean(FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED,
            true);
    conf.setInt(FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_SLEEP_MS,
            10);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_RACK_MS,
            5000);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_NODE_MS,
            5000);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setInt(FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 
      128);
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    Assert.assertEquals(true, scheduler.assignMultiple);
    Assert.assertEquals(3, scheduler.maxAssign);
    Assert.assertEquals(true, scheduler.sizeBasedWeight);
    Assert.assertEquals(.5, scheduler.nodeLocalityThreshold, .01);
    Assert.assertEquals(.7, scheduler.rackLocalityThreshold, .01);
    Assert.assertTrue("The continuous scheduling should be enabled",
            scheduler.continuousSchedulingEnabled);
    Assert.assertEquals(10, scheduler.continuousSchedulingSleepMs);
    Assert.assertEquals(5000, scheduler.nodeLocalityDelayMs);
    Assert.assertEquals(5000, scheduler.rackLocalityDelayMs);
    Assert.assertEquals(1024, scheduler.getMaximumResourceCapability().getMemory());
    Assert.assertEquals(512, scheduler.getMinimumResourceCapability().getMemory());
    Assert.assertEquals(128, 
      scheduler.getIncrementResourceCapability().getMemory());
  }
  
  @Test  
  public void testNonMinZeroResourcesSettings() throws IOException {
    FairScheduler fs = new FairScheduler();
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 1);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    fs.reinitialize(conf, null);
    Assert.assertEquals(256, fs.getMinimumResourceCapability().getMemory());
    Assert.assertEquals(1, fs.getMinimumResourceCapability().getVirtualCores());
    Assert.assertEquals(512, fs.getIncrementResourceCapability().getMemory());
    Assert.assertEquals(2, fs.getIncrementResourceCapability().getVirtualCores());
  }  
  
  @Test  
  public void testMinZeroResourcesSettings() throws IOException {  
    FairScheduler fs = new FairScheduler();  
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 0);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    fs.reinitialize(conf, null);  
    Assert.assertEquals(0, fs.getMinimumResourceCapability().getMemory());  
    Assert.assertEquals(0, fs.getMinimumResourceCapability().getVirtualCores());
    Assert.assertEquals(512, fs.getIncrementResourceCapability().getMemory());
    Assert.assertEquals(2, fs.getIncrementResourceCapability().getVirtualCores());
  }  
  
  @Test
  public void testAggregateCapacityTracking() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(1024, scheduler.getClusterCapacity().getMemory());

    // Add another node
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(512), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    assertEquals(1536, scheduler.getClusterCapacity().getMemory());

    // Remove the first node
    NodeRemovedSchedulerEvent nodeEvent3 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent3);
    assertEquals(512, scheduler.getClusterCapacity().getMemory());
  }

  @Test
  public void testSimpleFairShareCalculation() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(10 * 1024), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Have two queues which want entire cluster capacity
    createSchedulingRequest(10 * 1024, "queue1", "user1");
    createSchedulingRequest(10 * 1024, "queue2", "user1");

    scheduler.update();

    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());
    
    // Divided three ways - betwen the two queues and the default queue
    for (FSLeafQueue p : queues) {
      assertEquals(3414, p.getFairShare().getMemory());
      assertEquals(3414, p.getMetrics().getFairShareMB());
    }
  }
  
  @Test
  public void testSimpleHierarchicalFairShareCalculation() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    int capacity = 10 * 24;
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(capacity), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Have two queues which want entire cluster capacity
    createSchedulingRequest(10 * 1024, "parent.queue2", "user1");
    createSchedulingRequest(10 * 1024, "parent.queue3", "user1");

    scheduler.update();

    QueueManager queueManager = scheduler.getQueueManager();
    Collection<FSLeafQueue> queues = queueManager.getLeafQueues();
    assertEquals(3, queues.size());
    
    FSLeafQueue queue1 = queueManager.getLeafQueue("default", true);
    FSLeafQueue queue2 = queueManager.getLeafQueue("parent.queue2", true);
    FSLeafQueue queue3 = queueManager.getLeafQueue("parent.queue3", true);
    assertEquals(capacity / 2, queue1.getFairShare().getMemory());
    assertEquals(capacity / 2, queue1.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue2.getFairShare().getMemory());
    assertEquals(capacity / 4, queue2.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue3.getFairShare().getMemory());
    assertEquals(capacity / 4, queue3.getMetrics().getFairShareMB());
  }

  @Test
  public void testHierarchicalQueuesSimilarParents() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    QueueManager queueManager = scheduler.getQueueManager();
    FSLeafQueue leafQueue = queueManager.getLeafQueue("parent.child", true);
    Assert.assertEquals(2, queueManager.getLeafQueues().size());
    Assert.assertNotNull(leafQueue);
    Assert.assertEquals("root.parent.child", leafQueue.getName());

    FSLeafQueue leafQueue2 = queueManager.getLeafQueue("parent", true);
    Assert.assertNull(leafQueue2);
    Assert.assertEquals(2, queueManager.getLeafQueues().size());
    
    FSLeafQueue leafQueue3 = queueManager.getLeafQueue("parent.child.grandchild", true);
    Assert.assertNull(leafQueue3);
    Assert.assertEquals(2, queueManager.getLeafQueues().size());
    
    FSLeafQueue leafQueue4 = queueManager.getLeafQueue("parent.sister", true);
    Assert.assertNotNull(leafQueue4);
    Assert.assertEquals("root.parent.sister", leafQueue4.getName());
    Assert.assertEquals(3, queueManager.getLeafQueues().size());
  }

  @Test
  public void testSchedulerRootQueueMetrics() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure reserved memory gets updated correctly
    assertEquals(1024, scheduler.rootMetrics.getReservedMB());
    
    // Now another node checks in with capacity
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);


    // The old reservation should still be there...
    assertEquals(1024, scheduler.rootMetrics.getReservedMB());

    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.rootMetrics.getReservedMB());
  }

  @Test (timeout = 5000)
  public void testSimpleContainerAllocation() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Add another node
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(512, 2), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    createSchedulingRequest(512, 2, "queue1", "user1", 2);

    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Asked for less than increment allocation.
    assertEquals(FairSchedulerConfiguration.DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_MB,
        scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemory());

    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);

    assertEquals(1024, scheduler.getQueueManager().getQueue("queue1").
      getResourceUsage().getMemory());
    assertEquals(2, scheduler.getQueueManager().getQueue("queue1").
      getResourceUsage().getVirtualCores());

    // verify metrics
    QueueMetrics queue1Metrics = scheduler.getQueueManager().getQueue("queue1")
        .getMetrics();
    assertEquals(1024, queue1Metrics.getAllocatedMB());
    assertEquals(2, queue1Metrics.getAllocatedVirtualCores());
    assertEquals(1024, scheduler.getRootQueueMetrics().getAllocatedMB());
    assertEquals(2, scheduler.getRootQueueMetrics().getAllocatedVirtualCores());
    assertEquals(512, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(4, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
  }

  @Test (timeout = 5000)
  public void testSimpleContainerReservation() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    
    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemory());

    // Now queue 2 requests likewise
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue2", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is waiting with a reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
      getResourceUsage().getMemory());
    assertEquals(1024, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemory());

    // Now another node checks in with capacity
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);

    // Make sure this goes to queue 2
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemory());

    // The old reservation should still be there...
    assertEquals(1024, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemory());
    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemory());

  }

  @Test
  public void testUserAsDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    RMContext rmContext = resourceManager.getRMContext();
    Map<ApplicationId, RMApp> appsMap = rmContext.getRMApps();
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    RMApp rmApp = new RMAppImpl(appAttemptId.getApplicationId(), rmContext, conf,
        null, null, null, ApplicationSubmissionContext.newInstance(null, null,
        null, null, null, false, false, 0, null, null), null, null, 0, null, null);
    appsMap.put(appAttemptId.getApplicationId(), rmApp);
    
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(appAttemptId.getApplicationId(), "default",
          "user1");
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attempAddedEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    scheduler.handle(attempAddedEvent);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getRunnableAppSchedulables().size());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getRunnableAppSchedulables().size());
    assertEquals("root.user1", rmApp.getQueue());
  }
  
  @Test
  public void testNotUserAsDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    RMContext rmContext = resourceManager.getRMContext();
    Map<ApplicationId, RMApp> appsMap = rmContext.getRMApps();
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    RMApp rmApp = new RMAppImpl(appAttemptId.getApplicationId(), rmContext, conf,
        null, null, null, ApplicationSubmissionContext.newInstance(null, null,
        null, null, null, false, false, 0, null, null), null, null, 0, null, null);
    appsMap.put(appAttemptId.getApplicationId(), rmApp);

    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(appAttemptId.getApplicationId(), "default",
          "user2");
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attempAddedEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    scheduler.handle(attempAddedEvent);
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getRunnableAppSchedulables().size());
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("default", true)
        .getRunnableAppSchedulables().size());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user2", true)
        .getRunnableAppSchedulables().size());
  }

  @Test
  public void testEmptyQueueName() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // only default queue
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());

    // submit app with empty queue
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(appAttemptId.getApplicationId(), "", "user1");
    scheduler.handle(appAddedEvent);

    // submission rejected
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());
  }

  @Test
  public void testAssignToQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);
    
    FSLeafQueue queue1 = scheduler.assignToQueue(rmApp1, "default", "asterix");
    FSLeafQueue queue2 = scheduler.assignToQueue(rmApp2, "notdefault", "obelix");
    
    // assert FSLeafQueue's name is the correct name is the one set in the RMApp
    assertEquals(rmApp1.getQueue(), queue1.getName());
    assertEquals("root.asterix", rmApp1.getQueue());
    assertEquals(rmApp2.getQueue(), queue2.getName());
    assertEquals("root.notdefault", rmApp2.getQueue());
  }

  @Test
  public void testAssignToNonLeafQueueReturnsNull() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.getQueueManager().getLeafQueue("root.child1.granchild", true);
    scheduler.getQueueManager().getLeafQueue("root.child2", true);

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);

    // Trying to assign to non leaf queue would return null
    assertNull(scheduler.assignToQueue(rmApp1, "root.child1", "tintin"));
    assertNotNull(scheduler.assignToQueue(rmApp2, "root.child2", "snowy"));
  }
  
  @Test
  public void testQueuePlacementWithPolicy() throws Exception {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId appId;

    List<QueuePlacementRule> rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.PrimaryGroup().initialize(false, null));
    rules.add(new QueuePlacementRule.SecondaryGroupExistingQueue().initialize(false, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    Set<String> queues = Sets.newHashSet("root.user1", "root.user3group",
        "root.user4subgroup1", "root.user4subgroup2" , "root.user5subgroup2");
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, queues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user3");
    assertEquals("root.user3group", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user4");
    assertEquals("root.user4subgroup1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "user5");
    assertEquals("root.user5subgroup2", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());
    
    // test without specified as first rule
    rules = new ArrayList<QueuePlacementRule>();
    rules.add(new QueuePlacementRule.User().initialize(false, null));
    rules.add(new QueuePlacementRule.Specified().initialize(true, null));
    rules.add(new QueuePlacementRule.Default().initialize(true, null));
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, queues, conf);
    appId = createSchedulingRequest(1024, "somequeue", "user1");
    assertEquals("root.user1", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "somequeue", "otheruser");
    assertEquals("root.somequeue", scheduler.getSchedulerApp(appId).getQueueName());
    appId = createSchedulingRequest(1024, "default", "otheruser");
    assertEquals("root.default", scheduler.getSchedulerApp(appId).getQueueName());
  }

  @Test
  public void testFairShareWithMinAlloc() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    createSchedulingRequest(2 * 1024, "queueA", "user1");
    createSchedulingRequest(2 * 1024, "queueB", "user1");

    scheduler.update();

    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueA")) {
        assertEquals(1024, p.getFairShare().getMemory());
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(2048, p.getFairShare().getMemory());
      }
    }
  }

  /**
   * Make allocation requests and ensure they are reflected in queue demand.
   */
  @Test
  public void testQueueDemandCalculation() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    scheduler.addApplication(id11.getApplicationId(), "root.queue1", "user1");
    scheduler.addApplicationAttempt(id11, false);
    ApplicationAttemptId id21 = createAppAttemptId(2, 1);
    scheduler.addApplication(id21.getApplicationId(), "root.queue2", "user1");
    scheduler.addApplicationAttempt(id21, false);
    ApplicationAttemptId id22 = createAppAttemptId(2, 2);
    scheduler.addApplication(id22.getApplicationId(), "root.queue2", "user1");
    scheduler.addApplicationAttempt(id22, false);

    int minReqSize = 
        FairSchedulerConfiguration.DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_MB;
    
    // First ask, queue1 requests 1 large (minReqSize * 2).
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ResourceRequest request1 =
        createResourceRequest(minReqSize * 2, ResourceRequest.ANY, 1, 1, true);
    ask1.add(request1);
    scheduler.allocate(id11, ask1, new ArrayList<ContainerId>(), null, null);

    // Second ask, queue2 requests 1 large + (2 * minReqSize)
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ResourceRequest request2 = createResourceRequest(2 * minReqSize, "foo", 1, 1,
        false);
    ResourceRequest request3 = createResourceRequest(minReqSize, "bar", 1, 2,
        false);
    ask2.add(request2);
    ask2.add(request3);
    scheduler.allocate(id21, ask2, new ArrayList<ContainerId>(), null, null);

    // Third ask, queue2 requests 1 large
    List<ResourceRequest> ask3 = new ArrayList<ResourceRequest>();
    ResourceRequest request4 =
        createResourceRequest(2 * minReqSize, ResourceRequest.ANY, 1, 1, true);
    ask3.add(request4);
    scheduler.allocate(id22, ask3, new ArrayList<ContainerId>(), null, null);

    scheduler.update();

    assertEquals(2 * minReqSize, scheduler.getQueueManager().getQueue("root.queue1")
        .getDemand().getMemory());
    assertEquals(2 * minReqSize + 2 * minReqSize + (2 * minReqSize), scheduler
        .getQueueManager().getQueue("root.queue2").getDemand()
        .getMemory());
  }

  @Test
  public void testAppAdditionAndRemoval() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    ApplicationAttemptId attemptId =createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(attemptId.getApplicationId(), "default",
      "user1");
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attemptAddedEvent =
        new AppAttemptAddedSchedulerEvent(createAppAttemptId(1, 1), false);
    scheduler.handle(attemptAddedEvent);

    // Scheduler should have two queues (the default and the one created for user1)
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());

    // That queue should have one app
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getRunnableAppSchedulables().size());

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        createAppAttemptId(1, 1), RMAppAttemptState.FINISHED, false);

    // Now remove app
    scheduler.handle(appRemovedEvent1);

    // Queue should have no apps
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getRunnableAppSchedulables().size());
  }

  @Test
  public void testHierarchicalQueueAllocationFileParsing() throws IOException, SAXException, 
      AllocationConfigurationException, ParserConfigurationException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("<queue name=\"queueC\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    QueueManager queueManager = scheduler.getQueueManager();
    Collection<FSLeafQueue> leafQueues = queueManager.getLeafQueues();
    Assert.assertEquals(4, leafQueues.size());
    Assert.assertNotNull(queueManager.getLeafQueue("queueA", false));
    Assert.assertNotNull(queueManager.getLeafQueue("queueB.queueC", false));
    Assert.assertNotNull(queueManager.getLeafQueue("queueB.queueD", false));
    Assert.assertNotNull(queueManager.getLeafQueue("default", false));
    // Make sure querying for queues didn't create any new ones:
    Assert.assertEquals(4, leafQueues.size());
  }
  
  @Test
  public void testConfigureRootQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>");
    out.println("<queue name=\"root\">");
    out.println("  <schedulingPolicy>drf</schedulingPolicy>");
    out.println("  <queue name=\"child1\">");
    out.println("    <minResources>1024mb,1vcores</minResources>");
    out.println("  </queue>");
    out.println("  <queue name=\"child2\">");
    out.println("    <minResources>1024mb,4vcores</minResources>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    QueueManager queueManager = scheduler.getQueueManager();
    
    FSQueue root = queueManager.getRootQueue();
    assertTrue(root.getPolicy() instanceof DominantResourceFairnessPolicy);
    
    assertNotNull(queueManager.getLeafQueue("child1", false));
    assertNotNull(queueManager.getLeafQueue("child2", false));
  }
  
  @Test (timeout = 5000)
  public void testIsStarvedForMinShare() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 3 * 1024. Node update gives this all to A
    createSchedulingRequest(3 * 1024, "queueA", "user1");
    scheduler.update();
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    // Queue B arrives and wants 1 * 1024
    createSchedulingRequest(1 * 1024, "queueB", "user1");
    scheduler.update();
    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    // Queue A should be above min share, B below.
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueA")) {
        assertEquals(false, scheduler.isStarvedForMinShare(p));
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(true, scheduler.isStarvedForMinShare(p));
      }
    }

    // Node checks in again, should allocate for B
    scheduler.handle(nodeEvent2);
    // Now B should have min share ( = demand here)
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueB")) {
        assertEquals(false, scheduler.isStarvedForMinShare(p));
      }
    }
  }

  @Test (timeout = 5000)
  public void testIsStarvedForFairShare() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.75</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 3 * 1024. Node update gives this all to A
    createSchedulingRequest(3 * 1024, "queueA", "user1");
    scheduler.update();
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    // Queue B arrives and wants 1 * 1024
    createSchedulingRequest(1 * 1024, "queueB", "user1");
    scheduler.update();
    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    // Queue A should be above fair share, B below.
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueA")) {
        assertEquals(false, scheduler.isStarvedForFairShare(p));
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(true, scheduler.isStarvedForFairShare(p));
      }
    }

    // Node checks in again, should allocate for B
    scheduler.handle(nodeEvent2);
    // B should not be starved for fair share, since entire demand is
    // satisfied.
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueB")) {
        assertEquals(false, scheduler.isStarvedForFairShare(p));
      }
    }
  }

  @Test (timeout = 5000)
  /**
   * Make sure containers are chosen to be preempted in the correct order. Right
   * now this means decreasing order of priority.
   */
  public void testChoiceOfPreemptedContainers() throws Exception {
    conf.setLong(FairSchedulerConfiguration.PREEMPTION_INTERVAL, 5000);
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10000); 
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE + ".allocation.file", ALLOC_FILE);
    
    MockClock clock = new MockClock();
    scheduler.setClock(clock);
    
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create four nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 2,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
        MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 3,
            "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);


    // Queue A and B each request three containers
    ApplicationAttemptId app1 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    ApplicationAttemptId app2 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 2);
    ApplicationAttemptId app3 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 3);

    ApplicationAttemptId app4 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 1);
    ApplicationAttemptId app5 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 2);
    ApplicationAttemptId app6 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 3);

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 2; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);
    }

    assertEquals(1, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app5).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app6).getLiveContainers().size());

    // Now new requests arrive from queues C and D
    ApplicationAttemptId app7 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    ApplicationAttemptId app8 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    ApplicationAttemptId app9 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    ApplicationAttemptId app10 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 1);
    ApplicationAttemptId app11 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 2);
    ApplicationAttemptId app12 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 3);

    scheduler.update();

    // We should be able to claw back one container from A and B each.
    // Make sure it is lowest priority container.
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));
    assertEquals(1, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app5).getLiveContainers().size());
    
    // First verify we are adding containers to preemption list for the application
    assertTrue(!Collections.disjoint(scheduler.getSchedulerApp(app3).getLiveContainers(),
                                     scheduler.getSchedulerApp(app3).getPreemptionContainers()));
    assertTrue(!Collections.disjoint(scheduler.getSchedulerApp(app6).getLiveContainers(),
                                     scheduler.getSchedulerApp(app6).getPreemptionContainers()));

    // Pretend 15 seconds have passed
    clock.tick(15);

    // Trigger a kill by insisting we want containers back
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));

    // At this point the containers should have been killed (since we are not simulating AM)
    assertEquals(0, scheduler.getSchedulerApp(app6).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app3).getLiveContainers().size());

    // Trigger a kill by insisting we want containers back
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));

    // Pretend 15 seconds have passed
    clock.tick(15);

    // We should be able to claw back another container from A and B each.
    // Make sure it is lowest priority container.
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));
    
    assertEquals(1, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app5).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app6).getLiveContainers().size());

    // Now A and B are below fair share, so preemption shouldn't do anything
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));
    assertEquals(1, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app5).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app6).getLiveContainers().size());
  }

  @Test (timeout = 5000)
  /**
   * Tests the timing of decision to preempt tasks.
   */
  public void testPreemptionDecision() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    MockClock clock = new MockClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<maxResources>0mb,0vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>10</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create four nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 2,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
        MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024, 2), 3,
            "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);


    // Queue A and B each request three containers
    ApplicationAttemptId app1 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    ApplicationAttemptId app2 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 2);
    ApplicationAttemptId app3 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 3);

    ApplicationAttemptId app4 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 1);
    ApplicationAttemptId app5 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 2);
    ApplicationAttemptId app6 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 3);

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 2; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);
    }

    // Now new requests arrive from queues C and D
    ApplicationAttemptId app7 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    ApplicationAttemptId app8 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    ApplicationAttemptId app9 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    ApplicationAttemptId app10 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 1);
    ApplicationAttemptId app11 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 2);
    ApplicationAttemptId app12 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 3);

    scheduler.update();

    FSLeafQueue schedC =
        scheduler.getQueueManager().getLeafQueue("queueC", true);
    FSLeafQueue schedD =
        scheduler.getQueueManager().getLeafQueue("queueD", true);

    assertTrue(Resources.equals(
        Resources.none(), scheduler.resToPreempt(schedC, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.none(), scheduler.resToPreempt(schedD, clock.getTime())));
    // After minSharePreemptionTime has passed, they should want to preempt min
    // share.
    clock.tick(6);
    assertEquals(
        1024, scheduler.resToPreempt(schedC, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resToPreempt(schedD, clock.getTime()).getMemory());

    // After fairSharePreemptionTime has passed, they should want to preempt
    // fair share.
    scheduler.update();
    clock.tick(6);
    assertEquals(
        1536 , scheduler.resToPreempt(schedC, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resToPreempt(schedD, clock.getTime()).getMemory());
  }
  
  @Test (timeout = 5000)
  public void testMultipleContainersWaitingForReservation() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Request full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue2", "user2", 1);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue3", "user3", 1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // One container should get reservation and the other should get nothing
    assertEquals(1024,
        scheduler.getSchedulerApp(attId1).getCurrentReservation().getMemory());
    assertEquals(0,
        scheduler.getSchedulerApp(attId2).getCurrentReservation().getMemory());
  }

  @Test (timeout = 5000)
  public void testUserMaxRunningApps() throws Exception {
    // Set max running apps
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>1</maxRunningApps>");
    out.println("</user>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Request for app 1
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 1);
    
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    
    // App 1 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
    
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // App 2 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
    
    // Request another container for app 1
    createSchedulingRequestExistingApplication(1024, 1, attId1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // Request should be fulfilled
    assertEquals(2, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
  }
  
  @Test (timeout = 5000)
  public void testReservationWhileMultiplePriorities() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId attId = createSchedulingRequest(1024, 4, "queue1",
        "user1", 1, 2);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    
    FSSchedulerApp app = scheduler.getSchedulerApp(attId);
    assertEquals(1, app.getLiveContainers().size());
    
    ContainerId containerId = scheduler.getSchedulerApp(attId)
        .getLiveContainers().iterator().next().getContainerId();

    // Cause reservation to be created
    createSchedulingRequestExistingApplication(1024, 4, 2, attId);
    scheduler.update();
    scheduler.handle(updateEvent);

    assertEquals(1, app.getLiveContainers().size());
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    
    // Create request at higher priority
    createSchedulingRequestExistingApplication(1024, 4, 1, attId);
    scheduler.update();
    scheduler.handle(updateEvent);
    
    assertEquals(1, app.getLiveContainers().size());
    // Reserved container should still be at lower priority
    for (RMContainer container : app.getReservedContainers()) {
      assertEquals(2, container.getReservedPriority().getPriority());
    }
    
    // Complete container
    scheduler.allocate(attId, new ArrayList<ResourceRequest>(),
        Arrays.asList(containerId), null, null);
    assertEquals(1024, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(4, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    
    // Schedule at opening
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // Reserved container (at lower priority) should be run
    Collection<RMContainer> liveContainers = app.getLiveContainers();
    assertEquals(1, liveContainers.size());
    for (RMContainer liveContainer : liveContainers) {
      Assert.assertEquals(2, liveContainer.getContainer().getPriority().getPriority());
    }
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
  }
  
  @Test
  public void testAclSubmitApplication() throws Exception {
    // Set acl's
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("  <aclSubmitApps> </aclSubmitApps>");
    out.println("  <aclAdministerApps> </aclAdministerApps>");
    out.println("  <queue name=\"queue1\">");
    out.println("    <aclSubmitApps>norealuserhasthisname</aclSubmitApps>");
    out.println("    <aclAdministerApps>norealuserhasthisname</aclAdministerApps>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname", 1);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname2", 1);

    FSSchedulerApp app1 = scheduler.getSchedulerApp(attId1);
    assertNotNull("The application was not allowed", app1);
    FSSchedulerApp app2 = scheduler.getSchedulerApp(attId2);
    assertNull("The application was allowed", app2);
  }
  
  @Test (timeout = 5000)
  public void testMultipleNodesSingleRackRequest() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    RMNode node3 =
        MockNodes
            .newNodeInfo(2, Resources.createResource(1024), 3, "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    
    ApplicationAttemptId appId = createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    scheduler.addApplication(appId.getApplicationId(), "queue1", "user1");
    scheduler.addApplicationAttempt(appId, false);
    
    // 1 request with 2 nodes on the same rack. another request with 1 node on
    // a different rack
    List<ResourceRequest> asks = new ArrayList<ResourceRequest>();
    asks.add(createResourceRequest(1024, node1.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node2.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node3.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node1.getRackName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node3.getRackName(), 1, 1, true));
    asks.add(createResourceRequest(1024, ResourceRequest.ANY, 1, 2, true));

    scheduler.allocate(appId, asks, new ArrayList<ContainerId>(), null, null);
    
    // node 1 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent1);
    // should assign node local
    assertEquals(1, scheduler.getSchedulerApp(appId).getLiveContainers().size());

    // node 2 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);
    // should assign rack local
    assertEquals(2, scheduler.getSchedulerApp(appId).getLiveContainers().size());
  }
  
  @Test (timeout = 5000)
  public void testFifoWithinQueue() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(3072, 3), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Even if submitted at exact same time, apps will be deterministically
    // ordered by name.
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 2);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 2);
    FSSchedulerApp app1 = scheduler.getSchedulerApp(attId1);
    FSSchedulerApp app2 = scheduler.getSchedulerApp(attId2);
    
    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1", true);
    queue1.setPolicy(new FifoPolicy());
    
    scheduler.update();

    // First two containers should go to app 1, third should go to app 2.
    // Because tests set assignmultiple to false, each heartbeat assigns a single
    // container.
    
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);

    scheduler.handle(updateEvent);
    assertEquals(1, app1.getLiveContainers().size());
    assertEquals(0, app2.getLiveContainers().size());
    
    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size());
    assertEquals(0, app2.getLiveContainers().size());
    
    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size());
    assertEquals(1, app2.getLiveContainers().size());
  }

  @Test(timeout = 3000)
  public void testMaxAssign() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(16384, 16), 0,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId attId =
        createSchedulingRequest(1024, "root.default", "user", 8);
    FSSchedulerApp app = scheduler.getSchedulerApp(attId);

    // set maxAssign to 2: only 2 containers should be allocated
    scheduler.maxAssign = 2;
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Incorrect number of containers allocated", 2, app
        .getLiveContainers().size());

    // set maxAssign to -1: all remaining containers should be allocated
    scheduler.maxAssign = -1;
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Incorrect number of containers allocated", 8, app
        .getLiveContainers().size());
  }

  /**
   * Test to verify the behavior of
   * {@link FSQueue#assignContainer(FSSchedulerNode)})
   * 
   * Create two queues under root (fifoQueue and fairParent), and two queues
   * under fairParent (fairChild1 and fairChild2). Submit two apps to the
   * fifoQueue and one each to the fairChild* queues, all apps requiring 4
   * containers each of the total 16 container capacity
   * 
   * Assert the number of containers for each app after 4, 8, 12 and 16 updates.
   * 
   * @throws Exception
   */
  @Test(timeout = 5000)
  public void testAssignContainer() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    final String user = "user1";
    final String fifoQueue = "fifo";
    final String fairParent = "fairParent";
    final String fairChild1 = fairParent + ".fairChild1";
    final String fairChild2 = fairParent + ".fairChild2";

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 1, "127.0.0.1");
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 2, "127.0.0.2");

    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);

    scheduler.handle(nodeEvent1);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 =
        createSchedulingRequest(1024, fifoQueue, user, 4);
    ApplicationAttemptId attId2 =
        createSchedulingRequest(1024, fairChild1, user, 4);
    ApplicationAttemptId attId3 =
        createSchedulingRequest(1024, fairChild2, user, 4);
    ApplicationAttemptId attId4 =
        createSchedulingRequest(1024, fifoQueue, user, 4);

    FSSchedulerApp app1 = scheduler.getSchedulerApp(attId1);
    FSSchedulerApp app2 = scheduler.getSchedulerApp(attId2);
    FSSchedulerApp app3 = scheduler.getSchedulerApp(attId3);
    FSSchedulerApp app4 = scheduler.getSchedulerApp(attId4);

    scheduler.getQueueManager().getLeafQueue(fifoQueue, true)
        .setPolicy(SchedulingPolicy.parse("fifo"));
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);

    for (int i = 0; i < 8; i++) {
      scheduler.handle(updateEvent1);
      scheduler.handle(updateEvent2);
      if ((i + 1) % 2 == 0) {
        // 4 node updates: fifoQueue should have received 2, and fairChild*
        // should have received one each
        String ERR =
            "Wrong number of assigned containers after " + (i + 1) + " updates";
        if (i < 4) {
          // app1 req still not met
          assertEquals(ERR, (i + 1), app1.getLiveContainers().size());
          assertEquals(ERR, 0, app4.getLiveContainers().size());
        } else {
          // app1 req has been met, app4 should be served now
          assertEquals(ERR, 4, app1.getLiveContainers().size());
          assertEquals(ERR, (i - 3), app4.getLiveContainers().size());
        }
        assertEquals(ERR, (i + 1) / 2, app2.getLiveContainers().size());
        assertEquals(ERR, (i + 1) / 2, app3.getLiveContainers().size());
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testNotAllowSubmitApplication() throws Exception {
    // Set acl's
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("  <aclSubmitApps> </aclSubmitApps>");
    out.println("  <aclAdministerApps> </aclAdministerApps>");
    out.println("  <queue name=\"queue1\">");
    out.println("    <aclSubmitApps>userallow</aclSubmitApps>");
    out.println("    <aclAdministerApps>userallow</aclAdministerApps>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    int appId = this.APP_ID++;
    String user = "usernotallow";
    String queue = "queue1";
    ApplicationId applicationId = MockApps.newAppID(appId);
    String name = MockApps.newAppName();
    ApplicationMasterService masterService =
        new ApplicationMasterService(resourceManager.getRMContext(), scheduler);
    ApplicationSubmissionContext submissionContext = new ApplicationSubmissionContextPBImpl();
    ContainerLaunchContext clc =
        BuilderUtils.newContainerLaunchContext(null, null, null, null,
            null, null);
    submissionContext.setApplicationId(applicationId);
    submissionContext.setAMContainerSpec(clc);
    RMApp application =
        new RMAppImpl(applicationId, resourceManager.getRMContext(), conf, name, user, 
          queue, submissionContext, scheduler, masterService,
          System.currentTimeMillis(), "YARN", null);
    resourceManager.getRMContext().getRMApps().putIfAbsent(applicationId, application);
    application.handle(new RMAppEvent(applicationId, RMAppEventType.START));

    final int MAX_TRIES=20;
    int numTries = 0;
    while (!application.getState().equals(RMAppState.SUBMITTED) &&
        numTries < MAX_TRIES) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {ex.printStackTrace();}
      numTries++;
    }
    assertEquals("The application doesn't reach SUBMITTED.",
        RMAppState.SUBMITTED, application.getState());

    ApplicationAttemptId attId =
        ApplicationAttemptId.newInstance(applicationId, this.ATTEMPT_ID++);
    scheduler.addApplication(attId.getApplicationId(), queue, user);

    numTries = 0;
    while (application.getFinishTime() == 0 && numTries < MAX_TRIES) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {ex.printStackTrace();}
      numTries++;
    }
    assertEquals(FinalApplicationStatus.FAILED, application.getFinalApplicationStatus());
  }
  
  @Test
  public void testReservationThatDoesntFit() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    ApplicationAttemptId attId = createSchedulingRequest(2048, "queue1",
        "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    
    FSSchedulerApp app = scheduler.getSchedulerApp(attId);
    assertEquals(0, app.getLiveContainers().size());
    assertEquals(0, app.getReservedContainers().size());
    
    createSchedulingRequestExistingApplication(1024, 2, attId);
    scheduler.update();
    scheduler.handle(updateEvent);
    
    assertEquals(1, app.getLiveContainers().size());
    assertEquals(0, app.getReservedContainers().size());
  }
  
  @Test
  public void testRemoveNodeUpdatesRootQueueMetrics() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
	assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024, 4), 1,
        "127.0.0.1");
    NodeAddedSchedulerEvent addEvent = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(addEvent);
    
    assertEquals(1024, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(4, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    scheduler.update(); // update shouldn't change things
    assertEquals(1024, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(4, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    
    NodeRemovedSchedulerEvent removeEvent = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(removeEvent);
    
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    scheduler.update(); // update shouldn't change things
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
}

  @Test
  public void testStrictLocality() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 0);
    
    ResourceRequest nodeRequest = createResourceRequest(1024, node1.getHostName(), 1, 1, true);
    ResourceRequest rackRequest = createResourceRequest(1024, node1.getRackName(), 1, 1, false);
    ResourceRequest anyRequest = createResourceRequest(1024, ResourceRequest.ANY,
        1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId1);
    createSchedulingRequestExistingApplication(rackRequest, attId1);
    createSchedulingRequestExistingApplication(anyRequest, attId1);

    scheduler.update();

    NodeUpdateSchedulerEvent node1UpdateEvent = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent node2UpdateEvent = new NodeUpdateSchedulerEvent(node2);

    // no matter how many heartbeats, node2 should never get a container
    FSSchedulerApp app = scheduler.getSchedulerApp(attId1);
    for (int i = 0; i < 10; i++) {
      scheduler.handle(node2UpdateEvent);
      assertEquals(0, app.getLiveContainers().size());
      assertEquals(0, app.getReservedContainers().size());
    }
    // then node1 should get the container
    scheduler.handle(node1UpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }
  
  @Test
  public void testCancelStrictLocality() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 0);
    
    ResourceRequest nodeRequest = createResourceRequest(1024, node1.getHostName(), 1, 1, true);
    ResourceRequest rackRequest = createResourceRequest(1024, "rack1", 1, 1, false);
    ResourceRequest anyRequest = createResourceRequest(1024, ResourceRequest.ANY,
        1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId1);
    createSchedulingRequestExistingApplication(rackRequest, attId1);
    createSchedulingRequestExistingApplication(anyRequest, attId1);

    scheduler.update();

    NodeUpdateSchedulerEvent node2UpdateEvent = new NodeUpdateSchedulerEvent(node2);

    // no matter how many heartbeats, node2 should never get a container
    FSSchedulerApp app = scheduler.getSchedulerApp(attId1);
    for (int i = 0; i < 10; i++) {
      scheduler.handle(node2UpdateEvent);
      assertEquals(0, app.getLiveContainers().size());
    }
    
    // relax locality
    List<ResourceRequest> update = Arrays.asList(
        createResourceRequest(1024, node1.getHostName(), 1, 0, true),
        createResourceRequest(1024, "rack1", 1, 0, true),
        createResourceRequest(1024, ResourceRequest.ANY, 1, 1, true));
    scheduler.allocate(attId1, update, new ArrayList<ContainerId>(), null, null);
    
    // then node2 should get the container
    scheduler.handle(node2UpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  /**
   * If we update our ask to strictly request a node, it doesn't make sense to keep
   * a reservation on another.
   */
  @Test
  public void testReservationsStrictLocality() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue1",
        "user1", 0);
    FSSchedulerApp app = scheduler.getSchedulerApp(attId);
    
    ResourceRequest nodeRequest = createResourceRequest(1024, node2.getHostName(), 1, 2, true);
    ResourceRequest rackRequest = createResourceRequest(1024, "rack1", 1, 2, true);
    ResourceRequest anyRequest = createResourceRequest(1024, ResourceRequest.ANY,
        1, 2, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId);
    createSchedulingRequestExistingApplication(rackRequest, attId);
    createSchedulingRequestExistingApplication(anyRequest, attId);
    
    scheduler.update();

    NodeUpdateSchedulerEvent nodeUpdateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeUpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
    scheduler.handle(nodeUpdateEvent);
    assertEquals(1, app.getReservedContainers().size());
    
    // now, make our request node-specific (on a different node)
    rackRequest = createResourceRequest(1024, "rack1", 1, 1, false);
    anyRequest = createResourceRequest(1024, ResourceRequest.ANY,
        1, 1, false);
    scheduler.allocate(attId, Arrays.asList(rackRequest, anyRequest),
        new ArrayList<ContainerId>(), null, null);

    scheduler.handle(nodeUpdateEvent);
    assertEquals(0, app.getReservedContainers().size());
  }
  
  @Test
  public void testNoMoreCpuOnNode() throws IOException {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2048, 1),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    ApplicationAttemptId attId = createSchedulingRequest(1024, 1, "default",
        "user1", 2);
    FSSchedulerApp app = scheduler.getSchedulerApp(attId);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    assertEquals(1, app.getLiveContainers().size());
    scheduler.handle(updateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  @Test
  public void testBasicDRFAssignment() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, BuilderUtils.newResource(8192, 5));
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(2048, 1, "queue1",
        "user1", 2);
    FSSchedulerApp app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(1024, 2, "queue1",
        "user1", 2);
    FSSchedulerApp app2 = scheduler.getSchedulerApp(appAttId2);

    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getClusterCapacity());
    scheduler.getQueueManager().getQueue("queue1").setPolicy(drfPolicy);
    scheduler.update();

    // First both apps get a container
    // Then the first gets another container because its dominant share of
    // 2048/8192 is less than the other's of 2/5
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(updateEvent);
    Assert.assertEquals(1, app1.getLiveContainers().size());
    Assert.assertEquals(0, app2.getLiveContainers().size());

    scheduler.handle(updateEvent);
    Assert.assertEquals(1, app1.getLiveContainers().size());
    Assert.assertEquals(1, app2.getLiveContainers().size());

    scheduler.handle(updateEvent);
    Assert.assertEquals(2, app1.getLiveContainers().size());
    Assert.assertEquals(1, app2.getLiveContainers().size());
  }

  /**
   * Two apps on one queue, one app on another
   */
  @Test
  public void testBasicDRFWithQueues() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, BuilderUtils.newResource(8192, 7),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(3072, 1, "queue1",
        "user1", 2);
    FSSchedulerApp app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(2048, 2, "queue1",
        "user1", 2);
    FSSchedulerApp app2 = scheduler.getSchedulerApp(appAttId2);
    ApplicationAttemptId appAttId3 = createSchedulingRequest(1024, 2, "queue2",
        "user1", 2);
    FSSchedulerApp app3 = scheduler.getSchedulerApp(appAttId3);
    
    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getClusterCapacity());
    scheduler.getQueueManager().getQueue("root").setPolicy(drfPolicy);
    scheduler.getQueueManager().getQueue("queue1").setPolicy(drfPolicy);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(updateEvent);
    Assert.assertEquals(1, app1.getLiveContainers().size());
    scheduler.handle(updateEvent);
    Assert.assertEquals(1, app3.getLiveContainers().size());
    scheduler.handle(updateEvent);
    Assert.assertEquals(2, app3.getLiveContainers().size());
    scheduler.handle(updateEvent);
    Assert.assertEquals(1, app2.getLiveContainers().size());
  }
  
  @Test
  public void testDRFHierarchicalQueues() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, BuilderUtils.newResource(12288, 12),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(3074, 1, "queue1.subqueue1",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSSchedulerApp app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(1024, 3, "queue1.subqueue1",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSSchedulerApp app2 = scheduler.getSchedulerApp(appAttId2);
    ApplicationAttemptId appAttId3 = createSchedulingRequest(2048, 2, "queue1.subqueue2",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSSchedulerApp app3 = scheduler.getSchedulerApp(appAttId3);
    ApplicationAttemptId appAttId4 = createSchedulingRequest(1024, 2, "queue2",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSSchedulerApp app4 = scheduler.getSchedulerApp(appAttId4);
    
    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getClusterCapacity());
    scheduler.getQueueManager().getQueue("root").setPolicy(drfPolicy);
    scheduler.getQueueManager().getQueue("queue1").setPolicy(drfPolicy);
    scheduler.getQueueManager().getQueue("queue1.subqueue1").setPolicy(drfPolicy);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(updateEvent);
    // app1 gets first container because it asked first
    Assert.assertEquals(1, app1.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app4 gets second container because it's on queue2
    Assert.assertEquals(1, app4.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app4 gets another container because queue2's dominant share of memory
    // is still less than queue1's of cpu
    Assert.assertEquals(2, app4.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app3 gets one because queue1 gets one and queue1.subqueue2 is behind
    // queue1.subqueue1
    Assert.assertEquals(1, app3.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app4 would get another one, but it doesn't have any requests
    // queue1.subqueue2 is still using less than queue1.subqueue1, so it
    // gets another
    Assert.assertEquals(2, app3.getLiveContainers().size());
    // queue1.subqueue1 is behind again, so it gets one, which it gives to app2
    scheduler.handle(updateEvent);
    Assert.assertEquals(1, app2.getLiveContainers().size());
    
    // at this point, we've used all our CPU up, so nobody else should get a container
    scheduler.handle(updateEvent);

    Assert.assertEquals(1, app1.getLiveContainers().size());
    Assert.assertEquals(1, app2.getLiveContainers().size());
    Assert.assertEquals(2, app3.getLiveContainers().size());
    Assert.assertEquals(2, app4.getLiveContainers().size());
  }

  @Test(timeout = 30000)
  public void testHostPortNodeName() throws Exception {
    conf.setBoolean(YarnConfiguration
        .RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
    scheduler.reinitialize(conf, 
        resourceManager.getRMContext());
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024),
        1, "127.0.0.1", 1);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024),
        2, "127.0.0.1", 2);
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", 
        "user1", 0);

    ResourceRequest nodeRequest = createResourceRequest(1024, 
        node1.getNodeID().getHost() + ":" + node1.getNodeID().getPort(), 1,
        1, true);
    ResourceRequest rackRequest = createResourceRequest(1024, 
        node1.getRackName(), 1, 1, false);
    ResourceRequest anyRequest = createResourceRequest(1024, 
        ResourceRequest.ANY, 1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId1);
    createSchedulingRequestExistingApplication(rackRequest, attId1);
    createSchedulingRequestExistingApplication(anyRequest, attId1);

    scheduler.update();

    NodeUpdateSchedulerEvent node1UpdateEvent = new 
        NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent node2UpdateEvent = new 
        NodeUpdateSchedulerEvent(node2);

    // no matter how many heartbeats, node2 should never get a container  
    FSSchedulerApp app = scheduler.getSchedulerApp(attId1);
    for (int i = 0; i < 10; i++) {
      scheduler.handle(node2UpdateEvent);
      assertEquals(0, app.getLiveContainers().size());
      assertEquals(0, app.getReservedContainers().size());
    }
    // then node1 should get the container  
    scheduler.handle(node1UpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  private void verifyAppRunnable(ApplicationAttemptId attId, boolean runnable) {
    FSSchedulerApp app = scheduler.getSchedulerApp(attId);
    FSLeafQueue queue = app.getQueue();
    Collection<AppSchedulable> runnableApps =
        queue.getRunnableAppSchedulables();
    Collection<AppSchedulable> nonRunnableApps =
        queue.getNonRunnableAppSchedulables();
    assertEquals(runnable, runnableApps.contains(app.getAppSchedulable()));
    assertEquals(!runnable, nonRunnableApps.contains(app.getAppSchedulable()));
  }
  
  private void verifyQueueNumRunnable(String queueName, int numRunnableInQueue,
      int numNonRunnableInQueue) {
    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(queueName, false);
    assertEquals(numRunnableInQueue,
        queue.getRunnableAppSchedulables().size());
    assertEquals(numNonRunnableInQueue,
        queue.getNonRunnableAppSchedulables().size());
  }
  
  @Test
  public void testUserAndQueueMaxRunningApps() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("<maxRunningApps>2</maxRunningApps>");
    out.println("</queue>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>1</maxRunningApps>");
    out.println("</user>");
    out.println("</allocations>");
    out.close();
    
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    // exceeds no limits
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1");
    verifyAppRunnable(attId1, true);
    verifyQueueNumRunnable("queue1", 1, 0);
    // exceeds user limit
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue2", "user1");
    verifyAppRunnable(attId2, false);
    verifyQueueNumRunnable("queue2", 0, 1);
    // exceeds no limits
    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1", "user2");
    verifyAppRunnable(attId3, true);
    verifyQueueNumRunnable("queue1", 2, 0);
    // exceeds queue limit
    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1", "user2");
    verifyAppRunnable(attId4, false);
    verifyQueueNumRunnable("queue1", 2, 1);
    
    // Remove app 1 and both app 2 and app 4 should becomes runnable in its place
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
    verifyAppRunnable(attId2, true);
    verifyQueueNumRunnable("queue2", 1, 0);
    verifyAppRunnable(attId4, true);
    verifyQueueNumRunnable("queue1", 2, 0);
    
    // A new app to queue1 should not be runnable
    ApplicationAttemptId attId5 = createSchedulingRequest(1024, "queue1", "user2");
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1", 2, 1);
  }
  
  @Test
  public void testMaxRunningAppsHierarchicalQueues() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    MockClock clock = new MockClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("  <maxRunningApps>3</maxRunningApps>");
    out.println("  <queue name=\"sub1\"></queue>");
    out.println("  <queue name=\"sub2\"></queue>");
    out.println("  <queue name=\"sub3\">");
    out.println("    <maxRunningApps>1</maxRunningApps>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    // exceeds no limits
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1.sub1", "user1");
    verifyAppRunnable(attId1, true);
    verifyQueueNumRunnable("queue1.sub1", 1, 0);
    clock.tick(10);
    // exceeds no limits
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId2, true);
    verifyQueueNumRunnable("queue1.sub3", 1, 0);
    clock.tick(10);
    // exceeds no limits
    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId3, true);
    verifyQueueNumRunnable("queue1.sub2", 1, 0);
    clock.tick(10);
    // exceeds queue1 limit
    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId4, false);
    verifyQueueNumRunnable("queue1.sub2", 1, 1);
    clock.tick(10);
    // exceeds sub3 limit
    ApplicationAttemptId attId5 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1.sub3", 1, 1);
    clock.tick(10);
    
    // Even though the app was removed from sub3, the app from sub2 gets to go
    // because it came in first
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId2, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
    verifyAppRunnable(attId4, true);
    verifyQueueNumRunnable("queue1.sub2", 2, 0);
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1.sub3", 0, 1);

    // Now test removal of a non-runnable app
    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId5, RMAppAttemptState.KILLED, true);
    scheduler.handle(appRemovedEvent2);
    assertEquals(0, scheduler.maxRunningEnforcer.usersNonRunnableApps
        .get("user1").size());
    // verify app gone in queue accounting
    verifyQueueNumRunnable("queue1.sub3", 0, 0);
    // verify it doesn't become runnable when there would be space for it
    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(attId4, RMAppAttemptState.FINISHED, true);
    scheduler.handle(appRemovedEvent3);
    verifyQueueNumRunnable("queue1.sub2", 1, 0);
    verifyQueueNumRunnable("queue1.sub3", 0, 0);
  }

  @Test (timeout = 10000)
  public void testContinuousScheduling() throws Exception {
    // set continuous scheduling enabled
    FairScheduler fs = new FairScheduler();
    Configuration conf = createConfiguration();
    conf.setBoolean(FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED,
            true);
    fs.reinitialize(conf, resourceManager.getRMContext());
    Assert.assertTrue("Continuous scheduling should be enabled.",
            fs.isContinuousSchedulingEnabled());

    // Add two nodes
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    fs.handle(nodeEvent1);
    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    fs.handle(nodeEvent2);

    // available resource
    Assert.assertEquals(fs.getClusterCapacity().getMemory(), 16 * 1024);
    Assert.assertEquals(fs.getClusterCapacity().getVirtualCores(), 16);

    // send application request
    ApplicationAttemptId appAttemptId =
            createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    fs.addApplication(appAttemptId.getApplicationId(), "queue11", "user11");
    fs.addApplicationAttempt(appAttemptId, false);
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest request =
            createResourceRequest(1024, 1, ResourceRequest.ANY, 1, 1, true);
    ask.add(request);
    fs.allocate(appAttemptId, ask, new ArrayList<ContainerId>(), null, null);

    // waiting for continuous_scheduler_sleep_time
    // at least one pass
    Thread.sleep(fs.getConf().getContinuousSchedulingSleepMs() + 500);

    FSSchedulerApp app = fs.getSchedulerApp(appAttemptId);
    // Wait until app gets resources.
    while (app.getCurrentConsumption().equals(Resources.none())) { }

    // check consumption
    Assert.assertEquals(1024, app.getCurrentConsumption().getMemory());
    Assert.assertEquals(1, app.getCurrentConsumption().getVirtualCores());

    // another request
    request =
            createResourceRequest(1024, 1, ResourceRequest.ANY, 2, 1, true);
    ask.clear();
    ask.add(request);
    fs.allocate(appAttemptId, ask, new ArrayList<ContainerId>(), null, null);

    // Wait until app gets resources
    while (app.getCurrentConsumption()
            .equals(Resources.createResource(1024, 1))) { }

    Assert.assertEquals(2048, app.getCurrentConsumption().getMemory());
    Assert.assertEquals(2, app.getCurrentConsumption().getVirtualCores());

    // 2 containers should be assigned to 2 nodes
    Set<NodeId> nodes = new HashSet<NodeId>();
    Iterator<RMContainer> it = app.getLiveContainers().iterator();
    while (it.hasNext()) {
      nodes.add(it.next().getContainer().getNodeId());
    }
    Assert.assertEquals(2, nodes.size());
  }

  
  @Test
  public void testDontAllowUndeclaredPools() throws Exception{
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"jerry\">");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());
    QueueManager queueManager = scheduler.getQueueManager();
    
    FSLeafQueue jerryQueue = queueManager.getLeafQueue("jerry", false);
    FSLeafQueue defaultQueue = queueManager.getLeafQueue("default", false);
    
    // Should get put into jerry
    createSchedulingRequest(1024, "jerry", "someuser");
    assertEquals(1, jerryQueue.getRunnableAppSchedulables().size());

    // Should get forced into default
    createSchedulingRequest(1024, "newqueue", "someuser");
    assertEquals(1, jerryQueue.getRunnableAppSchedulables().size());
    assertEquals(1, defaultQueue.getRunnableAppSchedulables().size());
    
    // Would get put into someuser because of user-as-default-queue, but should
    // be forced into default
    createSchedulingRequest(1024, "default", "someuser");
    assertEquals(1, jerryQueue.getRunnableAppSchedulables().size());
    assertEquals(2, defaultQueue.getRunnableAppSchedulables().size());
    
    // Should get put into jerry because of user-as-default-queue
    createSchedulingRequest(1024, "default", "jerry");
    assertEquals(2, jerryQueue.getRunnableAppSchedulables().size());
    assertEquals(2, defaultQueue.getRunnableAppSchedulables().size());
  }

  @SuppressWarnings("resource")
  @Test
  public void testBlacklistNodes() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    final int GB = 1024;
    String host = "127.0.0.1";
    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(16 * GB, 16),
            0, host);
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttemptId =
        createSchedulingRequest(GB, "root.default", "user", 1);
    FSSchedulerApp app = scheduler.getSchedulerApp(appAttemptId);

    // Verify the blacklist can be updated independent of requesting containers
    scheduler.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null);
    assertTrue(app.isBlacklisted(host));
    scheduler.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host));
    assertFalse(scheduler.getSchedulerApp(appAttemptId).isBlacklisted(host));

    List<ResourceRequest> update = Arrays.asList(
        createResourceRequest(GB, node.getHostName(), 1, 0, true));

    // Verify a container does not actually get placed on the blacklisted host
    scheduler.allocate(appAttemptId, update,
        Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null);
    assertTrue(app.isBlacklisted(host));
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Incorrect number of containers allocated", 0, app
        .getLiveContainers().size());

    // Verify a container gets placed on the empty blacklist
    scheduler.allocate(appAttemptId, update,
        Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host));
    assertFalse(app.isBlacklisted(host));
    createSchedulingRequest(GB, "root.default", "user", 1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Incorrect number of containers allocated", 1, app
        .getLiveContainers().size());
  }
  
  @Test
  public void testGetAppsInQueue() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId appAttId1 =
        createSchedulingRequest(1024, 1, "queue1.subqueue1", "user1");
    ApplicationAttemptId appAttId2 =
        createSchedulingRequest(1024, 1, "queue1.subqueue2", "user1");
    ApplicationAttemptId appAttId3 =
        createSchedulingRequest(1024, 1, "default", "user1");
    
    List<ApplicationAttemptId> apps =
        scheduler.getAppsInQueue("queue1.subqueue1");
    assertEquals(1, apps.size());
    assertEquals(appAttId1, apps.get(0));
    // with and without root prefix should work
    apps = scheduler.getAppsInQueue("root.queue1.subqueue1");
    assertEquals(1, apps.size());
    assertEquals(appAttId1, apps.get(0));
    
    apps = scheduler.getAppsInQueue("user1");
    assertEquals(1, apps.size());
    assertEquals(appAttId3, apps.get(0));
    // with and without root prefix should work
    apps = scheduler.getAppsInQueue("root.user1");
    assertEquals(1, apps.size());
    assertEquals(appAttId3, apps.get(0));

    // apps in subqueues should be included
    apps = scheduler.getAppsInQueue("queue1");
    Assert.assertEquals(2, apps.size());
    Set<ApplicationAttemptId> appAttIds = Sets.newHashSet(apps.get(0), apps.get(1));
    assertTrue(appAttIds.contains(appAttId1));
    assertTrue(appAttIds.contains(appAttId2));
  }

  @Test
  public void testAddAndRemoveAppFromFairScheduler() throws Exception {
    FairScheduler scheduler =
        (FairScheduler) resourceManager.getResourceScheduler();
    TestSchedulerUtils.verifyAppAddedAndRemovedFromScheduler(
      scheduler.getSchedulerApplications(), scheduler, "default");
  }

  @Test
  public void testMoveRunnableApp() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    FSLeafQueue targetQueue = queueMgr.getLeafQueue("queue2", true);

    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    ApplicationId appId = appAttId.getApplicationId();
    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.handle(updateEvent);
    
    assertEquals(Resource.newInstance(1024, 1), oldQueue.getResourceUsage());
    scheduler.update();
    assertEquals(Resource.newInstance(3072, 3), oldQueue.getDemand());
    
    scheduler.moveApplication(appId, "queue2");
    FSSchedulerApp app = scheduler.getSchedulerApp(appAttId);
    assertSame(targetQueue, app.getQueue());
    assertFalse(oldQueue.getRunnableAppSchedulables()
        .contains(app.getAppSchedulable()));
    assertTrue(targetQueue.getRunnableAppSchedulables()
        .contains(app.getAppSchedulable()));
    assertEquals(Resource.newInstance(0, 0), oldQueue.getResourceUsage());
    assertEquals(Resource.newInstance(1024, 1), targetQueue.getResourceUsage());
    assertEquals(0, oldQueue.getNumRunnableApps());
    assertEquals(1, targetQueue.getNumRunnableApps());
    assertEquals(1, queueMgr.getRootQueue().getNumRunnableApps());
    
    scheduler.update();
    assertEquals(Resource.newInstance(0, 0), oldQueue.getDemand());
    assertEquals(Resource.newInstance(3072, 3), targetQueue.getDemand());
  }
  
  @Test
  public void testMoveNonRunnableApp() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    FSLeafQueue targetQueue = queueMgr.getLeafQueue("queue2", true);
    scheduler.getAllocationConfiguration().queueMaxApps.put("root.queue1", 0);
    scheduler.getAllocationConfiguration().queueMaxApps.put("root.queue2", 0);
    
    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    
    assertEquals(0, oldQueue.getNumRunnableApps());
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    assertEquals(0, oldQueue.getNumRunnableApps());
    assertEquals(0, targetQueue.getNumRunnableApps());
    assertEquals(0, queueMgr.getRootQueue().getNumRunnableApps());
  }
  
  @Test
  public void testMoveMakesAppRunnable() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    FSLeafQueue targetQueue = queueMgr.getLeafQueue("queue2", true);
    scheduler.getAllocationConfiguration().queueMaxApps.put("root.queue1", 0);
    
    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    
    FSSchedulerApp app = scheduler.getSchedulerApp(appAttId);
    assertTrue(oldQueue.getNonRunnableAppSchedulables()
        .contains(app.getAppSchedulable()));
    
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    assertFalse(oldQueue.getNonRunnableAppSchedulables()
        .contains(app.getAppSchedulable()));
    assertFalse(targetQueue.getNonRunnableAppSchedulables()
        .contains(app.getAppSchedulable()));
    assertTrue(targetQueue.getRunnableAppSchedulables()
        .contains(app.getAppSchedulable()));
    assertEquals(1, targetQueue.getNumRunnableApps());
    assertEquals(1, queueMgr.getRootQueue().getNumRunnableApps());
  }
    
  @Test (expected = YarnException.class)
  public void testMoveWouldViolateMaxAppsConstraints() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    QueueManager queueMgr = scheduler.getQueueManager();
    queueMgr.getLeafQueue("queue2", true);
    scheduler.getAllocationConfiguration().queueMaxApps.put("root.queue2", 0);
    
    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
  }
  
  @Test (expected = YarnException.class)
  public void testMoveWouldViolateMaxResourcesConstraints() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    queueMgr.getLeafQueue("queue2", true);
    scheduler.getAllocationConfiguration().maxQueueResources.put("root.queue2",
        Resource.newInstance(1024, 1));

    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(2048, 2));
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    
    assertEquals(Resource.newInstance(2048, 2), oldQueue.getResourceUsage());
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
  }
  
  @Test (expected = YarnException.class)
  public void testMoveToNonexistentQueue() throws Exception {
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.getQueueManager().getLeafQueue("queue1", true);
    
    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
  }
}
