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

import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueuePlacementRule.Default;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.google.common.collect.Sets;

@SuppressWarnings("unchecked")
public class TestFairScheduler extends FairSchedulerTestBase {
  private final static String ALLOC_FILE =
      new File(TEST_DIR, "test-queues").getAbsolutePath();

  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    conf = createConfiguration();
    resourceManager = new ResourceManager();
    resourceManager.init(conf);

    // TODO: This test should really be using MockRM. For now starting stuff
    // that is needed at a bare minimum.
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();

    // to initialize the master key
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();

    scheduler.setRMContext(resourceManager.getRMContext());
  }

  @After
  public void tearDown() {
    if (scheduler != null) {
      scheduler.stop();
      scheduler = null;
    }
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }


  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
    scheduler = new FairScheduler();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2048);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    try {
      scheduler.serviceInit(conf);
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
      scheduler.serviceInit(conf);
      fail("Exception is expected because the min vcores allocation is" +
        " larger than the max vcores allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue("The thrown exception is not the expected one.",
        e.getMessage().startsWith(
          "Invalid resource scheduler vcores"));
    }
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
    scheduler.init(conf);
    scheduler.start();
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
    scheduler = new FairScheduler();
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 1);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    scheduler.init(conf);
    scheduler.reinitialize(conf, null);
    Assert.assertEquals(256, scheduler.getMinimumResourceCapability().getMemory());
    Assert.assertEquals(1, scheduler.getMinimumResourceCapability().getVirtualCores());
    Assert.assertEquals(512, scheduler.getIncrementResourceCapability().getMemory());
    Assert.assertEquals(2, scheduler.getIncrementResourceCapability().getVirtualCores());
  }  
  
  @Test  
  public void testMinZeroResourcesSettings() throws IOException {  
    scheduler = new FairScheduler();
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 0);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    scheduler.init(conf);
    scheduler.reinitialize(conf, null);
    Assert.assertEquals(0, scheduler.getMinimumResourceCapability().getMemory());
    Assert.assertEquals(0, scheduler.getMinimumResourceCapability().getVirtualCores());
    Assert.assertEquals(512, scheduler.getIncrementResourceCapability().getMemory());
    Assert.assertEquals(2, scheduler.getIncrementResourceCapability().getVirtualCores());
  }  
  
  @Test
  public void testAggregateCapacityTracking() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(1024, scheduler.getClusterResource().getMemory());

    // Add another node
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(512), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    assertEquals(1536, scheduler.getClusterResource().getMemory());

    // Remove the first node
    NodeRemovedSchedulerEvent nodeEvent3 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent3);
    assertEquals(512, scheduler.getClusterResource().getMemory());
  }

  @Test
  public void testSimpleFairShareCalculation() throws IOException {
    scheduler.init(conf);
    scheduler.start();
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
    createSchedulingRequest(10 * 1024, "root.default", "user1");

    scheduler.update();
    scheduler.getQueueManager().getRootQueue()
        .setSteadyFairShare(scheduler.getClusterResource());
    scheduler.getQueueManager().getRootQueue().recomputeSteadyShares();

    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());
    
    // Divided three ways - between the two queues and the default queue
    for (FSLeafQueue p : queues) {
      assertEquals(3414, p.getFairShare().getMemory());
      assertEquals(3414, p.getMetrics().getFairShareMB());
      assertEquals(3414, p.getSteadyFairShare().getMemory());
      assertEquals(3414, p.getMetrics().getSteadyFairShareMB());
    }
  }

  @Test
  public void testFairShareWithMaxResources() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB maxResources,
    // the sum of queueA and queueB maxResources is more than
    // Integer.MAX_VALUE.
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<maxResources>1073741824 mb 1000 vcores</maxResources>");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<maxResources>1073741824 mb 1000 vcores</maxResources>");
    out.println("<weight>.75</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 1 * 1024.
    createSchedulingRequest(1 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.25, so its fair share should be 2 * 1024.
    assertEquals(2 * 1024, queue.getFairShare().getMemory());
    // queueB's weight is 0.75, so its fair share should be 6 * 1024.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(6 * 1024, queue.getFairShare().getMemory());
  }

  @Test
  public void testFairShareWithZeroWeight() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB weight zero.
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>0.0</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>0.0</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 2 * 1024.
    createSchedulingRequest(2 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.0, so its fair share should be 0.
    assertEquals(0, queue.getFairShare().getMemory());
    // queueB's weight is 0.0, so its fair share should be 0.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(0, queue.getFairShare().getMemory());
  }

  @Test
  public void testFairShareWithZeroWeightNoneZeroMinRes() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB weight zero.
    // set queueA and queueB minResources 1.
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>1 mb 1 vcores</minResources>");
    out.println("<weight>0.0</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>1 mb 1 vcores</minResources>");
    out.println("<weight>0.0</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 2 * 1024.
    createSchedulingRequest(2 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.0 and minResources is 1,
    // so its fair share should be 1 (minShare).
    assertEquals(1, queue.getFairShare().getMemory());
    // queueB's weight is 0.0 and minResources is 1,
    // so its fair share should be 1 (minShare).
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(1, queue.getFairShare().getMemory());
  }

  @Test
  public void testFairShareWithNoneZeroWeightNoneZeroMinRes()
      throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB weight 0.5.
    // set queueA and queueB minResources 1024.
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>1024 mb 1 vcores</minResources>");
    out.println("<weight>0.5</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>1024 mb 1 vcores</minResources>");
    out.println("<weight>0.5</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 4 * 1024.
    createSchedulingRequest(4 * 1024, "queueA", "user1");
    // Queue B wants 4 * 1024
    createSchedulingRequest(4 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.5 and minResources is 1024,
    // so its fair share should be 4096.
    assertEquals(4096, queue.getFairShare().getMemory());
    // queueB's weight is 0.5 and minResources is 1024,
    // so its fair share should be 4096.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(4096, queue.getFairShare().getMemory());
  }

  @Test
  public void testQueueInfo() throws IOException {
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

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 1 * 1024.
    createSchedulingRequest(1 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    // Capacity should be the same as weight of Queue,
    // because the sum of all active Queues' weight are 1.
    // Before NodeUpdate Event, CurrentCapacity should be 0
    QueueInfo queueInfo = scheduler.getQueueInfo("queueA", false, false);
    Assert.assertEquals(0.25f, queueInfo.getCapacity(), 0.0f);
    Assert.assertEquals(0.0f, queueInfo.getCurrentCapacity(), 0.0f);
    queueInfo = scheduler.getQueueInfo("queueB", false, false);
    Assert.assertEquals(0.75f, queueInfo.getCapacity(), 0.0f);
    Assert.assertEquals(0.0f, queueInfo.getCurrentCapacity(), 0.0f);

    // Each NodeUpdate Event will only assign one container.
    // To assign two containers, call handle NodeUpdate Event twice.
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);
    scheduler.handle(nodeEvent2);

    // After NodeUpdate Event, CurrentCapacity for queueA should be 1/2=0.5
    // and CurrentCapacity for queueB should be 6/6=1.
    queueInfo = scheduler.getQueueInfo("queueA", false, false);
    Assert.assertEquals(0.25f, queueInfo.getCapacity(), 0.0f);
    Assert.assertEquals(0.5f, queueInfo.getCurrentCapacity(), 0.0f);
    queueInfo = scheduler.getQueueInfo("queueB", false, false);
    Assert.assertEquals(0.75f, queueInfo.getCapacity(), 0.0f);
    Assert.assertEquals(1.0f, queueInfo.getCurrentCapacity(), 0.0f);
  }

  @Test
  public void testSimpleHierarchicalFairShareCalculation() throws IOException {
    scheduler.init(conf);
    scheduler.start();
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
    createSchedulingRequest(10 * 1024, "root.default", "user1");

    scheduler.update();
    scheduler.getQueueManager().getRootQueue()
        .setSteadyFairShare(scheduler.getClusterResource());
    scheduler.getQueueManager().getRootQueue().recomputeSteadyShares();

    QueueManager queueManager = scheduler.getQueueManager();
    Collection<FSLeafQueue> queues = queueManager.getLeafQueues();
    assertEquals(3, queues.size());
    
    FSLeafQueue queue1 = queueManager.getLeafQueue("default", true);
    FSLeafQueue queue2 = queueManager.getLeafQueue("parent.queue2", true);
    FSLeafQueue queue3 = queueManager.getLeafQueue("parent.queue3", true);
    assertEquals(capacity / 2, queue1.getFairShare().getMemory());
    assertEquals(capacity / 2, queue1.getMetrics().getFairShareMB());
    assertEquals(capacity / 2, queue1.getSteadyFairShare().getMemory());
    assertEquals(capacity / 2, queue1.getMetrics().getSteadyFairShareMB());
    assertEquals(capacity / 4, queue2.getFairShare().getMemory());
    assertEquals(capacity / 4, queue2.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue2.getSteadyFairShare().getMemory());
    assertEquals(capacity / 4, queue2.getMetrics().getSteadyFairShareMB());
    assertEquals(capacity / 4, queue3.getFairShare().getMemory());
    assertEquals(capacity / 4, queue3.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue3.getSteadyFairShare().getMemory());
    assertEquals(capacity / 4, queue3.getMetrics().getSteadyFairShareMB());
  }

  @Test
  public void testHierarchicalQueuesSimilarParents() throws IOException {
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
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

  @Test (timeout = 500000)
  public void testContainerReservationAttemptExceedingQueueMax()
      throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<queue name=\"queue1\">");
    out.println("<maxResources>2048mb,5vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queue2\">");
    out.println("<maxResources>2048mb,10vcores</maxResources>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(3072, 5), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of the queue
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(2048, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemory());

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemory());

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Ensure the reservation does not get created as allocated memory of
    // queue1 exceeds max
    assertEquals(0, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemory());
  }

    @Test (timeout = 500000)
  public void testContainerReservationNotExceedingQueueMax() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<queue name=\"queue1\">");
    out.println("<maxResources>3072mb,10vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queue2\">");
    out.println("<maxResources>2048mb,10vcores</maxResources>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(3072, 5), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of the queue
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(2048, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemory());

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
      getResourceUsage().getMemory());
    
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 1 is waiting with a reservation
    assertEquals(1024, scheduler.getSchedulerApp(attId1)
        .getCurrentReservation().getMemory());

    // Exercise checks that reservation fits
    scheduler.handle(updateEvent);

    // Ensure the reservation still exists as allocated memory of queue1 doesn't
    // exceed max
    assertEquals(1024, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemory());

    // Now reduce max Resources of queue1 down to 2048
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<queue name=\"queue1\">");
    out.println("<maxResources>2048mb,10vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queue2\">");
    out.println("<maxResources>2048mb,10vcores</maxResources>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.handle(updateEvent);

    // Make sure allocated memory of queue1 doesn't exceed its maximum
    assertEquals(2048, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemory());
    //the reservation of queue1 should be reclaim
    assertEquals(0, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemory());
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemory());
  }

  @Test
  public void testUserAsDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user1", null);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(appAttemptId.getApplicationId()).getQueue());
  }
  
  @Test
  public void testNotUserAsDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user2", null);
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user2", true)
        .getNumRunnableApps());
  }

  @Test
  public void testEmptyQueueName() throws Exception {
    scheduler.init(conf);
    scheduler.start();
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
  public void testQueueuNameWithPeriods() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // only default queue
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());

    // submit app with queue name (.A)
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent1 =
        new AppAddedSchedulerEvent(appAttemptId1.getApplicationId(), ".A", "user1");
    scheduler.handle(appAddedEvent1);
    // submission rejected
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId1));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());

    // submit app with queue name (A.)
    ApplicationAttemptId appAttemptId2 = createAppAttemptId(2, 1);
    AppAddedSchedulerEvent appAddedEvent2 =
        new AppAddedSchedulerEvent(appAttemptId2.getApplicationId(), "A.", "user1");
    scheduler.handle(appAddedEvent2);
    // submission rejected
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId2));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());

    // submit app with queue name (A.B)
    ApplicationAttemptId appAttemptId3 = createAppAttemptId(3, 1);
    AppAddedSchedulerEvent appAddedEvent3 =
        new AppAddedSchedulerEvent(appAttemptId3.getApplicationId(), "A.B", "user1");
    scheduler.handle(appAddedEvent3);
    // submission accepted
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId3));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());
  }

  @Test
  public void testAssignToQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
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
    Map<FSQueueType, Set<String>> configuredQueues = new HashMap<FSQueueType, Set<String>>();
    configuredQueues.put(FSQueueType.LEAF, queues);
    configuredQueues.put(FSQueueType.PARENT, new HashSet<String>());
    scheduler.getAllocationConfiguration().placementPolicy =
        new QueuePlacementPolicy(rules, configuredQueues, conf);
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
        new QueuePlacementPolicy(rules, configuredQueues, conf);
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

    scheduler.init(conf);
    scheduler.start();
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
  
  @Test
  public void testNestedUserQueue() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"user1group\" type=\"parent\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"nestedUserQueue\">");
    out.println("     <rule name=\"primaryGroup\" create=\"false\" />");
    out.println("</rule>");
    out.println("<rule name=\"default\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    FSLeafQueue user1Leaf = scheduler.assignToQueue(rmApp1, "root.default",
        "user1");

    assertEquals("root.user1group.user1", user1Leaf.getName());
  }

  @Test
  public void testFairShareAndWeightsInNestedUserQueueRule() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));

    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"parentq\" type=\"parent\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"nestedUserQueue\">");
    out.println("     <rule name=\"specified\" create=\"false\" />");
    out.println("</rule>");
    out.println("<rule name=\"default\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    int capacity = 16 * 1024;
    // create node with 16 G
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(capacity),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // user1,user2 submit their apps to parentq and create user queues
    createSchedulingRequest(10 * 1024, "root.parentq", "user1");
    createSchedulingRequest(10 * 1024, "root.parentq", "user2");
    // user3 submits app in default queue
    createSchedulingRequest(10 * 1024, "root.default", "user3");

    scheduler.update();
    scheduler.getQueueManager().getRootQueue()
        .setSteadyFairShare(scheduler.getClusterResource());
    scheduler.getQueueManager().getRootQueue().recomputeSteadyShares();

    Collection<FSLeafQueue> leafQueues = scheduler.getQueueManager()
        .getLeafQueues();

    for (FSLeafQueue leaf : leafQueues) {
      if (leaf.getName().equals("root.parentq.user1")
          || leaf.getName().equals("root.parentq.user2")) {
        // assert that the fair share is 1/4th node1's capacity
        assertEquals(capacity / 4, leaf.getFairShare().getMemory());
        // assert that the steady fair share is 1/4th node1's capacity
        assertEquals(capacity / 4, leaf.getSteadyFairShare().getMemory());
        // assert weights are equal for both the user queues
        assertEquals(1.0, leaf.getWeights().getWeight(ResourceType.MEMORY), 0);
      }
    }
  }

  @Test
  public void testSteadyFairShareWithReloadAndNodeAddRemove() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>");
    out.println("<queue name=\"root\">");
    out.println("  <schedulingPolicy>drf</schedulingPolicy>");
    out.println("  <queue name=\"child1\">");
    out.println("    <weight>1</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"child2\">");
    out.println("    <weight>1</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // The steady fair share for all queues should be 0
    QueueManager queueManager = scheduler.getQueueManager();
    assertEquals(0, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemory());
    assertEquals(0, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemory());

    // Add one node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(6144), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(6144, scheduler.getClusterResource().getMemory());

    // The steady fair shares for all queues should be updated
    assertEquals(2048, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemory());
    assertEquals(2048, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemory());

    // Reload the allocation configuration file
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>");
    out.println("<queue name=\"root\">");
    out.println("  <schedulingPolicy>drf</schedulingPolicy>");
    out.println("  <queue name=\"child1\">");
    out.println("    <weight>1</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"child2\">");
    out.println("    <weight>2</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"child3\">");
    out.println("    <weight>2</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // The steady fair shares for all queues should be updated
    assertEquals(1024, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemory());
    assertEquals(2048, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemory());
    assertEquals(2048, queueManager.getLeafQueue("child3", false)
        .getSteadyFairShare().getMemory());

    // Remove the node, steady fair shares should back to 0
    NodeRemovedSchedulerEvent nodeEvent2 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);
    assertEquals(0, scheduler.getClusterResource().getMemory());
    assertEquals(0, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemory());
    assertEquals(0, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemory());
  }

  @Test
  public void testSteadyFairShareWithQueueCreatedRuntime() throws Exception {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(6144), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(6144, scheduler.getClusterResource().getMemory());
    assertEquals(6144, scheduler.getQueueManager().getRootQueue()
        .getSteadyFairShare().getMemory());
    assertEquals(6144, scheduler.getQueueManager()
        .getLeafQueue("default", false).getSteadyFairShare().getMemory());

    // Submit one application
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId1, "default", "user1", null);
    assertEquals(3072, scheduler.getQueueManager()
        .getLeafQueue("default", false).getSteadyFairShare().getMemory());
    assertEquals(3072, scheduler.getQueueManager()
        .getLeafQueue("user1", false).getSteadyFairShare().getMemory());
  }

  /**
   * Make allocation requests and ensure they are reflected in queue demand.
   */
  @Test
  public void testQueueDemandCalculation() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);
    scheduler.addApplication(id11.getApplicationId(), "root.queue1", "user1", false);
    scheduler.addApplicationAttempt(id11, false, false);
    ApplicationAttemptId id21 = createAppAttemptId(2, 1);
    createMockRMApp(id21);
    scheduler.addApplication(id21.getApplicationId(), "root.queue2", "user1", false);
    scheduler.addApplicationAttempt(id21, false, false);
    ApplicationAttemptId id22 = createAppAttemptId(2, 2);
    createMockRMApp(id22);

    scheduler.addApplication(id22.getApplicationId(), "root.queue2", "user1", false);
    scheduler.addApplicationAttempt(id22, false, false);

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
    scheduler.init(conf);
    scheduler.start();
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
        .getNumRunnableApps());

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        createAppAttemptId(1, 1), RMAppAttemptState.FINISHED, false);

    // Now remove app
    scheduler.handle(appRemovedEvent1);

    // Queue should have no apps
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
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

    scheduler.init(conf);
    scheduler.start();
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
    out.println("  <fairSharePreemptionTimeout>100</fairSharePreemptionTimeout>");
    out.println("  <minSharePreemptionTimeout>120</minSharePreemptionTimeout>");
    out.println("  <fairSharePreemptionThreshold>.5</fairSharePreemptionThreshold>");
    out.println("</queue>");
    out.println("<defaultFairSharePreemptionTimeout>300</defaultFairSharePreemptionTimeout>");
    out.println("<defaultMinSharePreemptionTimeout>200</defaultMinSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.6</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    QueueManager queueManager = scheduler.getQueueManager();
    
    FSQueue root = queueManager.getRootQueue();
    assertTrue(root.getPolicy() instanceof DominantResourceFairnessPolicy);
    
    assertNotNull(queueManager.getLeafQueue("child1", false));
    assertNotNull(queueManager.getLeafQueue("child2", false));

    assertEquals(100000, root.getFairSharePreemptionTimeout());
    assertEquals(120000, root.getMinSharePreemptionTimeout());
    assertEquals(0.5f, root.getFairSharePreemptionThreshold(), 0.01);
  }

  @Test (timeout = 5000)
  /**
   * Make sure containers are chosen to be preempted in the correct order.
   */
  public void testChoiceOfPreemptedContainers() throws Exception {
    conf.setLong(FairSchedulerConfiguration.PREEMPTION_INTERVAL, 5000);
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10000);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE + ".allocation.file", ALLOC_FILE);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");
    
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
    out.println("<queue name=\"default\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create two nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 2,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    // Queue A and B each request two applications
    ApplicationAttemptId app1 =
        createSchedulingRequest(1 * 1024, 1, "queueA", "user1", 1, 1);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 2, app1);
    ApplicationAttemptId app2 =
        createSchedulingRequest(1 * 1024, 1, "queueA", "user1", 1, 3);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 4, app2);

    ApplicationAttemptId app3 =
        createSchedulingRequest(1 * 1024, 1, "queueB", "user1", 1, 1);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 2, app3);
    ApplicationAttemptId app4 =
        createSchedulingRequest(1 * 1024, 1, "queueB", "user1", 1, 3);
    createSchedulingRequestExistingApplication(1 * 1024, 1, 4, app4);

    scheduler.update();

    scheduler.getQueueManager().getLeafQueue("queueA", true)
        .setPolicy(SchedulingPolicy.parse("fifo"));
    scheduler.getQueueManager().getLeafQueue("queueB", true)
        .setPolicy(SchedulingPolicy.parse("fair"));

    // Sufficient node check-ins to fully schedule containers
    NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
    for (int i = 0; i < 4; i++) {
      scheduler.handle(nodeUpdate1);
      scheduler.handle(nodeUpdate2);
    }

    assertEquals(2, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app4).getLiveContainers().size());

    // Now new requests arrive from queueC and default
    createSchedulingRequest(1 * 1024, 1, "queueC", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, 1, "queueC", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, 1, "default", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, 1, "default", "user1", 1, 1);
    scheduler.update();

    // We should be able to claw back one container from queueA and queueB each.
    scheduler.preemptResources(Resources.createResource(2 * 1024));
    assertEquals(2, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(2, scheduler.getSchedulerApp(app3).getLiveContainers().size());

    // First verify we are adding containers to preemption list for the app.
    // For queueA (fifo), app2 is selected.
    // For queueB (fair), app4 is selected.
    assertTrue("App2 should have container to be preempted",
        !Collections.disjoint(
            scheduler.getSchedulerApp(app2).getLiveContainers(),
            scheduler.getSchedulerApp(app2).getPreemptionContainers()));
    assertTrue("App4 should have container to be preempted",
        !Collections.disjoint(
            scheduler.getSchedulerApp(app2).getLiveContainers(),
            scheduler.getSchedulerApp(app2).getPreemptionContainers()));

    // Pretend 15 seconds have passed
    clock.tick(15);

    // Trigger a kill by insisting we want containers back
    scheduler.preemptResources(Resources.createResource(2 * 1024));

    // At this point the containers should have been killed (since we are not simulating AM)
    assertEquals(1, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());
    // Inside each app, containers are sorted according to their priorities.
    // Containers with priority 4 are preempted for app2 and app4.
    Set<RMContainer> set = new HashSet<RMContainer>();
    for (RMContainer container :
        scheduler.getSchedulerApp(app2).getLiveContainers()) {
      if (container.getAllocatedPriority().getPriority() == 4) {
        set.add(container);
      }
    }
    for (RMContainer container :
        scheduler.getSchedulerApp(app4).getLiveContainers()) {
      if (container.getAllocatedPriority().getPriority() == 4) {
        set.add(container);
      }
    }
    assertTrue("Containers with priority=4 in app2 and app4 should be " +
        "preempted.", set.isEmpty());

    // Trigger a kill by insisting we want containers back
    scheduler.preemptResources(Resources.createResource(2 * 1024));

    // Pretend 15 seconds have passed
    clock.tick(15);

    // We should be able to claw back another container from A and B each.
    // For queueA (fifo), continue preempting from app2.
    // For queueB (fair), even app4 has a lowest priority container with p=4, it
    // still preempts from app3 as app3 is most over fair share.
    scheduler.preemptResources(Resources.createResource(2 * 1024));

    assertEquals(2, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(0, scheduler.getSchedulerApp(app2).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app3).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app4).getLiveContainers().size());

    // Now A and B are below fair share, so preemption shouldn't do anything
    scheduler.preemptResources(Resources.createResource(2 * 1024));
    assertTrue("App1 should have no container to be preempted",
        scheduler.getSchedulerApp(app1).getPreemptionContainers().isEmpty());
    assertTrue("App2 should have no container to be preempted",
        scheduler.getSchedulerApp(app2).getPreemptionContainers().isEmpty());
    assertTrue("App3 should have no container to be preempted",
        scheduler.getSchedulerApp(app3).getPreemptionContainers().isEmpty());
    assertTrue("App4 should have no container to be preempted",
        scheduler.getSchedulerApp(app4).getPreemptionContainers().isEmpty());
  }

  @Test
  public void testPreemptionIsNotDelayedToNextRound() throws Exception {
    conf.setLong(FairSchedulerConfiguration.PREEMPTION_INTERVAL, 5000);
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10000);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");

    MockClock clock = new MockClock();
    scheduler.setClock(clock);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>8</weight>");
    out.println("<queue name=\"queueA1\" />");
    out.println("<queue name=\"queueA2\" />");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>2</weight>");
    out.println("</queue>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node of 8G
    RMNode node1 = MockNodes.newNodeInfo(1,
        Resources.createResource(8 * 1024, 8), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Run apps in queueA.A1 and queueB
    ApplicationAttemptId app1 = createSchedulingRequest(1 * 1024, 1,
        "queueA.queueA1", "user1", 7, 1);
    // createSchedulingRequestExistingApplication(1 * 1024, 1, 2, app1);
    ApplicationAttemptId app2 = createSchedulingRequest(1 * 1024, 1, "queueB",
        "user2", 1, 1);

    scheduler.update();

    NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
    for (int i = 0; i < 8; i++) {
      scheduler.handle(nodeUpdate1);
    }

    // verify if the apps got the containers they requested
    assertEquals(7, scheduler.getSchedulerApp(app1).getLiveContainers().size());
    assertEquals(1, scheduler.getSchedulerApp(app2).getLiveContainers().size());

    // Now submit an app in queueA.queueA2
    ApplicationAttemptId app3 = createSchedulingRequest(1 * 1024, 1,
        "queueA.queueA2", "user3", 7, 1);
    scheduler.update();

    // Let 11 sec pass
    clock.tick(11);

    scheduler.update();
    Resource toPreempt = scheduler.resToPreempt(scheduler.getQueueManager()
        .getLeafQueue("queueA.queueA2", false), clock.getTime());
    assertEquals(3277, toPreempt.getMemory());

    // verify if the 3 containers required by queueA2 are preempted in the same
    // round
    scheduler.preemptResources(toPreempt);
    assertEquals(3, scheduler.getSchedulerApp(app1).getPreemptionContainers()
        .size());
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
    out.println("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>");
    out.println("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
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

  @Test
  /**
   * Tests the various timing of decision to preempt tasks.
   */
  public void testPreemptionDecisionWithVariousTimeout() throws Exception {
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
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>2</weight>");
    out.println("<minSharePreemptionTimeout>10</minSharePreemptionTimeout>");
    out.println("<fairSharePreemptionTimeout>25</fairSharePreemptionTimeout>");
    out.println("<queue name=\"queueB1\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("<queue name=\"queueB2\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<fairSharePreemptionTimeout>20</fairSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>1</weight>");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>");
    out.print("<defaultFairSharePreemptionTimeout>30</defaultFairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Check the min/fair share preemption timeout for each queue
    QueueManager queueMgr = scheduler.getQueueManager();
    assertEquals(30000, queueMgr.getQueue("root")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("default")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueA")
        .getFairSharePreemptionTimeout());
    assertEquals(25000, queueMgr.getQueue("queueB")
        .getFairSharePreemptionTimeout());
    assertEquals(25000, queueMgr.getQueue("queueB.queueB1")
        .getFairSharePreemptionTimeout());
    assertEquals(20000, queueMgr.getQueue("queueB.queueB2")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueC")
        .getFairSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("root")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("default")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueA")
        .getMinSharePreemptionTimeout());
    assertEquals(10000, queueMgr.getQueue("queueB")
        .getMinSharePreemptionTimeout());
    assertEquals(5000, queueMgr.getQueue("queueB.queueB1")
        .getMinSharePreemptionTimeout());
    assertEquals(10000, queueMgr.getQueue("queueB.queueB2")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueC")
        .getMinSharePreemptionTimeout());

    // Create one big node
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(6 * 1024, 6), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A takes all resources
    for (int i = 0; i < 6; i ++) {
      createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    }

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
    for (int i = 0; i < 6; i++) {
      scheduler.handle(nodeUpdate1);
    }

    // Now new requests arrive from queues B1, B2 and C
    createSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 2);
    createSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 3);
    createSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 2);
    createSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 3);
    createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    scheduler.update();

    FSLeafQueue queueB1 = queueMgr.getLeafQueue("queueB.queueB1", true);
    FSLeafQueue queueB2 = queueMgr.getLeafQueue("queueB.queueB2", true);
    FSLeafQueue queueC = queueMgr.getLeafQueue("queueC", true);

    assertTrue(Resources.equals(
        Resources.none(), scheduler.resToPreempt(queueB1, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.none(), scheduler.resToPreempt(queueB2, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.none(), scheduler.resToPreempt(queueC, clock.getTime())));

    // After 5 seconds, queueB1 wants to preempt min share
    scheduler.update();
    clock.tick(6);
    assertEquals(
       1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(
        0, scheduler.resToPreempt(queueB2, clock.getTime()).getMemory());
    assertEquals(
        0, scheduler.resToPreempt(queueC, clock.getTime()).getMemory());

    // After 10 seconds, queueB2 wants to preempt min share
    scheduler.update();
    clock.tick(5);
    assertEquals(
        1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resToPreempt(queueB2, clock.getTime()).getMemory());
    assertEquals(
        0, scheduler.resToPreempt(queueC, clock.getTime()).getMemory());

    // After 15 seconds, queueC wants to preempt min share
    scheduler.update();
    clock.tick(5);
    assertEquals(
        1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resToPreempt(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resToPreempt(queueC, clock.getTime()).getMemory());

    // After 20 seconds, queueB2 should want to preempt fair share
    scheduler.update();
    clock.tick(5);
    assertEquals(
        1024, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resToPreempt(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resToPreempt(queueC, clock.getTime()).getMemory());

    // After 25 seconds, queueB1 should want to preempt fair share
    scheduler.update();
    clock.tick(5);
    assertEquals(
        1536, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resToPreempt(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1024, scheduler.resToPreempt(queueC, clock.getTime()).getMemory());

    // After 30 seconds, queueC should want to preempt fair share
    scheduler.update();
    clock.tick(5);
    assertEquals(
        1536, scheduler.resToPreempt(queueB1, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resToPreempt(queueB2, clock.getTime()).getMemory());
    assertEquals(
        1536, scheduler.resToPreempt(queueC, clock.getTime()).getMemory());
  }

  @Test
  public void testBackwardsCompatiblePreemptionConfiguration() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<queue name=\"queueB1\">");
    out.println("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("<queue name=\"queueB2\">");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>");
    out.print("<defaultFairSharePreemptionTimeout>30</defaultFairSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>40</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Check the min/fair share preemption timeout for each queue
    QueueManager queueMgr = scheduler.getQueueManager();
    assertEquals(30000, queueMgr.getQueue("root")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("default")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueA")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB.queueB1")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB.queueB2")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueC")
        .getFairSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("root")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("default")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueA")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueB")
        .getMinSharePreemptionTimeout());
    assertEquals(5000, queueMgr.getQueue("queueB.queueB1")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueB.queueB2")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueC")
        .getMinSharePreemptionTimeout());

    // If both exist, we take the default one
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("</queue>");
    out.println("<queue name=\"queueA\">");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<queue name=\"queueB1\">");
    out.println("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
    out.println("</queue>");
    out.println("<queue name=\"queueB2\">");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>");
    out.print("<defaultFairSharePreemptionTimeout>25</defaultFairSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>30</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    assertEquals(25000, queueMgr.getQueue("root")
        .getFairSharePreemptionTimeout());
  }

  @Test
  public void testPreemptionVariablesForQueueCreatedRuntime() throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Set preemption variables for the root queue
    FSParentQueue root = scheduler.getQueueManager().getRootQueue();
    root.setMinSharePreemptionTimeout(10000);
    root.setFairSharePreemptionTimeout(15000);
    root.setFairSharePreemptionThreshold(.6f);

    // User1 submits one application
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user1", null);

    // The user1 queue should inherit the configurations from the root queue
    FSLeafQueue userQueue =
        scheduler.getQueueManager().getLeafQueue("user1", true);
    assertEquals(1, userQueue.getNumRunnableApps());
    assertEquals(10000, userQueue.getMinSharePreemptionTimeout());
    assertEquals(15000, userQueue.getFairSharePreemptionTimeout());
    assertEquals(.6f, userQueue.getFairSharePreemptionThreshold(), 0.001);
  }

  @Test (timeout = 5000)
  public void testMultipleContainersWaitingForReservation() throws IOException {
    scheduler.init(conf);
    scheduler.start();
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

    scheduler.init(conf);
    scheduler.start();
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
  public void testIncreaseQueueMaxRunningAppsOnTheFly() throws Exception {
  String allocBefore = "<?xml version=\"1.0\"?>" +
        "<allocations>" +
        "<queue name=\"root\">" +
        "<queue name=\"queue1\">" +
        "<maxRunningApps>1</maxRunningApps>" +
        "</queue>" +
        "</queue>" +
        "</allocations>";

    String allocAfter = "<?xml version=\"1.0\"?>" +
        "<allocations>" +
        "<queue name=\"root\">" +
        "<queue name=\"queue1\">" +
        "<maxRunningApps>3</maxRunningApps>" +
        "</queue>" +
        "</queue>" +
        "</allocations>";

    testIncreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  @Test (timeout = 5000)
  public void testIncreaseUserMaxRunningAppsOnTheFly() throws Exception {
    String allocBefore = "<?xml version=\"1.0\"?>"+
        "<allocations>"+
        "<queue name=\"root\">"+
        "<queue name=\"queue1\">"+
        "<maxRunningApps>10</maxRunningApps>"+
        "</queue>"+
        "</queue>"+
        "<user name=\"user1\">"+
        "<maxRunningApps>1</maxRunningApps>"+
        "</user>"+
        "</allocations>";

    String allocAfter = "<?xml version=\"1.0\"?>"+
        "<allocations>"+
        "<queue name=\"root\">"+
        "<queue name=\"queue1\">"+
        "<maxRunningApps>10</maxRunningApps>"+
        "</queue>"+
        "</queue>"+
        "<user name=\"user1\">"+
        "<maxRunningApps>3</maxRunningApps>"+
        "</user>"+
        "</allocations>";

    testIncreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  private void testIncreaseQueueSettingOnTheFlyInternal(String allocBefore,
      String allocAfter) throws Exception {
    // Set max running apps
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println(allocBefore);
    out.close();

    scheduler.init(conf);
    scheduler.start();
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

    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
    // App 3 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId3).getLiveContainers().size());
    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println(allocAfter);
    out.close();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId2).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 3 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId3).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 1
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId1, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId4).getLiveContainers().size());
  }

  @Test (timeout = 5000)
  public void testDecreaseQueueMaxRunningAppsOnTheFly() throws Exception {
  String allocBefore = "<?xml version=\"1.0\"?>" +
        "<allocations>" +
        "<queue name=\"root\">" +
        "<queue name=\"queue1\">" +
        "<maxRunningApps>3</maxRunningApps>" +
        "</queue>" +
        "</queue>" +
        "</allocations>";

    String allocAfter = "<?xml version=\"1.0\"?>" +
        "<allocations>" +
        "<queue name=\"root\">" +
        "<queue name=\"queue1\">" +
        "<maxRunningApps>1</maxRunningApps>" +
        "</queue>" +
        "</queue>" +
        "</allocations>";

    testDecreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  @Test (timeout = 5000)
  public void testDecreaseUserMaxRunningAppsOnTheFly() throws Exception {
    String allocBefore = "<?xml version=\"1.0\"?>"+
        "<allocations>"+
        "<queue name=\"root\">"+
        "<queue name=\"queue1\">"+
        "<maxRunningApps>10</maxRunningApps>"+
        "</queue>"+
        "</queue>"+
        "<user name=\"user1\">"+
        "<maxRunningApps>3</maxRunningApps>"+
        "</user>"+
        "</allocations>";

    String allocAfter = "<?xml version=\"1.0\"?>"+
        "<allocations>"+
        "<queue name=\"root\">"+
        "<queue name=\"queue1\">"+
        "<maxRunningApps>10</maxRunningApps>"+
        "</queue>"+
        "</queue>"+
        "<user name=\"user1\">"+
        "<maxRunningApps>1</maxRunningApps>"+
        "</user>"+
        "</allocations>";

    testDecreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  private void testDecreaseQueueSettingOnTheFlyInternal(String allocBefore,
      String allocAfter) throws Exception {
    // Set max running apps
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println(allocBefore);
    out.close();

    scheduler.init(conf);
    scheduler.start();
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

    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
    // App 3 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId3).getLiveContainers().size());
    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println(allocAfter);
    out.close();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should still be running
    assertEquals(1, scheduler.getSchedulerApp(attId2).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 3 should still be running
    assertEquals(1, scheduler.getSchedulerApp(attId3).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 1
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId1, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 2
    appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId2, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 3
    appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId3, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should be running now
    assertEquals(1, scheduler.getSchedulerApp(attId4).getLiveContainers().size());
  }

  @Test (timeout = 5000)
  public void testReservationWhileMultiplePriorities() throws IOException {
    scheduler.init(conf);
    scheduler.start();
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
    
    FSAppAttempt app = scheduler.getSchedulerApp(attId);
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
      Assert.assertEquals(2, liveContainer.getContainer().getPriority()
          .getPriority());
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

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname", 1);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname2", 1);

    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    assertNotNull("The application was not allowed", app1);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    assertNull("The application was allowed", app2);
  }
  
  @Test (timeout = 5000)
  public void testMultipleNodesSingleRackRequest() throws Exception {
    scheduler.init(conf);
    scheduler.start();
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
    
    ApplicationAttemptId attemptId =
        createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(attemptId);

    scheduler.addApplication(attemptId.getApplicationId(), "queue1", "user1",
        false);
    scheduler.addApplicationAttempt(attemptId, false, false);
    
    // 1 request with 2 nodes on the same rack. another request with 1 node on
    // a different rack
    List<ResourceRequest> asks = new ArrayList<ResourceRequest>();
    asks.add(createResourceRequest(1024, node1.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node2.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node3.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node1.getRackName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node3.getRackName(), 1, 1, true));
    asks.add(createResourceRequest(1024, ResourceRequest.ANY, 1, 2, true));

    scheduler.allocate(attemptId, asks, new ArrayList<ContainerId>(), null,
        null);
    
    // node 1 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent1);
    // should assign node local
    assertEquals(1, scheduler.getSchedulerApp(attemptId).getLiveContainers()
        .size());

    // node 2 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);
    // should assign rack local
    assertEquals(2, scheduler.getSchedulerApp(attemptId).getLiveContainers()
        .size());
  }
  
  @Test (timeout = 5000)
  public void testFifoWithinQueue() throws Exception {
    scheduler.init(conf);
    scheduler.start();
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
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    
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
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(16384, 16), 0,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId attId =
        createSchedulingRequest(1024, "root.default", "user", 8);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

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
  
  @Test(timeout = 3000)
  public void testMaxAssignWithZeroMemoryContainers() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(16384, 16), 0,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId attId =
        createSchedulingRequest(0, 1, "root.default", "user", 8);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

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
    scheduler.init(conf);
    scheduler.start();
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

    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);

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

    scheduler.init(conf);
    scheduler.start();
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
          System.currentTimeMillis(), "YARN", null, null);
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
    scheduler.addApplication(attId.getApplicationId(), queue, user, false);

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
  public void testRemoveNodeUpdatesRootQueueMetrics() throws IOException {
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
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
    FSAppAttempt app = scheduler.getSchedulerApp(attId1);
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
    scheduler.init(conf);
    scheduler.start();
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
    FSAppAttempt app = scheduler.getSchedulerApp(attId1);
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
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue1",
        "user1", 0);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);
    
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
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2048, 1),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    ApplicationAttemptId attId = createSchedulingRequest(1024, 1, "default",
        "user1", 2);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    assertEquals(1, app.getLiveContainers().size());
    scheduler.handle(updateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  @Test
  public void testBasicDRFAssignment() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, BuilderUtils.newResource(8192, 5));
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(2048, 1, "queue1",
        "user1", 2);
    FSAppAttempt app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(1024, 2, "queue1",
        "user1", 2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(appAttId2);

    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getClusterResource());
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
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, BuilderUtils.newResource(8192, 7),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(3072, 1, "queue1",
        "user1", 2);
    FSAppAttempt app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(2048, 2, "queue1",
        "user1", 2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(appAttId2);
    ApplicationAttemptId appAttId3 = createSchedulingRequest(1024, 2, "queue2",
        "user1", 2);
    FSAppAttempt app3 = scheduler.getSchedulerApp(appAttId3);
    
    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getClusterResource());
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
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, BuilderUtils.newResource(12288, 12),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(3074, 1, "queue1.subqueue1",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(1024, 3, "queue1.subqueue1",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app2 = scheduler.getSchedulerApp(appAttId2);
    ApplicationAttemptId appAttId3 = createSchedulingRequest(2048, 2, "queue1.subqueue2",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app3 = scheduler.getSchedulerApp(appAttId3);
    ApplicationAttemptId appAttId4 = createSchedulingRequest(1024, 2, "queue2",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app4 = scheduler.getSchedulerApp(appAttId4);
    
    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getClusterResource());
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
    scheduler.init(conf);
    scheduler.start();
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
    FSAppAttempt app = scheduler.getSchedulerApp(attId1);
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
    FSAppAttempt app = scheduler.getSchedulerApp(attId);
    FSLeafQueue queue = app.getQueue();
    assertEquals(runnable, queue.isRunnableApp(app));
    assertEquals(!runnable, queue.isNonRunnableApp(app));
  }
  
  private void verifyQueueNumRunnable(String queueName, int numRunnableInQueue,
      int numNonRunnableInQueue) {
    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(queueName, false);
    assertEquals(numRunnableInQueue, queue.getNumRunnableApps());
    assertEquals(numNonRunnableInQueue, queue.getNumNonRunnableApps());
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

    scheduler.init(conf);
    scheduler.start();
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
  public void testQueueMaxAMShare() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("<maxAMShare>0.2</maxAMShare>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(20480, 20),
            0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1", true);
    assertEquals("Queue queue1's fair share should be 0", 0, queue1
        .getFairShare().getMemory());

    createSchedulingRequest(1 * 1024, "root.default", "user1");
    scheduler.update();
    scheduler.handle(updateEvent);

    Resource amResource1 = Resource.newInstance(1024, 1);
    Resource amResource2 = Resource.newInstance(2048, 2);
    Resource amResource3 = Resource.newInstance(1860, 2);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();
    // Exceeds no limits
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application1's AM requests 1024 MB memory",
        1024, app1.getAMResource().getMemory());
    assertEquals("Application1's AM should be running",
        1, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemory());

    // Exceeds no limits
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application2's AM requests 1024 MB memory",
        1024, app2.getAMResource().getMemory());
    assertEquals("Application2's AM should be running",
        1, app2.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Exceeds queue limit
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application3's AM requests 1024 MB memory",
        1024, app3.getAMResource().getMemory());
    assertEquals("Application3's AM should not be running",
        0, app3.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Still can run non-AM container
    createSchedulingRequestExistingApplication(1024, 1, attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application1 should have two running containers",
        2, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Remove app1, app3's AM should become running
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1, RMAppAttemptState.FINISHED, false);
    scheduler.update();
    scheduler.handle(appRemovedEvent1);
    scheduler.handle(updateEvent);
    assertEquals("Application1's AM should be finished",
        0, app1.getLiveContainers().size());
    assertEquals("Application3's AM should be running",
        1, app3.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Exceeds queue limit
    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(2048, 2, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application4's AM requests 2048 MB memory",
        2048, app4.getAMResource().getMemory());
    assertEquals("Application4's AM should not be running",
        0, app4.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Exceeds queue limit
    ApplicationAttemptId attId5 = createAppAttemptId(5, 1);
    createApplicationWithAMResource(attId5, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(2048, 2, amPriority, attId5);
    FSAppAttempt app5 = scheduler.getSchedulerApp(attId5);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application5's AM requests 2048 MB memory",
        2048, app5.getAMResource().getMemory());
    assertEquals("Application5's AM should not be running",
        0, app5.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Remove un-running app doesn't affect others
    AppAttemptRemovedSchedulerEvent appRemovedEvent4 =
        new AppAttemptRemovedSchedulerEvent(attId4, RMAppAttemptState.KILLED, false);
    scheduler.handle(appRemovedEvent4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application5's AM should not be running",
        0, app5.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Remove app2 and app3, app5's AM should become running
    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId2, RMAppAttemptState.FINISHED, false);
    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(attId3, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent2);
    scheduler.handle(appRemovedEvent3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application2's AM should be finished",
        0, app2.getLiveContainers().size());
    assertEquals("Application3's AM should be finished",
        0, app3.getLiveContainers().size());
    assertEquals("Application5's AM should be running",
        1, app5.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Check amResource normalization
    ApplicationAttemptId attId6 = createAppAttemptId(6, 1);
    createApplicationWithAMResource(attId6, "queue1", "user1", amResource3);
    createSchedulingRequestExistingApplication(1860, 2, amPriority, attId6);
    FSAppAttempt app6 = scheduler.getSchedulerApp(attId6);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application6's AM should not be running",
        0, app6.getLiveContainers().size());
    assertEquals("Application6's AM requests 2048 MB memory",
        2048, app6.getAMResource().getMemory());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemory());

    // Remove all apps
    AppAttemptRemovedSchedulerEvent appRemovedEvent5 =
        new AppAttemptRemovedSchedulerEvent(attId5, RMAppAttemptState.FINISHED, false);
    AppAttemptRemovedSchedulerEvent appRemovedEvent6 =
        new AppAttemptRemovedSchedulerEvent(attId6, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent5);
    scheduler.handle(appRemovedEvent6);
    scheduler.update();
    assertEquals("Queue1's AM resource usage should be 0",
        0, queue1.getAmResourceUsage().getMemory());
  }

  @Test
  public void testQueueMaxAMShareDefault() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("</queue>");
    out.println("<queue name=\"queue2\">");
    out.println("<maxAMShare>0.4</maxAMShare>");
    out.println("</queue>");
    out.println("<queue name=\"queue3\">");
    out.println("</queue>");
    out.println("<queue name=\"queue4\">");
    out.println("</queue>");
    out.println("<queue name=\"queue5\">");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(8192, 20),
            0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    FSLeafQueue queue1 =
        scheduler.getQueueManager().getLeafQueue("queue1", true);
    assertEquals("Queue queue1's fair share should be 0", 0, queue1
        .getFairShare().getMemory());
    FSLeafQueue queue2 =
        scheduler.getQueueManager().getLeafQueue("queue2", true);
    assertEquals("Queue queue2's fair share should be 0", 0, queue2
        .getFairShare().getMemory());
    FSLeafQueue queue3 =
        scheduler.getQueueManager().getLeafQueue("queue3", true);
    assertEquals("Queue queue3's fair share should be 0", 0, queue3
        .getFairShare().getMemory());
    FSLeafQueue queue4 =
        scheduler.getQueueManager().getLeafQueue("queue4", true);
    assertEquals("Queue queue4's fair share should be 0", 0, queue4
        .getFairShare().getMemory());
    FSLeafQueue queue5 =
        scheduler.getQueueManager().getLeafQueue("queue5", true);
    assertEquals("Queue queue5's fair share should be 0", 0, queue5
        .getFairShare().getMemory());

    List<String> queues = Arrays.asList("root.queue3", "root.queue4",
        "root.queue5");
    for (String queue : queues) {
      createSchedulingRequest(1 * 1024, queue, "user1");
      scheduler.update();
      scheduler.handle(updateEvent);
    }

    Resource amResource1 = Resource.newInstance(1024, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();

    // The fair share is 2048 MB, and the default maxAMShare is 0.5f,
    // so the AM is accepted.
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queue1", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application1's AM requests 1024 MB memory",
        1024, app1.getAMResource().getMemory());
    assertEquals("Application1's AM should be running",
        1, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemory());

    // Now the fair share is 1639 MB, and the maxAMShare is 0.4f,
    // so the AM is not accepted.
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue2", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application2's AM requests 1024 MB memory",
        1024, app2.getAMResource().getMemory());
    assertEquals("Application2's AM should not be running",
        0, app2.getLiveContainers().size());
    assertEquals("Queue2's AM resource usage should be 0 MB memory",
        0, queue2.getAmResourceUsage().getMemory());
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

    scheduler.init(conf);
    scheduler.start();
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
    scheduler = new FairScheduler();
    Configuration conf = createConfiguration();
    conf.setBoolean(FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED,
            true);
    scheduler.setRMContext(resourceManager.getRMContext());
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    Assert.assertTrue("Continuous scheduling should be enabled.",
        scheduler.isContinuousSchedulingEnabled());

    // Add two nodes
    RMNode node1 =
            MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
                    "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
            MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 2,
                    "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    // available resource
    Assert.assertEquals(scheduler.getClusterResource().getMemory(), 16 * 1024);
    Assert.assertEquals(scheduler.getClusterResource().getVirtualCores(), 16);

    // send application request
    ApplicationAttemptId appAttemptId =
            createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(appAttemptId);

    scheduler.addApplication(appAttemptId.getApplicationId(), "queue11", "user11", false);
    scheduler.addApplicationAttempt(appAttemptId, false, false);
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest request =
            createResourceRequest(1024, 1, ResourceRequest.ANY, 1, 1, true);
    ask.add(request);
    scheduler.allocate(appAttemptId, ask, new ArrayList<ContainerId>(), null, null);

    // waiting for continuous_scheduler_sleep_time
    // at least one pass
    Thread.sleep(scheduler.getConf().getContinuousSchedulingSleepMs() + 500);

    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);
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
    scheduler.allocate(appAttemptId, ask, new ArrayList<ContainerId>(), null, null);

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
  public void testContinuousSchedulingWithNodeRemoved() throws Exception {
    // Disable continuous scheduling, will invoke continuous scheduling once manually
    scheduler.init(conf);
    scheduler.start();
    Assert.assertTrue("Continuous scheduling should be disabled.",
        !scheduler.isContinuousSchedulingEnabled());

    // Add two nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 2,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    Assert.assertEquals("We should have two alive nodes.",
        2, scheduler.getNumClusterNodes());

    // Remove one node
    NodeRemovedSchedulerEvent removeNode1 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(removeNode1);
    Assert.assertEquals("We should only have one alive node.",
        1, scheduler.getNumClusterNodes());

    // Invoke the continuous scheduling once
    try {
      scheduler.continuousSchedulingAttempt();
    } catch (Exception e) {
      fail("Exception happened when doing continuous scheduling. " +
        e.toString());
    }
  }

  @Test
  public void testContinuousSchedulingInterruptedException()
      throws Exception {
    scheduler.init(conf);
    scheduler.start();
    FairScheduler spyScheduler = spy(scheduler);
    Assert.assertTrue("Continuous scheduling should be disabled.",
        !spyScheduler.isContinuousSchedulingEnabled());
    // Add one nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    spyScheduler.handle(nodeEvent1);
    Assert.assertEquals("We should have one alive node.",
        1, spyScheduler.getNumClusterNodes());
    InterruptedException ie = new InterruptedException();
    doThrow(new YarnRuntimeException(ie)).when(spyScheduler).
        attemptScheduling(isA(FSSchedulerNode.class));
    // Invoke the continuous scheduling once
    try {
      spyScheduler.continuousSchedulingAttempt();
      fail("Expected InterruptedException to stop schedulingThread");
    } catch (InterruptedException e) {
      Assert.assertEquals(ie, e);
    }
  }

  @Test
  public void testDontAllowUndeclaredPools() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"jerry\">");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    QueueManager queueManager = scheduler.getQueueManager();

    FSLeafQueue jerryQueue = queueManager.getLeafQueue("jerry", false);
    FSLeafQueue defaultQueue = queueManager.getLeafQueue("default", false);

    // Should get put into jerry
    createSchedulingRequest(1024, "jerry", "someuser");
    assertEquals(1, jerryQueue.getNumRunnableApps());

    // Should get forced into default
    createSchedulingRequest(1024, "newqueue", "someuser");
    assertEquals(1, jerryQueue.getNumRunnableApps());
    assertEquals(1, defaultQueue.getNumRunnableApps());

    // Would get put into someuser because of user-as-default-queue, but should
    // be forced into default
    createSchedulingRequest(1024, "default", "someuser");
    assertEquals(1, jerryQueue.getNumRunnableApps());
    assertEquals(2, defaultQueue.getNumRunnableApps());

    // Should get put into jerry because of user-as-default-queue
    createSchedulingRequest(1024, "default", "jerry");
    assertEquals(2, jerryQueue.getNumRunnableApps());
    assertEquals(2, defaultQueue.getNumRunnableApps());
  }

  @Test
  public void testSchedulingOnRemovedNode() throws Exception {
    // Disable continuous scheduling, will invoke continuous scheduling manually
    scheduler.init(conf);
    scheduler.start();
    Assert.assertTrue("Continuous scheduling should be disabled.",
        !scheduler.isContinuousSchedulingEnabled());

    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);

    scheduler.addApplication(id11.getApplicationId(), "root.queue1", "user1",
        false);
    scheduler.addApplicationAttempt(id11, false, false);

    List<ResourceRequest> ask1 = new ArrayList<>();
    ResourceRequest request1 =
        createResourceRequest(1024, 8, ResourceRequest.ANY, 1, 1, true);

    ask1.add(request1);
    scheduler.allocate(id11, ask1, new ArrayList<ContainerId>(), null,
        null);

    String hostName = "127.0.0.1";
    RMNode node1 = MockNodes.newNodeInfo(1,
      Resources.createResource(8 * 1024, 8), 1, hostName);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    FSSchedulerNode node = (FSSchedulerNode)scheduler.getSchedulerNode(
      node1.getNodeID());

    NodeRemovedSchedulerEvent removeNode1 =
        new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(removeNode1);

    scheduler.attemptScheduling(node);

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(id11,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
  }

  @Test
  public void testDefaultRuleInitializesProperlyWhenPolicyNotConfigured()
      throws IOException {
    // This test verifies if default rule in queue placement policy
    // initializes properly when policy is not configured and
    // undeclared pools is not allowed.
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);

    // Create an alloc file with no queue placement policy
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    List<QueuePlacementRule> rules = scheduler.allocConf.placementPolicy
        .getRules();

    for (QueuePlacementRule rule : rules) {
      if (rule instanceof Default) {
        Default defaultRule = (Default) rule;
        assertNotNull(defaultRule.defaultQueueName);
      }
    }
  }
    
  @Test(timeout=5000)
  public void testRecoverRequestAfterPreemption() throws Exception {
    conf.setLong(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 10);
    
    MockClock clock = new MockClock();
    scheduler.setClock(clock);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    Priority priority = Priority.newInstance(20);
    String host = "127.0.0.1";
    int GB = 1024;

    // Create Node and raised Node Added event
    RMNode node = MockNodes.newNodeInfo(1,
        Resources.createResource(16 * 1024, 4), 0, host);
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    // Create 3 container requests and place it in ask
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest nodeLocalRequest = createResourceRequest(GB, 1, host,
        priority.getPriority(), 1, true);
    ResourceRequest rackLocalRequest = createResourceRequest(GB, 1,
        node.getRackName(), priority.getPriority(), 1, true);
    ResourceRequest offRackRequest = createResourceRequest(GB, 1,
        ResourceRequest.ANY, priority.getPriority(), 1, true);
    ask.add(nodeLocalRequest);
    ask.add(rackLocalRequest);
    ask.add(offRackRequest);

    // Create Request and update
    ApplicationAttemptId appAttemptId = createSchedulingRequest("queueA",
        "user1", ask);
    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeUpdate);

    assertEquals(1, scheduler.getSchedulerApp(appAttemptId).getLiveContainers()
        .size());
    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);

    // ResourceRequest will be empty once NodeUpdate is completed
    Assert.assertNull(app.getResourceRequest(priority, host));

    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    RMContainer rmContainer = app.getRMContainer(containerId1);

    // Create a preempt event and register for preemption
    scheduler.warnOrKillContainer(rmContainer);
    
    // Wait for few clock ticks
    clock.tick(5);
    
    // preempt now
    scheduler.warnOrKillContainer(rmContainer);

    // Trigger container rescheduled event
    scheduler.handle(new ContainerRescheduledEvent(rmContainer));

    List<ResourceRequest> requests = rmContainer.getResourceRequests();
    // Once recovered, resource request will be present again in app
    Assert.assertEquals(3, requests.size());
    for (ResourceRequest request : requests) {
      Assert.assertEquals(1,
          app.getResourceRequest(priority, request.getResourceName())
              .getNumContainers());
    }

    // Send node heartbeat
    scheduler.update();
    scheduler.handle(nodeUpdate);

    List<Container> containers = scheduler.allocate(appAttemptId,
        Collections.<ResourceRequest> emptyList(),
        Collections.<ContainerId> emptyList(), null, null).getContainers();

    // Now with updated ResourceRequest, a container is allocated for AM.
    Assert.assertTrue(containers.size() == 1);
  }
  
  @SuppressWarnings("resource")
  @Test
  public void testBlacklistNodes() throws Exception {
    scheduler.init(conf);
    scheduler.start();
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
    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);

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
    scheduler.init(conf);
    scheduler.start();
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
    AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> scheduler =
        (AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) resourceManager
          .getResourceScheduler();
    TestSchedulerUtils.verifyAppAddedAndRemovedFromScheduler(
      scheduler.getSchedulerApplications(), scheduler, "default");
  }

  @Test
  public void testMoveRunnableApp() throws Exception {
    scheduler.init(conf);
    scheduler.start();
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
    FSAppAttempt app = scheduler.getSchedulerApp(appAttId);
    assertSame(targetQueue, app.getQueue());
    assertFalse(oldQueue.isRunnableApp(app));
    assertTrue(targetQueue.isRunnableApp(app));
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
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    QueueManager queueMgr = scheduler.getQueueManager();
    FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
    FSLeafQueue targetQueue = queueMgr.getLeafQueue("queue2", true);
    scheduler.getAllocationConfiguration().queueMaxApps.put("root.queue1", 0);
    
    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    
    FSAppAttempt app = scheduler.getSchedulerApp(appAttId);
    assertTrue(oldQueue.isNonRunnableApp(app));
    
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    assertFalse(oldQueue.isNonRunnableApp(app));
    assertFalse(targetQueue.isNonRunnableApp(app));
    assertTrue(targetQueue.isRunnableApp(app));
    assertEquals(1, targetQueue.getNumRunnableApps());
    assertEquals(1, queueMgr.getRootQueue().getNumRunnableApps());
  }
    
  @Test (expected = YarnException.class)
  public void testMoveWouldViolateMaxAppsConstraints() throws Exception {
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
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
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.getQueueManager().getLeafQueue("queue1", true);
    
    ApplicationAttemptId appAttId =
        createSchedulingRequest(1024, 1, "queue1", "user1", 3);
    scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
  }

  @Test
  public void testLowestCommonAncestorForNonRootParent() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSLeafQueue aQueue = mock(FSLeafQueue.class);
    FSLeafQueue bQueue = mock(FSLeafQueue.class);
    when(aQueue.getName()).thenReturn("root.queue1.a");
    when(bQueue.getName()).thenReturn("root.queue1.b");

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("queue1", true);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    FSQueue ancestorQueue =
        scheduler.findLowestCommonAncestorQueue(aQueue, bQueue);
    assertEquals(ancestorQueue, queue1);
  }

  @Test
  public void testLowestCommonAncestorRootParent() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSLeafQueue aQueue = mock(FSLeafQueue.class);
    FSLeafQueue bQueue = mock(FSLeafQueue.class);
    when(aQueue.getName()).thenReturn("root.a");
    when(bQueue.getName()).thenReturn("root.b");

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("root", false);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    FSQueue ancestorQueue =
        scheduler.findLowestCommonAncestorQueue(aQueue, bQueue);
    assertEquals(ancestorQueue, queue1);
  }

  @Test
  public void testLowestCommonAncestorDeeperHierarchy() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSQueue aQueue = mock(FSLeafQueue.class);
    FSQueue bQueue = mock(FSLeafQueue.class);
    FSQueue a1Queue = mock(FSLeafQueue.class);
    FSQueue b1Queue = mock(FSLeafQueue.class);
    when(a1Queue.getName()).thenReturn("root.queue1.a.a1");
    when(b1Queue.getName()).thenReturn("root.queue1.b.b1");
    when(aQueue.getChildQueues()).thenReturn(Arrays.asList(a1Queue));
    when(bQueue.getChildQueues()).thenReturn(Arrays.asList(b1Queue));

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("queue1", true);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    FSQueue ancestorQueue =
        scheduler.findLowestCommonAncestorQueue(a1Queue, b1Queue);
    assertEquals(ancestorQueue, queue1);
  }

  @Test
  public void testThreadLifeCycle() throws InterruptedException {
    conf.setBoolean(
        FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED, true);
    scheduler.init(conf);
    scheduler.start();

    Thread updateThread = scheduler.updateThread;
    Thread schedulingThread = scheduler.schedulingThread;

    assertTrue(updateThread.isAlive());
    assertTrue(schedulingThread.isAlive());

    scheduler.stop();

    int numRetries = 100;
    while (numRetries-- > 0 &&
        (updateThread.isAlive() || schedulingThread.isAlive())) {
      Thread.sleep(50);
    }

    assertNotEquals("One of the threads is still alive", 0, numRetries);
  }

  @Test
  public void testPerfMetricsInited() {
    scheduler.init(conf);
    scheduler.start();
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    scheduler.fsOpDurations.getMetrics(collector, true);
    assertEquals("Incorrect number of perf metrics", 1,
        collector.getRecords().size());
  }
}
