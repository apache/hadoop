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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueuePlacementRule.Default;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import com.google.common.collect.Sets;

@SuppressWarnings("unchecked")
public class TestFairScheduler extends FairSchedulerTestBase {
  private final int GB = 1024;
  private final static String ALLOC_FILE =
      new File(TEST_DIR, "test-queues").getAbsolutePath();

  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    conf = createConfiguration();
    resourceManager = new MockRM(conf);

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
    YarnAuthorizationProvider.destroy();
  }


  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
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

  @SuppressWarnings("deprecation")
  @Test(timeout=2000)
  public void testLoadConfigurationOnInitialize() throws IOException {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setInt(FairSchedulerConfiguration.MAX_ASSIGN, 3);
    conf.setBoolean(FairSchedulerConfiguration.SIZE_BASED_WEIGHT, true);
    conf.setDouble(FairSchedulerConfiguration.LOCALITY_THRESHOLD_NODE, .5);
    conf.setDouble(FairSchedulerConfiguration.LOCALITY_THRESHOLD_RACK, .7);
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
    ResourceUtils.resetResourceTypes(conf);
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
    Assert.assertEquals(1024, scheduler.getMaximumResourceCapability().getMemorySize());
    Assert.assertEquals(512, scheduler.getMinimumResourceCapability().getMemorySize());
    Assert.assertEquals(128, 
      scheduler.getIncrementResourceCapability().getMemorySize());
  }
  
  @Test  
  public void testNonMinZeroResourcesSettings() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 1);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    ResourceUtils.resetResourceTypes(conf);
    scheduler.init(conf);
    scheduler.reinitialize(conf, null);
    Assert.assertEquals(256, scheduler.getMinimumResourceCapability().getMemorySize());
    Assert.assertEquals(1, scheduler.getMinimumResourceCapability().getVirtualCores());
    Assert.assertEquals(512, scheduler.getIncrementResourceCapability().getMemorySize());
    Assert.assertEquals(2, scheduler.getIncrementResourceCapability().getVirtualCores());
  }  
  
  @Test  
  public void testMinZeroResourcesSettings() throws IOException {  
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 0);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    ResourceUtils.resetResourceTypes(conf);
    scheduler.init(conf);
    scheduler.reinitialize(conf, null);
    Assert.assertEquals(0, scheduler.getMinimumResourceCapability().getMemorySize());
    Assert.assertEquals(0, scheduler.getMinimumResourceCapability().getVirtualCores());
    Assert.assertEquals(512, scheduler.getIncrementResourceCapability().getMemorySize());
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
    assertEquals(1024, scheduler.getClusterResource().getMemorySize());

    // Add another node
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(512), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    assertEquals(1536, scheduler.getClusterResource().getMemorySize());

    // Remove the first node
    NodeRemovedSchedulerEvent nodeEvent3 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent3);
    assertEquals(512, scheduler.getClusterResource().getMemorySize());
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
      assertEquals(3414, p.getFairShare().getMemorySize());
      assertEquals(3414, p.getMetrics().getFairShareMB());
      assertEquals(3414, p.getSteadyFairShare().getMemorySize());
      assertEquals(3414, p.getMetrics().getSteadyFairShareMB());
    }
  }

  /**
   * Test fair shares when max resources are set but are too high to impact
   * the shares.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testFairShareWithHighMaxResources() throws IOException {
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
    assertEquals("Queue A did not get its expected fair share",
        2 * 1024, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.75, so its fair share should be 6 * 1024.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals("Queue B did not get its expected fair share",
        6 * 1024, queue.getFairShare().getMemorySize());
  }

  /**
   * Test fair shares when max resources are set and are low enough to impact
   * the shares.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testFairShareWithLowMaxResources() throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));

    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"queueA\">");
    out.println("    <maxResources>1024 mb 1 vcores</maxResources>");
    out.println("    <weight>0.75</weight>");
    out.println("  </queue>");
    out.println("  <queue name=\"queueB\">");
    out.println("    <maxResources>3072 mb 3 vcores</maxResources>");
    out.println("    <weight>0.25</weight>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");

    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId attId1 =
        createSchedulingRequest(1024, 1, "queueA", "user1", 2);
    ApplicationAttemptId attId2 =
        createSchedulingRequest(1024, 1, "queueB", "user1", 4);

    scheduler.update();

    FSLeafQueue queue =
        scheduler.getQueueManager().getLeafQueue("queueA", false);
    // queueA's weight is 0.5, so its fair share should be 6GB, but it's
    // capped at 1GB.
    assertEquals("Queue A did not get its expected fair share",
        1 * 1024, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.5, so its fair share should be 2GB, but the
    // other queue is capped at 1GB, so queueB's share is 7GB,
    // capped at 3GB.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals("Queue B did not get its expected fair share",
        3 * 1024, queue.getFairShare().getMemorySize());

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);

    // App 1 should be running with 1 container
    assertEquals("App 1 is not running with the correct number of containers",
        1, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
    // App 2 should be running with 3 containers
    assertEquals("App 2 is not running with the correct number of containers",
        3, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
  }

  /**
   * Test the child max resource settings.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testChildMaxResources() throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));

    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"queueA\" type=\"parent\">");
    out.println("    <maxChildResources>2048mb,2vcores</maxChildResources>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");

    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId attId1 =
        createSchedulingRequest(1024, 1, "queueA.queueB", "user1", 8);
    ApplicationAttemptId attId2 =
        createSchedulingRequest(1024, 1, "queueA.queueC", "user1", 8);

    scheduler.update();

    NodeUpdateSchedulerEvent nodeEvent = new NodeUpdateSchedulerEvent(node1);

    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);

    // Apps should be running with 2 containers
    assertEquals("App 1 is not running with the correct number of containers",
        2, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
    assertEquals("App 2 is not running with the correct number of containers",
        2, scheduler.getSchedulerApp(attId2).getLiveContainers().size());

    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"queueA\" type=\"parent\">");
    out.println("    <maxChildResources>3072mb,3vcores</maxChildResources>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.update();
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);

    // Apps should be running with 3 containers now
    assertEquals("App 1 is not running with the correct number of containers",
        3, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
    assertEquals("App 2 is not running with the correct number of containers",
        3, scheduler.getSchedulerApp(attId2).getLiveContainers().size());

    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"queueA\" type=\"parent\">");
    out.println("    <maxChildResources>1024mb,1vcores</maxChildResources>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.update();
    scheduler.handle(nodeEvent);

    // Apps still should be running with 3 containers because we don't preempt
    assertEquals("App 1 is not running with the correct number of containers",
        3, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
    assertEquals("App 2 is not running with the correct number of containers",
        3, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
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
    assertEquals(0, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.0, so its fair share should be 0.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(0, queue.getFairShare().getMemorySize());
  }

  /**
   * Test if we compute the maximum AM resource correctly.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testComputeMaxAMResource() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueFSZeroWithMax\">");
    out.println("<weight>0</weight>");
    out.println("<maxAMShare>0.5</maxAMShare>");
    out.println("<maxResources>4096 mb 4 vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueFSZeroWithAVL\">");
    out.println("<weight>0.0</weight>");
    out.println("<maxAMShare>0.5</maxAMShare>");
    out.println("</queue>");
    out.println("<queue name=\"queueFSNonZero\">");
    out.println("<weight>1</weight>");
    out.println("<maxAMShare>0.5</maxAMShare>");
    out.println("</queue>");
    out.println("<defaultQueueSchedulingPolicy>drf" +
        "</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    long memCapacity = 20 * GB;
    int cpuCapacity = 20;
    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(memCapacity,
            cpuCapacity), 0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    Resource amResource = Resource.newInstance(1 * GB, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();

    // queueFSZeroWithMax
    FSLeafQueue queueFSZeroWithMax = scheduler.getQueueManager().
        getLeafQueue("queueFSZeroWithMax", true);
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queueFSZeroWithMax", "user1",
        amResource);
    createSchedulingRequestExistingApplication(1 * GB, 1, amPriority, attId1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // queueFSZeroWithMax's weight is 0.0, so its fair share should be 0, we use
    // the min(maxShare, available resource) to compute maxAMShare, in this
    // case, we use maxShare, since it is smaller than available resource.
    assertEquals("QueueFSZeroWithMax's fair share should be zero",
        0, queueFSZeroWithMax.getFairShare().getMemorySize());
    Resource expectedAMResource = Resources.multiplyAndRoundUp(
        queueFSZeroWithMax.getMaxShare(), queueFSZeroWithMax.getMaxAMShare());
    assertEquals("QueueFSZeroWithMax's maximum AM resource should be "
        + "maxShare * maxAMShare", expectedAMResource.getMemorySize(),
        queueFSZeroWithMax.getMetrics().getMaxAMShareMB());
    assertEquals("QueueFSZeroWithMax's maximum AM resource should be "
        + "maxShare * maxAMShare", expectedAMResource.getVirtualCores(),
        queueFSZeroWithMax.getMetrics().getMaxAMShareVCores());
    assertEquals("QueueFSZeroWithMax's AM resource usage should be the same to "
        + "AM resource request",
        amResource.getMemorySize(),
        queueFSZeroWithMax.getMetrics().getAMResourceUsageMB());

    // queueFSZeroWithAVL
    amResource = Resources.createResource(1 * GB, 1);
    FSLeafQueue queueFSZeroWithAVL = scheduler.getQueueManager().
        getLeafQueue("queueFSZeroWithAVL", true);
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queueFSZeroWithAVL", "user1",
        amResource);
    createSchedulingRequestExistingApplication(1 * GB, 1, amPriority, attId2);
    scheduler.update();
    scheduler.handle(updateEvent);

    // queueFSZeroWithAVL's weight is 0.0, so its fair share is 0, and we use
    // the min(maxShare, available resource) to compute maxAMShare, in this
    // case, we use available resource since it is smaller than the
    // default maxShare.
    expectedAMResource = Resources.multiplyAndRoundUp(
        Resources.createResource(memCapacity - amResource.getMemorySize(),
            cpuCapacity - amResource.getVirtualCores()),
        queueFSZeroWithAVL.getMaxAMShare());
    assertEquals("QueueFSZeroWithAVL's fair share should be zero",
        0, queueFSZeroWithAVL.getFairShare().getMemorySize());
    assertEquals("QueueFSZeroWithAVL's maximum AM resource should be "
        + " available resource * maxAMShare",
        expectedAMResource.getMemorySize(),
        queueFSZeroWithAVL.getMetrics().getMaxAMShareMB());
    assertEquals("QueueFSZeroWithAVL's maximum AM resource should be "
        + " available resource * maxAMShare",
        expectedAMResource.getVirtualCores(),
        queueFSZeroWithAVL.getMetrics().getMaxAMShareVCores());
    assertEquals("QueueFSZeroWithMax's AM resource usage should be the same to "
        + "AM resource request",
        amResource.getMemorySize(),
        queueFSZeroWithAVL.getMetrics().getAMResourceUsageMB());

    // queueFSNonZero
    amResource = Resources.createResource(1 * GB, 1);
    FSLeafQueue queueFSNonZero = scheduler.getQueueManager().
        getLeafQueue("queueFSNonZero", true);
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queueFSNonZero", "user1",
        amResource);
    createSchedulingRequestExistingApplication(1 * GB, 1, amPriority, attId3);
    scheduler.update();
    scheduler.handle(updateEvent);

    // queueFSNonZero's weight is 1, so its fair share is not 0, and we use the
    // fair share to compute maxAMShare
    assertNotEquals("QueueFSNonZero's fair share shouldn't be zero",
        0, queueFSNonZero.getFairShare().getMemorySize());
    expectedAMResource = Resources.multiplyAndRoundUp(
        queueFSNonZero.getFairShare(), queueFSNonZero.getMaxAMShare());
    assertEquals("QueueFSNonZero's maximum AM resource should be "
        + " fair share * maxAMShare", expectedAMResource.getMemorySize(),
        queueFSNonZero.getMetrics().getMaxAMShareMB());
    assertEquals("QueueFSNonZero's maximum AM resource should be "
        + " fair share * maxAMShare", expectedAMResource.getVirtualCores(),
        queueFSNonZero.getMetrics().getMaxAMShareVCores());
    assertEquals("QueueFSNonZero's AM resource usage should be the same to "
        + "AM resource request",
        amResource.getMemorySize(),
        queueFSNonZero.getMetrics().getAMResourceUsageMB());
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
    assertEquals(1, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.0 and minResources is 1,
    // so its fair share should be 1 (minShare).
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(1, queue.getFairShare().getMemorySize());
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
    assertEquals(4096, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.5 and minResources is 1024,
    // so its fair share should be 4096.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(4096, queue.getFairShare().getMemorySize());
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
    // test queueMetrics
    Assert.assertEquals(0, queueInfo.getQueueStatistics()
        .getAllocatedContainers());
    Assert.assertEquals(0, queueInfo.getQueueStatistics()
        .getAllocatedMemoryMB());
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
    // test queueMetrics
    Assert.assertEquals(1, queueInfo.getQueueStatistics()
        .getAllocatedContainers());
    Assert.assertEquals(1024, queueInfo.getQueueStatistics()
        .getAllocatedMemoryMB());
    queueInfo = scheduler.getQueueInfo("queueB", false, false);
    Assert.assertEquals(0.75f, queueInfo.getCapacity(), 0.0f);
    Assert.assertEquals(1.0f, queueInfo.getCurrentCapacity(), 0.0f);
    // test queueMetrics
    Assert.assertEquals(1, queueInfo.getQueueStatistics()
        .getAllocatedContainers());
    Assert.assertEquals(6144, queueInfo.getQueueStatistics()
        .getAllocatedMemoryMB());
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
    assertEquals(capacity / 2, queue1.getFairShare().getMemorySize());
    assertEquals(capacity / 2, queue1.getMetrics().getFairShareMB());
    assertEquals(capacity / 2, queue1.getSteadyFairShare().getMemorySize());
    assertEquals(capacity / 2, queue1.getMetrics().getSteadyFairShareMB());
    assertEquals(capacity / 4, queue2.getFairShare().getMemorySize());
    assertEquals(capacity / 4, queue2.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue2.getSteadyFairShare().getMemorySize());
    assertEquals(capacity / 4, queue2.getMetrics().getSteadyFairShareMB());
    assertEquals(capacity / 4, queue3.getFairShare().getMemorySize());
    assertEquals(capacity / 4, queue3.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue3.getSteadyFairShare().getMemorySize());
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
    assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);

    assertEquals(1024, scheduler.getQueueManager().getQueue("queue1").
      getResourceUsage().getMemorySize());
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
        getResourceUsage().getMemorySize());

    // Now queue 2 requests likewise
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue2", "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is waiting with a reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
    assertEquals(1024, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemorySize());

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
        getResourceUsage().getMemorySize());

    // The old reservation should still be there...
    assertEquals(1024, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemorySize());
    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemorySize());

  }

  @Test (timeout = 5000)
  public void testOffSwitchAppReservationThreshold() throws Exception {
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0.50f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add three node
    RMNode node1 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);


    // Ensure capacity on all nodes are allocated
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));

    // Verify capacity allocation
    assertEquals(6144, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    // Create new app with a resource request that can be satisfied by any
    // node but would be
    ApplicationAttemptId attId = createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));

    assertEquals(1,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));

    // No new reservations should happen since it exceeds threshold
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));

    // Add 1 more node
    RMNode node4 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent4 = new NodeAddedSchedulerEvent(node4);
    scheduler.handle(nodeEvent4);

    // New node satisfies resource request
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));
    assertEquals(8192, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));
    scheduler.update();

    // Verify number of reservations have decremented
    assertEquals(0,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
  }

  @Test (timeout = 5000)
  public void testRackLocalAppReservationThreshold() throws Exception {
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0.50f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add four node
    RMNode node1 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // These 3 on different rack
    RMNode node2 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);

    RMNode node4 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent4 = new NodeAddedSchedulerEvent(node4);
    scheduler.handle(nodeEvent4);

    // Ensure capacity on all nodes are allocated
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));

    // Verify capacity allocation
    assertEquals(8192, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    // Create new app with a resource request that can be satisfied by any
    // node but would be
    ApplicationAttemptId attemptId =
            createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(attemptId);

    scheduler.addApplication(attemptId.getApplicationId(), "queue1", "user1",
            false);
    scheduler.addApplicationAttempt(attemptId, false, false);
    List<ResourceRequest> asks = new ArrayList<ResourceRequest>();
    asks.add(createResourceRequest(2048, node2.getRackName(), 1, 1, false));

    scheduler.allocate(attemptId, asks, null, new ArrayList<ContainerId>(), null,
            null, NULL_UPDATE_REQUESTS);

    ApplicationAttemptId attId = createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));

    assertEquals(1,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));

    // No new reservations should happen since it exceeds threshold
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));

    // Add 1 more node
    RMNode node5 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent5 = new NodeAddedSchedulerEvent(node5);
    scheduler.handle(nodeEvent5);

    // New node satisfies resource request
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));
    assertEquals(10240, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));
    scheduler.update();

    // Verify number of reservations have decremented
    assertEquals(0,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
  }

  @Test (timeout = 5000)
  public void testReservationThresholdWithAssignMultiple() throws Exception {
    // set reservable-nodes to 0 which make reservation exceed
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0f);
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setBoolean(FairSchedulerConfiguration.DYNAMIC_MAX_ASSIGN, false);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add two node
    RMNode node1 =
        MockNodes
                .newNodeInfo(1, Resources.createResource(4096, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
        MockNodes
                .newNodeInfo(2, Resources.createResource(4096, 4), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    //create one request and assign containers
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue1", "user1", 10);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));

    // Verify capacity allocation
    assertEquals(8192, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    // Verify number of reservations have decremented
    assertEquals(0,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
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
        getResourceUsage().getMemorySize());

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Ensure the reservation does not get created as allocated memory of
    // queue1 exceeds max
    assertEquals(0, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemorySize());
  }

  /**
   * The test verifies that zero-FairShare queues (because of zero/tiny
   * weight) can get resources for the AM.
   */
  @Test
  public void testRequestAMResourceInZeroFairShareQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("<weight>0.0</weight>");
    out.println("<maxResources>4096mb,10vcores</maxResources>");
    out.println("<maxAMShare>0.5</maxAMShare>");
    out.println("</queue>");
    out.println("<queue name=\"queue2\">");
    out.println("<weight>2.0</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queue3\">");
    out.println("<weight>0.000001</weight>");
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

    //create request for non-zero weight queue
    createSchedulingRequest(1024, "root.queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // A managed AM which need 3G memory will not get resource,
    // since it request more than the maxAMShare (4G * 0.5 = 2G).
    Resource amResource = Resource.newInstance(1024, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "root.queue1", "user1", amResource);
    createSchedulingRequestExistingApplication(3 * 1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application 1 should not be running",
            0, app1.getLiveContainers().size());

    // A managed AM which need 2G memory will get resource,
    // since it request no more than the maxAMShare (4G * 0.5 = 2G).
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "root.queue1", "user1", amResource);
    createSchedulingRequestExistingApplication(2 * 1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application 2 should be running",
            1, app2.getLiveContainers().size());

    // A managed AM which need 1G memory will get resource, even thought its
    // fair share is 0 because its weight is tiny(0.000001).
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "root.queue3", "user1", amResource);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application 3 should be running",
            1, app3.getLiveContainers().size());
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
        getResourceUsage().getMemorySize());

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
      getResourceUsage().getMemorySize());
    
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 1 is waiting with a reservation
    assertEquals(1024, scheduler.getSchedulerApp(attId1)
        .getCurrentReservation().getMemorySize());

    // Exercise checks that reservation fits
    scheduler.handle(updateEvent);

    // Ensure the reservation still exists as allocated memory of queue1 doesn't
    // exceed max
    assertEquals(1024, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemorySize());

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
        getResourceUsage().getMemorySize());
    //the reservation of queue1 should be reclaim
    assertEquals(0, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemorySize());
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
  }

  @Test
  public void testReservationThresholdGatesReservations() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<defaultQueueSchedulingPolicy>drf" +
        "</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    // Set threshold to 2 * 1024 ==> 2048 MB & 2 * 1 ==> 2 vcores (test will
    // use vcores)
    conf.setFloat(FairSchedulerConfiguration.
            RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE,
        2f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(4096, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of node
    createSchedulingRequest(4096, 4, "queue1", "user1", 1, 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);

    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(4096, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    // Now queue 2 requests below threshold
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue2", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 has no reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
    assertEquals(0,
        scheduler.getSchedulerApp(attId).getReservedContainers().size());

    // Now queue requests CPU above threshold
    createSchedulingRequestExistingApplication(1024, 3, 1, attId);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is waiting with a reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
    assertEquals(3, scheduler.getSchedulerApp(attId).getCurrentReservation()
        .getVirtualCores());

    // Now another node checks in with capacity
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024, 4), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);

    // Make sure this goes to queue 2
    assertEquals(3, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getVirtualCores());

    // The old reservation should still be there...
    assertEquals(3, scheduler.getSchedulerApp(attId).getCurrentReservation()
        .getVirtualCores());
    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.getSchedulerApp(attId).getCurrentReservation()
        .getVirtualCores());
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
  public void testAssignToBadDefaultQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queuePlacementPolicy>");
    out.println("<rule name=\"specified\" create=\"false\" />");
    out.println("<rule name=\"default\" create=\"false\" />");
    out.println("</queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);

    try {
      FSLeafQueue queue1 = scheduler.assignToQueue(rmApp1, "default",
          "asterix");
    } catch (IllegalStateException ise) {
      fail("Bad queue placement policy terminal rule should not throw " +
          "exception ");
    }
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
        assertEquals(1024, p.getFairShare().getMemorySize());
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(2048, p.getFairShare().getMemorySize());
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
        assertEquals(capacity / 4, leaf.getFairShare().getMemorySize());
        // assert that the steady fair share is 1/4th node1's capacity
        assertEquals(capacity / 4, leaf.getSteadyFairShare().getMemorySize());
        // assert weights are equal for both the user queues
        assertEquals(1.0, leaf.getWeight(), 0);
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
        .getSteadyFairShare().getMemorySize());
    assertEquals(0, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());

    // Add one node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(6144), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(6144, scheduler.getClusterResource().getMemorySize());

    // The steady fair shares for all queues should be updated
    assertEquals(2048, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(2048, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());

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
        .getSteadyFairShare().getMemorySize());
    assertEquals(2048, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(2048, queueManager.getLeafQueue("child3", false)
        .getSteadyFairShare().getMemorySize());

    // Remove the node, steady fair shares should back to 0
    NodeRemovedSchedulerEvent nodeEvent2 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);
    assertEquals(0, scheduler.getClusterResource().getMemorySize());
    assertEquals(0, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(0, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());
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
    assertEquals(6144, scheduler.getClusterResource().getMemorySize());
    assertEquals(6144, scheduler.getQueueManager().getRootQueue()
        .getSteadyFairShare().getMemorySize());
    assertEquals(6144, scheduler.getQueueManager()
        .getLeafQueue("default", false).getSteadyFairShare().getMemorySize());

    // Submit one application
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId1, "default", "user1", null);
    assertEquals(3072, scheduler.getQueueManager()
        .getLeafQueue("default", false).getSteadyFairShare().getMemorySize());
    assertEquals(3072, scheduler.getQueueManager()
        .getLeafQueue("user1", false).getSteadyFairShare().getMemorySize());
  }

  /**
   * Make allocation requests and ensure they are reflected in queue demand.
   */
  @Test
  public void testQueueDemandCalculation() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    int minReqSize =
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB;

    // First ask, queue1 requests 1 large (minReqSize * 2).
    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);
    scheduler.addApplication(id11.getApplicationId(),
        "root.queue1", "user1", false);
    scheduler.addApplicationAttempt(id11, false, false);
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ResourceRequest request1 = createResourceRequest(minReqSize * 2,
        ResourceRequest.ANY, 1, 1, true);
    ask1.add(request1);
    scheduler.allocate(id11, ask1, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    // Second ask, queue2 requests 1 large.
    ApplicationAttemptId id21 = createAppAttemptId(2, 1);
    createMockRMApp(id21);
    scheduler.addApplication(id21.getApplicationId(),
        "root.queue2", "user1", false);
    scheduler.addApplicationAttempt(id21, false, false);
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ResourceRequest request2 = createResourceRequest(2 * minReqSize,
        "foo", 1, 1, false);
    ResourceRequest request3 = createResourceRequest(2 * minReqSize,
        ResourceRequest.ANY, 1, 1, false);
    ask2.add(request2);
    ask2.add(request3);
    scheduler.allocate(id21, ask2, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    // Third ask, queue2 requests 2 small (minReqSize).
    ApplicationAttemptId id22 = createAppAttemptId(2, 2);
    createMockRMApp(id22);
    scheduler.addApplication(id22.getApplicationId(),
        "root.queue2", "user1", false);
    scheduler.addApplicationAttempt(id22, false, false);
    List<ResourceRequest> ask3 = new ArrayList<ResourceRequest>();
    ResourceRequest request4 = createResourceRequest(minReqSize,
        "bar", 2, 2, true);
    ResourceRequest request5 = createResourceRequest(minReqSize,
        ResourceRequest.ANY, 2, 2, true);
    ask3.add(request4);
    ask3.add(request5);
    scheduler.allocate(id22, ask3, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    scheduler.update();

    assertEquals(2 * minReqSize, scheduler.getQueueManager().getQueue("root.queue1")
        .getDemand().getMemorySize());
    assertEquals(2 * minReqSize + 2 * minReqSize, scheduler
        .getQueueManager().getQueue("root.queue2").getDemand()
        .getMemorySize());
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

  @Test(timeout = 5000)
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
        scheduler.getSchedulerApp(attId1).getCurrentReservation().getMemorySize());
    assertEquals(0,
        scheduler.getSchedulerApp(attId2).getCurrentReservation().getMemorySize());
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

  /**
   * Reserve at a lower priority and verify the lower priority request gets
   * allocated
   */
  @Test (timeout = 5000)
  public void testReservationWithMultiplePriorities() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2048, 2));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);

    // Create first app and take up half resources so the second app that asks
    // for the entire node won't have enough.
    FSAppAttempt app1 = scheduler.getSchedulerApp(
        createSchedulingRequest(1024, 1, "queue", "user", 1));
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Basic allocation failed", 1, app1.getLiveContainers().size());

    // Create another app and reserve at a lower priority first
    ApplicationAttemptId attId =
        createSchedulingRequest(2048, 2, "queue1", "user1", 1, 2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Reservation at lower priority failed",
        1, app2.getReservedContainers().size());

    // Request container on the second app at a higher priority
    createSchedulingRequestExistingApplication(2048, 2, 1, attId);

    // Complete the first container so we can trigger allocation for app2
    ContainerId containerId =
        app1.getLiveContainers().iterator().next().getContainerId();
    scheduler.allocate(app1.getApplicationAttemptId(), new ArrayList<>(), null,
        Arrays.asList(containerId), null, null, NULL_UPDATE_REQUESTS);

    // Trigger allocation for app2
    scheduler.handle(updateEvent);

    // Reserved container (at lower priority) should be run
    Collection<RMContainer> liveContainers = app2.getLiveContainers();
    assertEquals("Allocation post completion failed", 1, liveContainers.size());
    assertEquals("High prio container allocated against low prio reservation",
        2, liveContainers.iterator().next().getContainer().
            getPriority().getPriority());
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

    scheduler.allocate(attemptId, asks, null, new ArrayList<ContainerId>(), null,
        null, NULL_UPDATE_REQUESTS);
    
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
  public void testFixedMaxAssign() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setBoolean(FairSchedulerConfiguration.DYNAMIC_MAX_ASSIGN, false);
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


  /**
   * Test to verify the behavior of dynamic-max-assign.
   * 1. Verify the value of maxassign doesn't affect number of containers
   * affected.
   * 2. Verify the node is fully allocated.
   */
  @Test(timeout = 3000)
  public void testDynamicMaxAssign() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(8192, 8), 0,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId attId =
        createSchedulingRequest(1024, 1, "root.default", "user", 12);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

    // Set maxassign to a value smaller than half the remaining resources
    scheduler.maxAssign = 2;
    scheduler.update();
    scheduler.handle(updateEvent);
    // New container allocations should be floor(8/2) + 1 = 5
    assertEquals("Incorrect number of containers allocated", 5,
        app.getLiveContainers().size());

    // Set maxassign to a value larger than half the remaining resources
    scheduler.maxAssign = 4;
    scheduler.update();
    scheduler.handle(updateEvent);
    // New container allocations should be floor(3/2) + 1 = 2
    assertEquals("Incorrect number of containers allocated", 7,
        app.getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);
    // New container allocations should be 1
    assertEquals("Incorrect number of containers allocated", 8,
        app.getLiveContainers().size());
  }

  @Test(timeout = 3000)
  public void testMaxAssignWithZeroMemoryContainers() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setBoolean(FairSchedulerConfiguration.DYNAMIC_MAX_ASSIGN, false);
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
    scheduler.allocate(attId1, update, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);
    
    // then node2 should get the container
    scheduler.handle(node2UpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  @Test
  public void testAMStrictLocalityRack() throws IOException {
    testAMStrictLocality(false, false);
  }

  @Test
  public void testAMStrictLocalityNode() throws IOException {
    testAMStrictLocality(true, false);
  }

  @Test
  public void testAMStrictLocalityRackInvalid() throws IOException {
    testAMStrictLocality(false, true);
  }

  @Test
  public void testAMStrictLocalityNodeInvalid() throws IOException {
    testAMStrictLocality(true, true);
  }

  private void testAMStrictLocality(boolean node, boolean invalid)
      throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1,
        "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(2, Resources.createResource(1024), 2,
        "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    List<ResourceRequest> reqs = new ArrayList<>();
    ResourceRequest nodeRequest = createResourceRequest(1024,
        node2.getHostName(), 1, 1, true);
    if (node && invalid) {
      nodeRequest.setResourceName("invalid");
    }
    ResourceRequest rackRequest = createResourceRequest(1024,
        node2.getRackName(), 1, 1, !node);
    if (!node && invalid) {
      rackRequest.setResourceName("invalid");
    }
    ResourceRequest anyRequest = createResourceRequest(1024,
        ResourceRequest.ANY, 1, 1, false);
    reqs.add(anyRequest);
    reqs.add(rackRequest);
    if (node) {
      reqs.add(nodeRequest);
    }

    ApplicationAttemptId attId1 =
        createSchedulingRequest("queue1", "user1", reqs);

    scheduler.update();

    NodeUpdateSchedulerEvent node2UpdateEvent =
        new NodeUpdateSchedulerEvent(node2);

    FSAppAttempt app = scheduler.getSchedulerApp(attId1);

    // node2 should get the container
    scheduler.handle(node2UpdateEvent);
    if (invalid) {
      assertEquals(0, app.getLiveContainers().size());
      assertEquals(0, scheduler.getNode(node2.getNodeID()).getNumContainers());
      assertEquals(0, scheduler.getNode(node1.getNodeID()).getNumContainers());
    } else {
      assertEquals(1, app.getLiveContainers().size());
      assertEquals(1, scheduler.getNode(node2.getNodeID()).getNumContainers());
      assertEquals(0, scheduler.getNode(node1.getNodeID()).getNumContainers());
    }
  }

  /**
   * Strict locality requests shouldn't reserve resources on another node.
   */
  @Test
  public void testReservationsStrictLocality() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add two nodes
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024, 1));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024, 1));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    // Submit application without container requests
    ApplicationAttemptId attId =
        createSchedulingRequest(1024, "queue1", "user1", 0);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

    // Request a container on node2
    ResourceRequest nodeRequest =
        createResourceRequest(1024, node2.getHostName(), 1, 1, true);
    ResourceRequest rackRequest =
        createResourceRequest(1024, "rack1", 1, 1, false);
    ResourceRequest anyRequest =
        createResourceRequest(1024, ResourceRequest.ANY, 1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId);
    createSchedulingRequestExistingApplication(rackRequest, attId);
    createSchedulingRequestExistingApplication(anyRequest, attId);
    scheduler.update();

    // Heartbeat from node1. App shouldn't get an allocation or reservation
    NodeUpdateSchedulerEvent nodeUpdateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeUpdateEvent);
    assertEquals("App assigned a container on the wrong node",
        0, app.getLiveContainers().size());
    scheduler.handle(nodeUpdateEvent);
    assertEquals("App reserved a container on the wrong node",
        0, app.getReservedContainers().size());
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
    drfPolicy.initialize(scheduler.getContext());
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
    drfPolicy.initialize(scheduler.getContext());
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
    drfPolicy.initialize(scheduler.getContext());
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
  public void testMultipleCompletedEvent() throws Exception {
    // Set up a fair scheduler
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

    // Create a node
    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(20480, 20),
            0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    // Launch an app
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(
        attId1, "queue1", "user1",
        Resource.newInstance(1024, 1));
    createSchedulingRequestExistingApplication(
        1024, 1,
        RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority(), attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);

    RMContainer container = app1.getLiveContainersMap().
        values().iterator().next();
    scheduler.completedContainer(container, SchedulerUtils
        .createAbnormalContainerStatus(container.getContainerId(),
            SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
    scheduler.completedContainer(container, SchedulerUtils
        .createAbnormalContainerStatus(container.getContainerId(),
            SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.FINISHED);
    assertEquals(Resources.none(), app1.getResourceUsage());
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
        .getFairShare().getMemorySize());

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
        1024, app1.getAMResource().getMemorySize());
    assertEquals("Application1's AM should be running",
        1, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemorySize());

    // Exceeds no limits
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application2's AM requests 1024 MB memory",
        1024, app2.getAMResource().getMemorySize());
    assertEquals("Application2's AM should be running",
        1, app2.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Exceeds queue limit
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application3's AM resource shouldn't be updated",
        0, app3.getAMResource().getMemorySize());
    assertEquals("Application3's AM should not be running",
        0, app3.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Still can run non-AM container
    createSchedulingRequestExistingApplication(1024, 1, attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application1 should have two running containers",
        2, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Remove app1, app3's AM should become running
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1, RMAppAttemptState.FINISHED, false);
    scheduler.update();
    scheduler.handle(appRemovedEvent1);
    scheduler.handle(updateEvent);
    assertEquals("Application1's AM should be finished",
        0, app1.getLiveContainers().size());
    assertEquals("Finished application usage should be none",
        Resources.none(), app1.getResourceUsage());
    assertEquals("Application3's AM should be running",
        1, app3.getLiveContainers().size());
    assertEquals("Application3's AM requests 1024 MB memory",
        1024, app3.getAMResource().getMemorySize());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Exceeds queue limit
    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(2048, 2, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application4's AM resource shouldn't be updated",
        0, app4.getAMResource().getMemorySize());
    assertEquals("Application4's AM should not be running",
        0, app4.getLiveContainers().size());
    assertEquals("Finished application usage should be none",
        Resources.none(), app4.getResourceUsage());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Exceeds queue limit
    ApplicationAttemptId attId5 = createAppAttemptId(5, 1);
    createApplicationWithAMResource(attId5, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(2048, 2, amPriority, attId5);
    FSAppAttempt app5 = scheduler.getSchedulerApp(attId5);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application5's AM resource shouldn't be updated",
        0, app5.getAMResource().getMemorySize());
    assertEquals("Application5's AM should not be running",
        0, app5.getLiveContainers().size());
    assertEquals("Finished application usage should be none",
        Resources.none(), app5.getResourceUsage());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Remove un-running app doesn't affect others
    AppAttemptRemovedSchedulerEvent appRemovedEvent4 =
        new AppAttemptRemovedSchedulerEvent(attId4, RMAppAttemptState.KILLED, false);
    scheduler.handle(appRemovedEvent4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application5's AM should not be running",
        0, app5.getLiveContainers().size());
    assertEquals("Finished application usage should be none",
        Resources.none(), app5.getResourceUsage());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

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
    assertEquals("Finished application usage should be none",
        Resources.none(), app2.getResourceUsage());
    assertEquals("Application3's AM should be finished",
        0, app3.getLiveContainers().size());
    assertEquals("Finished application usage should be none",
        Resources.none(), app3.getResourceUsage());
    assertEquals("Application5's AM should be running",
        1, app5.getLiveContainers().size());
    assertEquals("Application5's AM requests 2048 MB memory",
        2048, app5.getAMResource().getMemorySize());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // request non-AM container for app5
    createSchedulingRequestExistingApplication(1024, 1, attId5);
    assertEquals("Application5's AM should have 1 container",
        1, app5.getLiveContainers().size());
    // complete AM container before non-AM container is allocated.
    // spark application hit this situation.
    RMContainer amContainer5 = (RMContainer)app5.getLiveContainers().toArray()[0];
    ContainerExpiredSchedulerEvent containerExpired =
        new ContainerExpiredSchedulerEvent(amContainer5.getContainerId());
    scheduler.handle(containerExpired);
    assertEquals("Application5's AM should have 0 container",
        0, app5.getLiveContainers().size());
    assertEquals("Finished application usage should be none",
        Resources.none(), app5.getResourceUsage());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());
    scheduler.update();
    scheduler.handle(updateEvent);
    // non-AM container should be allocated
    // check non-AM container allocation is not rejected
    // due to queue MaxAMShare limitation.
    assertEquals("Application5 should have 1 container",
        1, app5.getLiveContainers().size());
    // check non-AM container allocation won't affect queue AmResourceUsage
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Check amResource normalization
    ApplicationAttemptId attId6 = createAppAttemptId(6, 1);
    createApplicationWithAMResource(attId6, "queue1", "user1", amResource3);
    createSchedulingRequestExistingApplication(1860, 2, amPriority, attId6);
    FSAppAttempt app6 = scheduler.getSchedulerApp(attId6);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application6's AM should not be running",
        0, app6.getLiveContainers().size());
    assertEquals("Finished application usage should be none",
        Resources.none(), app6.getResourceUsage());
    assertEquals("Application6's AM resource shouldn't be updated",
        0, app6.getAMResource().getMemorySize());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    // Remove all apps
    AppAttemptRemovedSchedulerEvent appRemovedEvent5 =
        new AppAttemptRemovedSchedulerEvent(attId5, RMAppAttemptState.FINISHED, false);
    AppAttemptRemovedSchedulerEvent appRemovedEvent6 =
        new AppAttemptRemovedSchedulerEvent(attId6, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent5);
    scheduler.handle(appRemovedEvent6);
    scheduler.update();
    assertEquals("Queue1's AM resource usage should be 0",
        0, queue1.getAmResourceUsage().getMemorySize());
  }

  @Test
  public void testQueueMaxAMShareDefault() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 6);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("</queue>");
    out.println("<queue name=\"queue2\">");
    out.println("<maxAMShare>0.4</maxAMShare>");
    out.println("</queue>");
    out.println("<queue name=\"queue3\">");
    out.println("<maxResources>10240 mb 4 vcores</maxResources>");
    out.println("</queue>");
    out.println("<queue name=\"queue4\">");
    out.println("</queue>");
    out.println("<queue name=\"queue5\">");
    out.println("</queue>");
    out.println(
        "<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(8192, 10),
            0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    FSLeafQueue queue1 =
        scheduler.getQueueManager().getLeafQueue("queue1", true);
    assertEquals("Queue queue1's fair share should be 0", 0, queue1
        .getFairShare().getMemorySize());
    FSLeafQueue queue2 =
        scheduler.getQueueManager().getLeafQueue("queue2", true);
    assertEquals("Queue queue2's fair share should be 0", 0, queue2
        .getFairShare().getMemorySize());
    FSLeafQueue queue3 =
        scheduler.getQueueManager().getLeafQueue("queue3", true);
    assertEquals("Queue queue3's fair share should be 0", 0, queue3
        .getFairShare().getMemorySize());
    FSLeafQueue queue4 =
        scheduler.getQueueManager().getLeafQueue("queue4", true);
    assertEquals("Queue queue4's fair share should be 0", 0, queue4
        .getFairShare().getMemorySize());
    FSLeafQueue queue5 =
        scheduler.getQueueManager().getLeafQueue("queue5", true);
    assertEquals("Queue queue5's fair share should be 0", 0, queue5
        .getFairShare().getMemorySize());

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
        1024, app1.getAMResource().getMemorySize());
    assertEquals("Application1's AM should be running",
        1, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemorySize());

    // Now the fair share is 1639 MB, and the maxAMShare is 0.4f,
    // so the AM is not accepted.
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue2", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application2's AM resource shouldn't be updated",
        0, app2.getAMResource().getMemorySize());
    assertEquals("Application2's AM should not be running",
        0, app2.getLiveContainers().size());
    assertEquals("Queue2's AM resource usage should be 0 MB memory",
        0, queue2.getAmResourceUsage().getMemorySize());

    // Remove the app2
    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId2,
                RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent2);
    scheduler.update();

    // AM3 can pass the fair share checking, but it takes all available VCore,
    // So the AM3 is not accepted.
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue3", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 6, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application3's AM resource shouldn't be updated",
            0, app3.getAMResource().getMemorySize());
    assertEquals("Application3's AM should not be running",
            0, app3.getLiveContainers().size());
    assertEquals("Queue3's AM resource usage should be 0 MB memory",
            0, queue3.getAmResourceUsage().getMemorySize());

    // AM4 can pass the fair share checking and it doesn't takes all
    // available VCore, but it need 5 VCores which are more than
    // maxResources(4 VCores). So the AM4 is not accepted.
    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue3", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 5, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Application4's AM resource shouldn't be updated",
            0, app4.getAMResource().getMemorySize());
    assertEquals("Application4's AM should not be running",
            0, app4.getLiveContainers().size());
    assertEquals("Queue3's AM resource usage should be 0 MB memory",
            0, queue3.getAmResourceUsage().getMemorySize());
  }

  /**
   * The test verifies container gets reserved when not over maxAMShare,
   * reserved container gets unreserved when over maxAMShare,
   * container doesn't get reserved when over maxAMShare,
   * reserved container is turned into an allocation and
   * superfluously reserved container gets unreserved.
   * 1. create three nodes: Node1 is 10G, Node2 is 10G and Node3 is 5G.
   * 2. APP1 allocated 1G on Node1 and APP2 allocated 1G on Node2.
   * 3. APP3 reserved 10G on Node1 and Node2.
   * 4. APP4 allocated 5G on Node3, which makes APP3 over maxAMShare.
   * 5. Remove APP1 to make Node1 have 10G available resource.
   * 6. APP3 unreserved its container on Node1 because it is over maxAMShare.
   * 7. APP5 allocated 1G on Node1 after APP3 unreserved its container.
   * 8. Remove APP3.
   * 9. APP6 failed to reserve a 10G container on Node1 due to AMShare limit.
   * 10. APP7 allocated 1G on Node1.
   * 11. Remove APP4 and APP5.
   * 12. APP6 reserved 10G on Node1 and Node2.
   * 13. APP8 failed to allocate a 1G container on Node1 and Node2 because
   *     APP6 reserved Node1 and Node2.
   * 14. Remove APP2.
   * 15. APP6 turned the 10G reservation into an allocation on node2.
   * 16. APP6 unreserved its container on node1, APP8 allocated 1G on Node1.
   */
  @Test
  public void testQueueMaxAMShareWithContainerReservation() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 1f);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("<maxAMShare>0.5</maxAMShare>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(10240, 10),
            1, "127.0.0.1");
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(10240, 10),
            2, "127.0.0.2");
    RMNode node3 =
        MockNodes.newNodeInfo(1, Resources.createResource(5120, 5),
            3, "127.0.0.3");
    NodeAddedSchedulerEvent nodeE1 = new NodeAddedSchedulerEvent(node1);
    NodeUpdateSchedulerEvent updateE1 = new NodeUpdateSchedulerEvent(node1);
    NodeAddedSchedulerEvent nodeE2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateE2 = new NodeUpdateSchedulerEvent(node2);
    NodeAddedSchedulerEvent nodeE3 = new NodeAddedSchedulerEvent(node3);
    NodeUpdateSchedulerEvent updateE3 = new NodeUpdateSchedulerEvent(node3);
    scheduler.handle(nodeE1);
    scheduler.handle(nodeE2);
    scheduler.handle(nodeE3);
    scheduler.update();
    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1",
        true);
    Resource amResource1 = Resource.newInstance(1024, 1);
    Resource amResource2 = Resource.newInstance(1024, 1);
    Resource amResource3 = Resource.newInstance(10240, 1);
    Resource amResource4 = Resource.newInstance(5120, 1);
    Resource amResource5 = Resource.newInstance(1024, 1);
    Resource amResource6 = Resource.newInstance(10240, 1);
    Resource amResource7 = Resource.newInstance(1024, 1);
    Resource amResource8 = Resource.newInstance(1024, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    // Allocate app1's AM container on node1.
    scheduler.handle(updateE1);
    assertEquals("Application1's AM requests 1024 MB memory",
        1024, app1.getAMResource().getMemorySize());
    assertEquals("Application1's AM should be running",
        1, app1.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemorySize());

    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    // Allocate app2's AM container on node2.
    scheduler.handle(updateE2);
    assertEquals("Application2's AM requests 1024 MB memory",
        1024, app2.getAMResource().getMemorySize());
    assertEquals("Application2's AM should be running",
        1, app2.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue1", "user1", amResource3);
    createSchedulingRequestExistingApplication(10240, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    // app3 reserves a container on node1 because node1's available resource
    // is less than app3's AM container resource.
    scheduler.handle(updateE1);
    // Similarly app3 reserves a container on node2.
    scheduler.handle(updateE2);
    assertEquals("Application3's AM resource shouldn't be updated",
        0, app3.getAMResource().getMemorySize());
    assertEquals("Application3's AM should not be running",
        0, app3.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue1", "user1", amResource4);
    createSchedulingRequestExistingApplication(5120, 1, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    // app4 can't allocate its AM container on node1 because
    // app3 already reserved its container on node1.
    scheduler.handle(updateE1);
    assertEquals("Application4's AM resource shouldn't be updated",
        0, app4.getAMResource().getMemorySize());
    assertEquals("Application4's AM should not be running",
        0, app4.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    scheduler.update();
    // Allocate app4's AM container on node3.
    scheduler.handle(updateE3);
    assertEquals("Application4's AM requests 5120 MB memory",
        5120, app4.getAMResource().getMemorySize());
    assertEquals("Application4's AM should be running",
        1, app4.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemorySize());

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1,
            RMAppAttemptState.FINISHED, false);
    // Release app1's AM container on node1.
    scheduler.handle(appRemovedEvent1);
    assertEquals("Queue1's AM resource usage should be 6144 MB memory",
        6144, queue1.getAmResourceUsage().getMemorySize());

    ApplicationAttemptId attId5 = createAppAttemptId(5, 1);
    createApplicationWithAMResource(attId5, "queue1", "user1", amResource5);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId5);
    FSAppAttempt app5 = scheduler.getSchedulerApp(attId5);
    scheduler.update();
    // app5 can allocate its AM container on node1 after
    // app3 unreserve its container on node1 due to
    // exceeding queue MaxAMShare limit.
    scheduler.handle(updateE1);
    assertEquals("Application5's AM requests 1024 MB memory",
        1024, app5.getAMResource().getMemorySize());
    assertEquals("Application5's AM should be running",
        1, app5.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemorySize());

    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(attId3,
            RMAppAttemptState.FINISHED, false);
    // Remove app3.
    scheduler.handle(appRemovedEvent3);
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemorySize());

    ApplicationAttemptId attId6 = createAppAttemptId(6, 1);
    createApplicationWithAMResource(attId6, "queue1", "user1", amResource6);
    createSchedulingRequestExistingApplication(10240, 1, amPriority, attId6);
    FSAppAttempt app6 = scheduler.getSchedulerApp(attId6);
    scheduler.update();
    // app6 can't reserve a container on node1 because
    // it exceeds queue MaxAMShare limit.
    scheduler.handle(updateE1);
    assertEquals("Application6's AM resource shouldn't be updated",
        0, app6.getAMResource().getMemorySize());
    assertEquals("Application6's AM should not be running",
        0, app6.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 7168 MB memory",
        7168, queue1.getAmResourceUsage().getMemorySize());

    ApplicationAttemptId attId7 = createAppAttemptId(7, 1);
    createApplicationWithAMResource(attId7, "queue1", "user1", amResource7);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId7);
    FSAppAttempt app7 = scheduler.getSchedulerApp(attId7);
    scheduler.update();
    // Allocate app7's AM container on node1 to prove
    // app6 didn't reserve a container on node1.
    scheduler.handle(updateE1);
    assertEquals("Application7's AM requests 1024 MB memory",
        1024, app7.getAMResource().getMemorySize());
    assertEquals("Application7's AM should be running",
        1, app7.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 8192 MB memory",
        8192, queue1.getAmResourceUsage().getMemorySize());

    AppAttemptRemovedSchedulerEvent appRemovedEvent4 =
        new AppAttemptRemovedSchedulerEvent(attId4,
            RMAppAttemptState.FINISHED, false);
    // Release app4's AM container on node3.
    scheduler.handle(appRemovedEvent4);
    assertEquals("Queue1's AM resource usage should be 3072 MB memory",
        3072, queue1.getAmResourceUsage().getMemorySize());

    AppAttemptRemovedSchedulerEvent appRemovedEvent5 =
        new AppAttemptRemovedSchedulerEvent(attId5,
            RMAppAttemptState.FINISHED, false);
    // Release app5's AM container on node1.
    scheduler.handle(appRemovedEvent5);
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
              2048, queue1.getAmResourceUsage().getMemorySize());

    scheduler.update();
    // app6 reserves a container on node1 because node1's available resource
    // is less than app6's AM container resource and
    // app6 is not over AMShare limit.
    scheduler.handle(updateE1);
    // Similarly app6 reserves a container on node2.
    scheduler.handle(updateE2);

    ApplicationAttemptId attId8 = createAppAttemptId(8, 1);
    createApplicationWithAMResource(attId8, "queue1", "user1", amResource8);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId8);
    FSAppAttempt app8 = scheduler.getSchedulerApp(attId8);
    scheduler.update();
    // app8 can't allocate a container on node1 because
    // app6 already reserved a container on node1.
    scheduler.handle(updateE1);
    assertEquals("Application8's AM resource shouldn't be updated",
        0, app8.getAMResource().getMemorySize());
    assertEquals("Application8's AM should not be running",
        0, app8.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());
    scheduler.update();
    // app8 can't allocate a container on node2 because
    // app6 already reserved a container on node2.
    scheduler.handle(updateE2);
    assertEquals("Application8's AM resource shouldn't be updated",
        0, app8.getAMResource().getMemorySize());
    assertEquals("Application8's AM should not be running",
        0, app8.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 2048 MB memory",
        2048, queue1.getAmResourceUsage().getMemorySize());

    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId2,
            RMAppAttemptState.FINISHED, false);
    // Release app2's AM container on node2.
    scheduler.handle(appRemovedEvent2);
    assertEquals("Queue1's AM resource usage should be 1024 MB memory",
        1024, queue1.getAmResourceUsage().getMemorySize());

    scheduler.update();
    // app6 turns the reservation into an allocation on node2.
    scheduler.handle(updateE2);
    assertEquals("Application6's AM requests 10240 MB memory",
        10240, app6.getAMResource().getMemorySize());
    assertEquals("Application6's AM should be running",
        1, app6.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 11264 MB memory",
        11264, queue1.getAmResourceUsage().getMemorySize());

    scheduler.update();
    // app6 unreserve its container on node1 because
    // it already got a container on node2.
    // Now app8 can allocate its AM container on node1.
    scheduler.handle(updateE1);
    assertEquals("Application8's AM requests 1024 MB memory",
        1024, app8.getAMResource().getMemorySize());
    assertEquals("Application8's AM should be running",
        1, app8.getLiveContainers().size());
    assertEquals("Queue1's AM resource usage should be 12288 MB memory",
        12288, queue1.getAmResourceUsage().getMemorySize());
  }

  @Test
  public void testMaxRunningAppsHierarchicalQueues() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
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
    clock.tickSec(10);
    // exceeds no limits
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId2, true);
    verifyQueueNumRunnable("queue1.sub3", 1, 0);
    clock.tickSec(10);
    // exceeds no limits
    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId3, true);
    verifyQueueNumRunnable("queue1.sub2", 1, 0);
    clock.tickSec(10);
    // exceeds queue1 limit
    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId4, false);
    verifyQueueNumRunnable("queue1.sub2", 1, 1);
    clock.tickSec(10);
    // exceeds sub3 limit
    ApplicationAttemptId attId5 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1.sub3", 1, 1);
    clock.tickSec(10);

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

  @SuppressWarnings("deprecation")
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
    scheduler.allocate(id11, ask1, null, new ArrayList<ContainerId>(), null,
        null, NULL_UPDATE_REQUESTS);

    String hostName = "127.0.0.1";
    RMNode node1 = MockNodes.newNodeInfo(1,
      Resources.createResource(8 * 1024, 8), 1, hostName);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    FSSchedulerNode node = scheduler.getSchedulerNode(node1.getNodeID());

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

    List<QueuePlacementRule> rules =
        scheduler.allocConf.placementPolicy.getRules();

    for (QueuePlacementRule rule : rules) {
      if (rule instanceof Default) {
        Default defaultRule = (Default) rule;
        assertNotNull(defaultRule.defaultQueueName);
      }
    }
  }
  
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
        null, Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null, NULL_UPDATE_REQUESTS);
    assertTrue(app.isPlaceBlacklisted(host));
    scheduler.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        null, Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host), NULL_UPDATE_REQUESTS);
    assertFalse(scheduler.getSchedulerApp(appAttemptId)
        .isPlaceBlacklisted(host));

    List<ResourceRequest> update = Arrays.asList(
        createResourceRequest(GB, node.getHostName(), 1, 0, true));

    // Verify a container does not actually get placed on the blacklisted host
    scheduler.allocate(appAttemptId, update, null, Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null, NULL_UPDATE_REQUESTS);
    assertTrue(app.isPlaceBlacklisted(host));
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals("Incorrect number of containers allocated", 0, app
        .getLiveContainers().size());

    // Verify a container gets placed on the empty blacklist
    scheduler.allocate(appAttemptId, update, null, Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host), NULL_UPDATE_REQUESTS);
    assertFalse(app.isPlaceBlacklisted(host));
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
  public void testResourceUsageByMoveApp() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(1 * GB, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId appAttId =
        createSchedulingRequest(1 * GB, 2, "parent1.queue1", "user1", 2);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    QueueManager queueMgr = scheduler.getQueueManager();
    FSQueue parent1 = queueMgr.getParentQueue("parent1", true);
    FSQueue parent2 = queueMgr.getParentQueue("parent2", true);
    FSQueue queue2 = queueMgr.getLeafQueue("parent2.queue2", true);
    FSQueue queue1 = queueMgr.getLeafQueue("parent1.queue1", true);

    Assert.assertEquals(parent2.getResourceUsage().getMemorySize(), 0);
    Assert.assertEquals(queue2.getResourceUsage().getMemorySize(), 0);
    Assert.assertEquals(parent1.getResourceUsage().getMemorySize(), 1 * GB);
    Assert.assertEquals(queue1.getResourceUsage().getMemorySize(), 1 * GB);

    scheduler.moveApplication(appAttId.getApplicationId(), "parent2.queue2");

    Assert.assertEquals(parent2.getResourceUsage().getMemorySize(), 1 * GB);
    Assert.assertEquals(queue2.getResourceUsage().getMemorySize(), 1 * GB);
    Assert.assertEquals(parent1.getResourceUsage().getMemorySize(), 0);
    Assert.assertEquals(queue1.getResourceUsage().getMemorySize(), 0);
  }
    
  @Test (expected = YarnException.class)
  public void testMoveWouldViolateMaxAppsConstraints() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    QueueManager queueMgr = scheduler.getQueueManager();
    FSQueue queue2 = queueMgr.getLeafQueue("queue2", true);
    queue2.setMaxRunningApps(0);

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
    FSQueue queue2 = queueMgr.getLeafQueue("queue2", true);
    queue2.setMaxShare(
        new ConfigurableResource(Resource.newInstance(1024, 1)));

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
  public void testDoubleRemoval() throws Exception {
    String testUser = "user1"; // convenience var
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId attemptId = createAppAttemptId(1, 1);
    // The placement rule will add the app to the user based queue but the
    // passed in queue must exist.
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(attemptId.getApplicationId(), testUser,
            testUser);
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attemptAddedEvent =
        new AppAttemptAddedSchedulerEvent(createAppAttemptId(1, 1), false);
    scheduler.handle(attemptAddedEvent);

    // Get a handle on the attempt.
    FSAppAttempt attempt = scheduler.getSchedulerApp(attemptId);

    AppAttemptRemovedSchedulerEvent attemptRemovedEvent =
        new AppAttemptRemovedSchedulerEvent(createAppAttemptId(1, 1),
            RMAppAttemptState.FINISHED, false);

    // Make sure the app attempt is in the queue.
    List<ApplicationAttemptId> attemptList =
        scheduler.getAppsInQueue(testUser);
    assertNotNull("Queue missing", attemptList);
    assertTrue("Attempt should be in the queue",
        attemptList.contains(attemptId));
    assertFalse("Attempt is stopped", attempt.isStopped());

    // Now remove the app attempt
    scheduler.handle(attemptRemovedEvent);
    // The attempt is not in the queue, and stopped
    attemptList = scheduler.getAppsInQueue(testUser);
    assertFalse("Attempt should not be in the queue",
        attemptList.contains(attemptId));
    assertTrue("Attempt should have been stopped", attempt.isStopped());

    // Now remove the app attempt again, since it is stopped nothing happens.
    scheduler.handle(attemptRemovedEvent);
    // The attempt should still show the original queue info.
    assertTrue("Attempt queue has changed",
        attempt.getQueue().getName().endsWith(testUser));
  }

  @Test (expected = YarnException.class)
  public void testMoveAfterRemoval() throws Exception {
    String testUser = "user1"; // convenience var
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId attemptId = createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(attemptId.getApplicationId(), testUser,
            testUser);
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attemptAddedEvent =
        new AppAttemptAddedSchedulerEvent(createAppAttemptId(1, 1), false);
    scheduler.handle(attemptAddedEvent);

    // Get a handle on the attempt.
    FSAppAttempt attempt = scheduler.getSchedulerApp(attemptId);

    AppAttemptRemovedSchedulerEvent attemptRemovedEvent =
        new AppAttemptRemovedSchedulerEvent(createAppAttemptId(1, 1),
            RMAppAttemptState.FINISHED, false);

    // Remove the app attempt
    scheduler.handle(attemptRemovedEvent);
    // Make sure the app attempt is not in the queue and stopped.
    List<ApplicationAttemptId> attemptList =
        scheduler.getAppsInQueue(testUser);
    assertNotNull("Queue missing", attemptList);
    assertFalse("Attempt should not be in the queue",
        attemptList.contains(attemptId));
    assertTrue("Attempt should have been stopped", attempt.isStopped());
    // The attempt should still show the original queue info.
    assertTrue("Attempt queue has changed",
        attempt.getQueue().getName().endsWith(testUser));

    // Now move the app: not using an event since there is none
    // in the scheduler. This should throw.
    scheduler.moveApplication(attemptId.getApplicationId(), "default");
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

  @Test
  public void testQueueNameWithTrailingSpace() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // only default queue
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());

    // submit app with queue name "A"
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent1 = new AppAddedSchedulerEvent(
        appAttemptId1.getApplicationId(), "A", "user1");
    scheduler.handle(appAddedEvent1);
    // submission accepted
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());
    assertNotNull(scheduler.getSchedulerApplications().get(appAttemptId1.
        getApplicationId()));

    AppAttemptAddedSchedulerEvent attempAddedEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    scheduler.handle(attempAddedEvent);
    // That queue should have one app
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("A", true)
        .getNumRunnableApps());
    assertNotNull(scheduler.getSchedulerApp(appAttemptId1));

    // submit app with queue name "A "
    ApplicationAttemptId appAttemptId2 = createAppAttemptId(2, 1);
    AppAddedSchedulerEvent appAddedEvent2 = new AppAddedSchedulerEvent(
        appAttemptId2.getApplicationId(), "A ", "user1");
    scheduler.handle(appAddedEvent2);
    // submission rejected
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApplications().get(appAttemptId2.
        getApplicationId()));
    assertNull(scheduler.getSchedulerApp(appAttemptId2));

    // submit app with queue name "B.C"
    ApplicationAttemptId appAttemptId3 = createAppAttemptId(3, 1);
    AppAddedSchedulerEvent appAddedEvent3 = new AppAddedSchedulerEvent(
        appAttemptId3.getApplicationId(), "B.C", "user1");
    scheduler.handle(appAddedEvent3);
    // submission accepted
    assertEquals(3, scheduler.getQueueManager().getLeafQueues().size());
    assertNotNull(scheduler.getSchedulerApplications().get(appAttemptId3.
        getApplicationId()));

    attempAddedEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId3, false);
    scheduler.handle(attempAddedEvent);
    // That queue should have one app
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("B.C", true)
        .getNumRunnableApps());
    assertNotNull(scheduler.getSchedulerApp(appAttemptId3));

    // submit app with queue name "A\u00a0" (non-breaking space)
    ApplicationAttemptId appAttemptId4 = createAppAttemptId(4, 1);
    AppAddedSchedulerEvent appAddedEvent4 = new AppAddedSchedulerEvent(
        appAttemptId4.getApplicationId(), "A\u00a0", "user1");
    scheduler.handle(appAddedEvent4);
    // submission rejected
    assertEquals(3, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApplications().get(appAttemptId4.
        getApplicationId()));
    assertNull(scheduler.getSchedulerApp(appAttemptId4));
  }

  @Test
  public void testEmptyQueueNameInConfigFile() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set empty queue name
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"\">");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    try {
      scheduler.init(conf);
      Assert.fail("scheduler init should fail because" +
          " empty queue name.");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(
          "Failed to initialize FairScheduler"));
    }
  }

  @Test
  public void testUserAsDefaultQueueWithLeadingTrailingSpaceUserName()
      throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "  user1", null);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(appAttemptId.getApplicationId()).getQueue());

    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "default", "user1  ", null);
    assertEquals(2, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(attId2.getApplicationId()).getQueue());

    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "default", "user1", null);
    assertEquals(3, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(attId3.getApplicationId()).getQueue());
  }

  @Test
  public void testRemovedNodeDecomissioningNode() throws Exception {
    // Register nodemanager
    NodeManager nm = registerNode("host_decom", 1234, 2345,
        NetworkTopology.DEFAULT_RACK, Resources.createResource(8 * GB, 4));

    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);

    // Force remove the node to simulate race condition
    ((FairScheduler) resourceManager.getResourceScheduler())
        .getNodeTracker().removeNode(nm.getNodeId());
    // Kick off another heartbeat with the node state mocked to decommissioning
    RMNode spyNode =
        Mockito.spy(resourceManager.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));
  }

  @Test
  public void testResourceUpdateDecommissioningNode() throws Exception {
    // Mock the RMNodeResourceUpdate event handler to update SchedulerNode
    // to have 0 available resource
    RMContext spyContext = Mockito.spy(resourceManager.getRMContext());
    Dispatcher mockDispatcher = mock(AsyncDispatcher.class);
    when(mockDispatcher.getEventHandler()).thenReturn(new EventHandler() {
      @Override
      public void handle(Event event) {
        if (event instanceof RMNodeResourceUpdateEvent) {
          RMNodeResourceUpdateEvent resourceEvent =
              (RMNodeResourceUpdateEvent) event;
          resourceManager
              .getResourceScheduler()
              .getSchedulerNode(resourceEvent.getNodeId())
              .updateTotalResource(resourceEvent.getResourceOption().getResource());
        }
      }
    });
    Mockito.doReturn(mockDispatcher).when(spyContext).getDispatcher();
    ((FairScheduler) resourceManager.getResourceScheduler())
        .setRMContext(spyContext);
    ((AsyncDispatcher) mockDispatcher).start();
    // Register node
    String host_0 = "host_0";
    NodeManager nm_0 = registerNode(host_0, 1234, 2345,
        NetworkTopology.DEFAULT_RACK, Resources.createResource(8 * GB, 4));

    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm_0.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);

    // Kick off another heartbeat with the node state mocked to decommissioning
    // This should update the schedulernodes to have 0 available resource
    RMNode spyNode =
        Mockito.spy(resourceManager.getRMContext().getRMNodes()
            .get(nm_0.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));

    // Check the used resource is 0 GB 0 core
    // Assert.assertEquals(1 * GB, nm_0.getUsed().getMemory());
    Resource usedResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getAllocatedResource();
    Assert.assertEquals(usedResource.getMemorySize(), 0);
    Assert.assertEquals(usedResource.getVirtualCores(), 0);
    // Check total resource of scheduler node is also changed to 0 GB 0 core
    Resource totalResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getTotalResource();
    Assert.assertEquals(totalResource.getMemorySize(), 0 * GB);
    Assert.assertEquals(totalResource.getVirtualCores(), 0);
    // Check the available resource is 0/0
    Resource availableResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getUnallocatedResource();
    Assert.assertEquals(availableResource.getMemorySize(), 0);
    Assert.assertEquals(availableResource.getVirtualCores(), 0);
  }

  private NodeManager registerNode(String hostName, int containerManagerPort,
                                   int httpPort, String rackName,
                                   Resource capability)
      throws IOException, YarnException {
    NodeManager nm = new NodeManager(hostName, containerManagerPort, httpPort,
        rackName, capability, resourceManager);

    // after YARN-5375, scheduler event is processed in rm main dispatcher,
    // wait it processed, or may lead dead lock
    if (resourceManager instanceof MockRM) {
      ((MockRM) resourceManager).drainEvents();
    }

    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(resourceManager.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  @Test(timeout = 120000)
  public void testContainerAllocationWithContainerIdLeap() throws Exception {
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0.50f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add two node
    RMNode node1 = MockNodes.newNodeInfo(1,
        Resources.createResource(3072, 10), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1,
        Resources.createResource(3072, 10), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId app1 =
        createSchedulingRequest(2048, "queue1", "user1", 2);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));

    ApplicationAttemptId app2 =
        createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));

    assertEquals(4096, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    //container will be reserved at node1
    RMContainer reservedContainer1 =
        scheduler.getSchedulerNode(node1.getNodeID()).getReservedContainer();
    assertNotEquals(reservedContainer1, null);
    RMContainer reservedContainer2 =
        scheduler.getSchedulerNode(node2.getNodeID()).getReservedContainer();
    assertEquals(reservedContainer2, null);

    for (int i = 0; i < 10; i++) {
      scheduler.handle(new NodeUpdateSchedulerEvent(node1));
      scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    }

    // release resource
    scheduler.handle(new AppAttemptRemovedSchedulerEvent(
        app1, RMAppAttemptState.KILLED, false));

    assertEquals(0, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    // container will be allocated at node2
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    assertEquals(scheduler.getSchedulerApp(app2).
        getLiveContainers().size(), 1);

    long maxId = 0;
    for (RMContainer container :
        scheduler.getSchedulerApp(app2).getLiveContainers()) {
      assertTrue(
          container.getContainer().getNodeId().equals(node2.getNodeID()));
      if (container.getContainerId().getContainerId() > maxId) {
        maxId = container.getContainerId().getContainerId();
      }
    }

    long reservedId = reservedContainer1.getContainerId().getContainerId();
    assertEquals(reservedId + 1, maxId);
  }

  @Test(timeout = 120000)
  public void testRefreshQueuesWhenRMHA() throws Exception {
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);
    conf.setBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, false);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    HAServiceProtocol.StateChangeRequestInfo requestInfo =
        new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    // 1. start a standby RM, file 'ALLOC_FILE' is empty, so there is no queues
    MockRM rm1 = new MockRM(conf, null);
    rm1.init(conf);
    rm1.start();
    rm1.getAdminService().transitionToStandby(requestInfo);

    // 2. add a new queue "test_queue"
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"test_queue\">");
    out.println("  <maxRunningApps>3</maxRunningApps>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    // 3. start a active RM
    MockRM rm2 = new MockRM(conf);
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm2.getRMStateStore();
    rm2.start();

    MockNM nm =
        new MockNM("127.0.0.1:1234", 15120, rm2.getResourceTrackerService());
    nm.registerNode();

    rm2.getAdminService().transitionToActive(requestInfo);

    // 4. submit a app to the new added queue "test_queue"
    RMApp app = rm2.submitApp(200, "test_app", "user", null, "test_queue");
    RMAppAttempt attempt0 = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am0 = rm2.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();
    assertEquals("root.test_queue", app.getQueue());

    // 5. Transit rm1 to active, recover app
    ((RMContextImpl)rm1.getRMContext()).setStateStore(memStore);
    rm1.getAdminService().transitionToActive(requestInfo);
    rm1.drainEvents();
    assertEquals(1, rm1.getRMContext().getRMApps().size());
    RMApp recoveredApp =
        rm1.getRMContext().getRMApps().values().iterator().next();
    assertEquals("root.test_queue", recoveredApp.getQueue());

    rm1.stop();
    rm2.stop();
  }

  @Test
  public void testReservationMetrics() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    QueueMetrics metrics = scheduler.getRootQueueMetrics();

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(4096, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user1", null);

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.update();
    scheduler.handle(updateEvent);

    createSchedulingRequestExistingApplication(1024, 1, 1, appAttemptId);
    scheduler.update();
    scheduler.handle(updateEvent);

    // no reservation yet
    assertEquals(0, metrics.getReservedContainers());
    assertEquals(0, metrics.getReservedMB());
    assertEquals(0, metrics.getReservedVirtualCores());

    // create reservation of {4096, 4}
    createSchedulingRequestExistingApplication(4096, 4, 1, appAttemptId);
    scheduler.update();
    scheduler.handle(updateEvent);

    // reservation created
    assertEquals(1, metrics.getReservedContainers());
    assertEquals(4096, metrics.getReservedMB());
    assertEquals(4, metrics.getReservedVirtualCores());

    // remove AppAttempt
    AppAttemptRemovedSchedulerEvent attRemoveEvent =
        new AppAttemptRemovedSchedulerEvent(
            appAttemptId,
            RMAppAttemptState.KILLED,
            false);
    scheduler.handle(attRemoveEvent);

    // The reservation metrics should be subtracted
    assertEquals(0, metrics.getReservedContainers());
    assertEquals(0, metrics.getReservedMB());
    assertEquals(0, metrics.getReservedVirtualCores());
  }


  @Test
  public void testUpdateDemand() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    Resource maxResource = Resources.createResource(1024 * 8);
    ConfigurableResource maxResourceConf =
        new ConfigurableResource(maxResource);

    FSAppAttempt app1 = mock(FSAppAttempt.class);
    Mockito.when(app1.getDemand()).thenReturn(maxResource);
    Mockito.when(app1.getResourceUsage()).thenReturn(Resources.none());
    FSAppAttempt app2 = mock(FSAppAttempt.class);
    Mockito.when(app2.getDemand()).thenReturn(maxResource);
    Mockito.when(app2.getResourceUsage()).thenReturn(Resources.none());

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("queue1", true);

    FSLeafQueue aQueue =
        new FSLeafQueue("root.queue1.a", scheduler, queue1);
    aQueue.setMaxShare(maxResourceConf);
    aQueue.addApp(app1, true);

    FSLeafQueue bQueue =
        new FSLeafQueue("root.queue1.b", scheduler, queue1);
    bQueue.setMaxShare(maxResourceConf);
    bQueue.addApp(app2, true);

    queue1.setMaxShare(maxResourceConf);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    queue1.updateDemand();

    assertTrue("Demand is greater than max allowed ",
        Resources.equals(queue1.getDemand(), maxResource));
    assertTrue("Demand of child queue not updated ",
        Resources.equals(aQueue.getDemand(), maxResource) &&
        Resources.equals(bQueue.getDemand(), maxResource));
  }

  @Test
  public void testDumpState() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"parent\">");
    out.println("  <queue name=\"child1\">");
    out.println("    <weight>1</weight>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSLeafQueue child1 =
        scheduler.getQueueManager().getLeafQueue("parent.child1", false);
    Resource resource = Resource.newInstance(4 * GB, 4);
    child1.setMaxShare(new ConfigurableResource(resource));
    FSAppAttempt app = mock(FSAppAttempt.class);
    Mockito.when(app.getDemand()).thenReturn(resource);
    Mockito.when(app.getResourceUsage()).thenReturn(resource);
    child1.addApp(app, true);
    child1.updateDemand();

    String childQueueString = "{Name: root.parent.child1,"
        + " Weight: 1.0,"
        + " Policy: fair,"
        + " FairShare: <memory:0, vCores:0>,"
        + " SteadyFairShare: <memory:0, vCores:0>,"
        + " MaxShare: <memory:4096, vCores:4>,"
        + " MinShare: <memory:0, vCores:0>,"
        + " ResourceUsage: <memory:4096, vCores:4>,"
        + " Demand: <memory:4096, vCores:4>,"
        + " Runnable: 1,"
        + " NumPendingApps: 0,"
        + " NonRunnable: 0,"
        + " MaxAMShare: 0.5,"
        + " MaxAMResource: <memory:0, vCores:0>,"
        + " AMResourceUsage: <memory:0, vCores:0>,"
        + " LastTimeAtMinShare: " + clock.getTime()
        + "}";

    assertEquals("Unexpected state dump string",
        childQueueString, child1.dumpState());
    FSParentQueue parent =
        scheduler.getQueueManager().getParentQueue("parent", false);
    parent.setMaxShare(new ConfigurableResource(resource));
    parent.updateDemand();

    String parentQueueString = "{Name: root.parent,"
        + " Weight: 1.0,"
        + " Policy: fair,"
        + " FairShare: <memory:0, vCores:0>,"
        + " SteadyFairShare: <memory:0, vCores:0>,"
        + " MaxShare: <memory:4096, vCores:4>,"
        + " MinShare: <memory:0, vCores:0>,"
        + " ResourceUsage: <memory:4096, vCores:4>,"
        + " Demand: <memory:4096, vCores:4>,"
        + " MaxAMShare: 0.5,"
        + " Runnable: 0}";

    assertEquals("Unexpected state dump string",
        parentQueueString + ", " + childQueueString, parent.dumpState());
  }

  @Test
  public void testCompletedContainerOnRemovedNode() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(2048), 2,
        "127.0.0.2");
    scheduler.handle(new NodeAddedSchedulerEvent(node));

    // Create application attempt
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createMockRMApp(appAttemptId);
    scheduler.addApplication(appAttemptId.getApplicationId(), "root.queue1",
        "user1", false);
    scheduler.addApplicationAttempt(appAttemptId, false, false);

    // Create container request that goes to a specific node.
    // Without the 2nd and 3rd request we do not get live containers
    List<ResourceRequest> ask1 = new ArrayList<>();
    ResourceRequest request1 =
        createResourceRequest(1024, node.getHostName(), 1, 1, true);
    ask1.add(request1);
    ResourceRequest request2 =
        createResourceRequest(1024, node.getRackName(), 1, 1, false);
    ask1.add(request2);
    ResourceRequest request3 =
        createResourceRequest(1024, ResourceRequest.ANY, 1, 1, false);
    ask1.add(request3);

    // Perform allocation
    scheduler.allocate(appAttemptId, ask1, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node));

    // Get the allocated containers for the application (list can not be null)
    Collection<RMContainer> clist = scheduler.getSchedulerApp(appAttemptId)
        .getLiveContainers();
    Assert.assertEquals(1, clist.size());

    // Make sure that we remove the correct node (should never fail)
    RMContainer rmc = clist.iterator().next();
    NodeId containerNodeID = rmc.getAllocatedNode();
    assertEquals(node.getNodeID(), containerNodeID);

    // Remove node
    scheduler.handle(new NodeRemovedSchedulerEvent(node));

    // Call completedContainer() should not fail even if the node has been
    // removed
    scheduler.completedContainer(rmc,
        SchedulerUtils.createAbnormalContainerStatus(rmc.getContainerId(),
            SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.EXPIRE);
  }
}
