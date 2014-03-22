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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TestCapacityScheduler {
  private static final Log LOG = LogFactory.getLog(TestCapacityScheduler.class);
  private final int GB = 1024;
  
  private static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String A1 = A + ".a1";
  private static final String A2 = A + ".a2";
  private static final String B1 = B + ".b1";
  private static final String B2 = B + ".b2";
  private static final String B3 = B + ".b3";
  private static float A_CAPACITY = 10.5f;
  private static float B_CAPACITY = 89.5f;
  private static float A1_CAPACITY = 30;
  private static float A2_CAPACITY = 70;
  private static float B1_CAPACITY = 79.2f;
  private static float B2_CAPACITY = 0.8f;
  private static float B3_CAPACITY = 20;

  private ResourceManager resourceManager = null;
  private RMContext mockContext;
  
  @Before
  public void setUp() throws Exception {
    resourceManager = new ResourceManager();
    CapacitySchedulerConfiguration csConf 
       = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, 
        CapacityScheduler.class, ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    mockContext = mock(RMContext.class);
    when(mockContext.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());
  }

  @After
  public void tearDown() throws Exception {
    resourceManager.stop();
  }


  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
    ResourceScheduler scheduler = new CapacityScheduler();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2048);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    try {
      scheduler.reinitialize(conf, mockContext);
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
      scheduler.reinitialize(conf, mockContext);
      fail("Exception is expected because the min vcores allocation is" +
        " larger than the max vcores allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue("The thrown exception is not the expected one.",
        e.getMessage().startsWith(
          "Invalid resource scheduler vcores"));
    }
  }

  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager
      registerNode(String hostName, int containerManagerPort, int httpPort,
          String rackName, Resource capability)
          throws IOException, YarnException {
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm =
        new org.apache.hadoop.yarn.server.resourcemanager.NodeManager(
            hostName, containerManagerPort, httpPort, rackName, capability,
            resourceManager);
    NodeAddedSchedulerEvent nodeAddEvent1 = 
        new NodeAddedSchedulerEvent(resourceManager.getRMContext()
            .getRMNodes().get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  @Test
  public void testCapacityScheduler() throws Exception {

    LOG.info("--- START: testCapacityScheduler ---");
        
    // Register node1
    String host_0 = "host_0";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_0 = 
      registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(4 * GB, 1));
    
    // Register node2
    String host_1 = "host_1";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_1 = 
      registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK, 
          Resources.createResource(2 * GB, 1));

    // ResourceRequest priorities
    Priority priority_0 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(0); 
    Priority priority_1 = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(1);
    
    // Submit an application
    Application application_0 = new Application("user_0", "a1", resourceManager);
    application_0.submit();
    
    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(1 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);
    
    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 = new Task(application_0, priority_1, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_0);
       
    // Submit another application
    Application application_1 = new Application("user_1", "b2", resourceManager);
    application_1.submit();
    
    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);
    
    Resource capability_1_0 = Resources.createResource(3 * GB, 1);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);
    
    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 = new Task(application_1, priority_1, 
        new String[] {host_0, host_1});
    application_1.addTask(task_1_0);
        
    // Send resource requests to the scheduler
    application_0.schedule();
    application_1.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Kick!");
    
    // task_0_0 and task_1_0 allocated, used=4G
    nodeUpdate(nm_0);
    
    // nothing allocated
    nodeUpdate(nm_1);
    
    // Get allocations from the scheduler
    application_0.schedule();     // task_0_0 
    checkApplicationResourceUsage(1 * GB, application_0);

    application_1.schedule();     // task_1_0
    checkApplicationResourceUsage(3 * GB, application_1);
    
    checkNodeResourceUsage(4*GB, nm_0);  // task_0_0 (1G) and task_1_0 (3G)
    checkNodeResourceUsage(0*GB, nm_1);  // no tasks, 2G available

    LOG.info("Adding new tasks...");
    
    Task task_1_1 = new Task(application_1, priority_0, 
        new String[] {ResourceRequest.ANY});
    application_1.addTask(task_1_1);

    application_1.schedule();

    Task task_0_1 = new Task(application_0, priority_0, 
        new String[] {host_0, host_1});
    application_0.addTask(task_0_1);

    application_0.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Sending hb from " + nm_0.getHostName());
    // nothing new, used=4G
    nodeUpdate(nm_0);
    
    LOG.info("Sending hb from " + nm_1.getHostName());
    // task_0_1 is prefer as locality, used=2G
    nodeUpdate(nm_1);
    
    // Get allocations from the scheduler
    LOG.info("Trying to allocate...");
    application_0.schedule();
    checkApplicationResourceUsage(1 * GB, application_0);

    application_1.schedule();
    checkApplicationResourceUsage(5 * GB, application_1);
    
    nodeUpdate(nm_0);
    nodeUpdate(nm_1);
    
    checkNodeResourceUsage(4*GB, nm_0);
    checkNodeResourceUsage(2*GB, nm_1);

    LOG.info("--- END: testCapacityScheduler ---");
  }

  private void nodeUpdate(
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm) {
    RMNode node = resourceManager.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);
  }
  
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);
    
    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);
    
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    LOG.info("Setup top-level queues a and b");
  }
  
  @Test
  public void testMaximumCapacitySetup() {
    float delta = 0.0000001f;
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    assertEquals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE,conf.getMaximumCapacity(A),delta);
    conf.setMaximumCapacity(A, 50.0f);
    assertEquals(50.0f, conf.getMaximumCapacity(A),delta);
    conf.setMaximumCapacity(A, -1);
    assertEquals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE,conf.getMaximumCapacity(A),delta);
  }
  
  
  @Test
  public void testRefreshQueues() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.reinitialize(conf, new RMContextImpl(null, null, null, null, null,
      null, new RMContainerTokenSecretManager(conf),
      new NMTokenSecretManagerInRM(conf),
      new ClientToAMTokenSecretManagerInRM(), null));
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    cs.reinitialize(conf, mockContext);
    checkQueueCapacities(cs, 80f, 20f);
  }

  private void checkQueueCapacities(CapacityScheduler cs,
      float capacityA, float capacityB) {
    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueA1 = findQueue(queueA, A1);
    CSQueue queueA2 = findQueue(queueA, A2);
    CSQueue queueB1 = findQueue(queueB, B1);
    CSQueue queueB2 = findQueue(queueB, B2);
    CSQueue queueB3 = findQueue(queueB, B3);

    float capA = capacityA / 100.0f;
    float capB = capacityB / 100.0f;

    checkQueueCapacity(queueA, capA, capA, 1.0f, 1.0f);
    checkQueueCapacity(queueB, capB, capB, 1.0f, 1.0f);
    checkQueueCapacity(queueA1, A1_CAPACITY / 100.0f,
        (A1_CAPACITY/100.0f) * capA, 1.0f, 1.0f);
    checkQueueCapacity(queueA2, A2_CAPACITY / 100.0f,
        (A2_CAPACITY/100.0f) * capA, 1.0f, 1.0f);
    checkQueueCapacity(queueB1, B1_CAPACITY / 100.0f,
        (B1_CAPACITY/100.0f) * capB, 1.0f, 1.0f);
    checkQueueCapacity(queueB2, B2_CAPACITY / 100.0f,
        (B2_CAPACITY/100.0f) * capB, 1.0f, 1.0f);
    checkQueueCapacity(queueB3, B3_CAPACITY / 100.0f,
        (B3_CAPACITY/100.0f) * capB, 1.0f, 1.0f);
  }

  private void checkQueueCapacity(CSQueue q, float expectedCapacity,
      float expectedAbsCapacity, float expectedMaxCapacity,
      float expectedAbsMaxCapacity) {
    final float epsilon = 1e-5f;
    assertEquals("capacity", expectedCapacity, q.getCapacity(), epsilon);
    assertEquals("absolute capacity", expectedAbsCapacity,
        q.getAbsoluteCapacity(), epsilon);
    assertEquals("maximum capacity", expectedMaxCapacity,
        q.getMaximumCapacity(), epsilon);
    assertEquals("absolute maximum capacity", expectedAbsMaxCapacity,
        q.getAbsoluteMaximumCapacity(), epsilon);
  }

  private CSQueue findQueue(CSQueue root, String queuePath) {
    if (root.getQueuePath().equals(queuePath)) {
      return root;
    }

    List<CSQueue> childQueues = root.getChildQueues();
    if (childQueues != null) {
      for (CSQueue q : childQueues) {
        if (queuePath.startsWith(q.getQueuePath())) {
          CSQueue result = findQueue(q, queuePath);
          if (result != null) {
            return result;
          }
        }
      }
    }

    return null;
  }

  private void checkApplicationResourceUsage(int expected, 
      Application application) {
    Assert.assertEquals(expected, application.getUsedResources().getMemory());
  }
  
  private void checkNodeResourceUsage(int expected,
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager node) {
    Assert.assertEquals(expected, node.getUsed().getMemory());
    node.checkResourceUsage();
  }

  /** Test that parseQueue throws an exception when two leaf queues have the
   *  same name
 * @throws IOException
   */
  @Test(expected=IOException.class)
  public void testParseQueue() throws IOException {
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());

    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setQueues(CapacitySchedulerConfiguration.ROOT + ".a.a1", new String[] {"b1"} );
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".a.a1.b1", 100.0f);
    conf.setUserLimitFactor(CapacitySchedulerConfiguration.ROOT + ".a.a1.b1", 100.0f);

    cs.reinitialize(conf, new RMContextImpl(null, null, null, null, null,
      null, new RMContainerTokenSecretManager(conf),
      new NMTokenSecretManagerInRM(conf),
      new ClientToAMTokenSecretManagerInRM(), null));
  }

  @Test
  public void testReconnectedNode() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.reinitialize(csConf, new RMContextImpl(null, null, null, null,
      null, null, new RMContainerTokenSecretManager(csConf),
      new NMTokenSecretManagerInRM(csConf),
      new ClientToAMTokenSecretManagerInRM(), null));

    RMNode n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1);
    RMNode n2 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 2);

    cs.handle(new NodeAddedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n2));

    Assert.assertEquals(6 * GB, cs.getClusterResources().getMemory());

    // reconnect n1 with downgraded memory
    n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 1);
    cs.handle(new NodeRemovedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n1));

    Assert.assertEquals(4 * GB, cs.getClusterResources().getMemory());
  }

  @Test
  public void testRefreshQueuesWithNewQueue() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.reinitialize(conf, new RMContextImpl(null, null, null, null, null,
      null, new RMContainerTokenSecretManager(conf),
      new NMTokenSecretManagerInRM(conf),
      new ClientToAMTokenSecretManagerInRM(), null));
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    // Add a new queue b4
    String B4 = B + ".b4";
    float B4_CAPACITY = 10;
    
    B3_CAPACITY -= B4_CAPACITY;
    try {
      conf.setCapacity(A, 80f);
      conf.setCapacity(B, 20f);
      conf.setQueues(B, new String[] {"b1", "b2", "b3", "b4"});
      conf.setCapacity(B1, B1_CAPACITY);
      conf.setCapacity(B2, B2_CAPACITY);
      conf.setCapacity(B3, B3_CAPACITY);
      conf.setCapacity(B4, B4_CAPACITY);
      cs.reinitialize(conf,mockContext);
      checkQueueCapacities(cs, 80f, 20f);
      
      // Verify parent for B4
      CSQueue rootQueue = cs.getRootQueue();
      CSQueue queueB = findQueue(rootQueue, B);
      CSQueue queueB4 = findQueue(queueB, B4);

      assertEquals(queueB, queueB4.getParent());
    } finally {
      B3_CAPACITY += B4_CAPACITY;
    }
  }
  @Test
  public void testCapacitySchedulerInfo() throws Exception {
    QueueInfo queueInfo = resourceManager.getResourceScheduler().getQueueInfo("a", true, true);
    Assert.assertEquals(queueInfo.getQueueName(), "a");
    Assert.assertEquals(queueInfo.getChildQueues().size(), 2);

    List<QueueUserACLInfo> userACLInfo = resourceManager.getResourceScheduler().getQueueUserAclInfo();
    Assert.assertNotNull(userACLInfo);
    for (QueueUserACLInfo queueUserACLInfo : userACLInfo) {
      Assert.assertEquals(getQueueCount(userACLInfo, queueUserACLInfo.getQueueName()), 1);
    }

  }

  private int getQueueCount(List<QueueUserACLInfo> queueInformation, String queueName) {
    int result = 0;
    for (QueueUserACLInfo queueUserACLInfo : queueInformation) {
      if (queueName.equals(queueUserACLInfo.getQueueName())) {
        result++;
      }
    }
    return result;
  }

  @SuppressWarnings("resource")
  @Test
  public void testBlackListNodes() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    String host = "127.0.0.1";
    RMNode node =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, host);
    cs.handle(new NodeAddedSchedulerEvent(node));

    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "default", "user");
    cs.handle(addAppEvent);
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);

    // Verify the blacklist can be updated independent of requesting containers
    cs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null);
    Assert.assertTrue(cs.getApplicationAttempt(appAttemptId).isBlacklisted(host));
    cs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host));
    Assert.assertFalse(cs.getApplicationAttempt(appAttemptId).isBlacklisted(host));
    rm.stop();
  }

    @Test (timeout = 5000)
    public void testApplicationComparator()
    {
      CapacityScheduler cs = new CapacityScheduler();
      Comparator<FiCaSchedulerApp> appComparator= cs.getApplicationComparator();
      ApplicationId id1 = ApplicationId.newInstance(1, 1);
      ApplicationId id2 = ApplicationId.newInstance(1, 2);
      ApplicationId id3 = ApplicationId.newInstance(2, 1);
      //same clusterId
      FiCaSchedulerApp app1 = Mockito.mock(FiCaSchedulerApp.class);
      when(app1.getApplicationId()).thenReturn(id1);
      FiCaSchedulerApp app2 = Mockito.mock(FiCaSchedulerApp.class);
      when(app2.getApplicationId()).thenReturn(id2);
      FiCaSchedulerApp app3 = Mockito.mock(FiCaSchedulerApp.class);
      when(app3.getApplicationId()).thenReturn(id3);
      assertTrue(appComparator.compare(app1, app2) < 0);
      //different clusterId
      assertTrue(appComparator.compare(app1, app3) < 0);
      assertTrue(appComparator.compare(app2, app3) < 0);
    }
    
    @Test
    public void testGetAppsInQueue() throws Exception {
      Application application_0 = new Application("user_0", "a1", resourceManager);
      application_0.submit();
      
      Application application_1 = new Application("user_0", "a2", resourceManager);
      application_1.submit();
      
      Application application_2 = new Application("user_0", "b2", resourceManager);
      application_2.submit();
      
      ResourceScheduler scheduler = resourceManager.getResourceScheduler();
      
      List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
      assertEquals(1, appsInA1.size());
      
      List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
      assertTrue(appsInA.contains(application_0.getApplicationAttemptId()));
      assertTrue(appsInA.contains(application_1.getApplicationAttemptId()));
      assertEquals(2, appsInA.size());

      List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
      assertTrue(appsInRoot.contains(application_0.getApplicationAttemptId()));
      assertTrue(appsInRoot.contains(application_1.getApplicationAttemptId()));
      assertTrue(appsInRoot.contains(application_2.getApplicationAttemptId()));
      assertEquals(3, appsInRoot.size());
      
      Assert.assertNull(scheduler.getAppsInQueue("nonexistentqueue"));
    }

  @Test
  public void testAddAndRemoveAppFromCapacityScheduler() throws Exception {

    AsyncDispatcher rmDispatcher = new AsyncDispatcher();
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.reinitialize(conf, new RMContextImpl(rmDispatcher, null, null, null,
      null, null, new RMContainerTokenSecretManager(conf),
      new NMTokenSecretManagerInRM(conf),
      new ClientToAMTokenSecretManagerInRM(), null));

    SchedulerApplication app =
        TestSchedulerUtils.verifyAppAddedAndRemovedFromScheduler(
          cs.getSchedulerApplications(), cs, "a1");
    Assert.assertEquals("a1", app.getQueue().getQueueName());
  }
  
  @Test
  public void testAsyncScheduling() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    final int NODES = 100;
    
    // Register nodes
    for (int i=0; i < NODES; ++i) {
      String host = "192.168.1." + i;
      RMNode node =
          MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, host);
      cs.handle(new NodeAddedSchedulerEvent(node));
    }
    
    // Now directly exercise the scheduling loop
    for (int i=0; i < NODES; ++i) {
      CapacityScheduler.schedule(cs);
    }
  }

}
