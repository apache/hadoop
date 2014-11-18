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
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.MemoryRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
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
    resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new MemoryRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
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
    if (resourceManager != null) {
      resourceManager.stop();
    }
  }


  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
    ResourceScheduler scheduler = new CapacityScheduler();
    scheduler.setRMContext(resourceManager.getRMContext());
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
    RMContextImpl rmContext =  new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null);
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, rmContext);
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    cs.reinitialize(conf, mockContext);
    checkQueueCapacities(cs, 80f, 20f);
    cs.stop();
  }

  void checkQueueCapacities(CapacityScheduler cs,
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
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(csConf);
    cs.start();
    cs.reinitialize(csConf, new RMContextImpl(null, null, null, null,
      null, null, new RMContainerTokenSecretManager(csConf),
      new NMTokenSecretManagerInRM(csConf),
      new ClientToAMTokenSecretManagerInRM(), null));

    RMNode n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1);
    RMNode n2 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 2);

    cs.handle(new NodeAddedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n2));

    Assert.assertEquals(6 * GB, cs.getClusterResource().getMemory());

    // reconnect n1 with downgraded memory
    n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 1);
    cs.handle(new NodeRemovedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n1));

    Assert.assertEquals(4 * GB, cs.getClusterResource().getMemory());
    cs.stop();
  }

  @Test
  public void testRefreshQueuesWithNewQueue() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
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
      cs.stop();
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
  
  @Test
  public void testResourceOverCommit() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * GB);
    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 2GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    // check node report, 2 GB used and 2 GB available
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm1.getAvailableResource().getMemory());

    // add request for containers
    am1.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, 2 * GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler, 2 GB given to AM1, resource remaining 0
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      alloc1Response = am1.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());
    
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    // check node report, 4 GB used and 0 GB available
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemory());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemory());

    // check container is assigned with 2 GB.
    Container c1 = allocated1.get(0);
    Assert.assertEquals(2 * GB, c1.getResource().getMemory());
    
    // update node resource to 2 GB, so resource is over-consumed.
    Map<NodeId, ResourceOption> nodeResourceMap = 
        new HashMap<NodeId, ResourceOption>();
    nodeResourceMap.put(nm1.getNodeId(), 
        ResourceOption.newInstance(Resource.newInstance(2 * GB, 1), -1));
    UpdateNodeResourceRequest request = 
        UpdateNodeResourceRequest.newInstance(nodeResourceMap);
    AdminService as = ((MockRM)rm).getAdminService();
    as.updateNodeResource(request);
    
    // Now, the used resource is still 4 GB, and available resource is minus value.
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(-2 * GB, report_nm1.getAvailableResource().getMemory());
    
    // Check container can complete successfully in case of resource over-commitment.
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        c1.getId(), ContainerState.COMPLETE, "", 0);
    nm1.containerStatus(containerStatus);
    int waitCount = 0;
    while (attempt1.getJustFinishedContainers().size() < 1
        && waitCount++ != 20) {
      LOG.info("Waiting for containers to be finished for app 1... Tried "
          + waitCount + " times already..");
      Thread.sleep(100);
    }
    Assert.assertEquals(1, attempt1.getJustFinishedContainers().size());
    Assert.assertEquals(1, am1.schedule().getCompletedContainersStatuses().size());
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());
    // As container return 2 GB back, the available resource becomes 0 again.
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());
    
    // Verify no NPE is trigger in schedule after resource is updated.
    am1.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, 3 * GB, 1, 1);
    alloc1Response = am1.schedule();
    Assert.assertEquals("Shouldn't have enough resource to allocate containers",
        0, alloc1Response.getAllocatedContainers().size());
    int times = 0;
    // try 10 times as scheduling is async process.
    while (alloc1Response.getAllocatedContainers().size() < 1
        && times++ < 10) {
      LOG.info("Waiting for containers to be allocated for app 1... Tried "
          + times + " times already..");
      Thread.sleep(100);
    }
    Assert.assertEquals("Shouldn't have enough resource to allocate containers",
        0, alloc1Response.getAllocatedContainers().size());
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
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    @SuppressWarnings("unchecked")
    AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> cs =
        (AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) rm
          .getResourceScheduler();

    SchedulerApplication<SchedulerApplicationAttempt> app =
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
  
  private MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }

  private void waitForAppPreemptionInfo(RMApp app, Resource preempted,
      int numAMPreempted, int numTaskPreempted,
      Resource currentAttemptPreempted, boolean currentAttemptAMPreempted,
      int numLatestAttemptTaskPreempted) throws InterruptedException {
    while (true) {
      RMAppMetrics appPM = app.getRMAppMetrics();
      RMAppAttemptMetrics attemptPM =
          app.getCurrentAppAttempt().getRMAppAttemptMetrics();

      if (appPM.getResourcePreempted().equals(preempted)
          && appPM.getNumAMContainersPreempted() == numAMPreempted
          && appPM.getNumNonAMContainersPreempted() == numTaskPreempted
          && attemptPM.getResourcePreempted().equals(currentAttemptPreempted)
          && app.getCurrentAppAttempt().getRMAppAttemptMetrics()
            .getIsPreempted() == currentAttemptAMPreempted
          && attemptPM.getNumNonAMContainersPreempted() == 
             numLatestAttemptTaskPreempted) {
        return;
      }
      Thread.sleep(500);
    }
  }

  private void waitForNewAttemptCreated(RMApp app,
      ApplicationAttemptId previousAttemptId) throws InterruptedException {
    while (app.getCurrentAppAttempt().equals(previousAttemptId)) {
      Thread.sleep(500);
    }
  }
  
  @Test(timeout = 30000)
  public void testAllocateDoesNotBlockOnSchedulerLock() throws Exception {
    final YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MyContainerManager containerManager = new MyContainerManager();
    final MockRMWithAMS rm =
        new MockRMWithAMS(conf, containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    RMApp app = rm.submitApp(1024, "appname", "appuser", acls);

    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    int msecToWait = 10000;
    int msecToSleep = 100;
    while (attempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED
        && msecToWait > 0) {
      LOG.info("Waiting for AppAttempt to reach LAUNCHED state. "
          + "Current state is " + attempt.getAppAttemptState());
      Thread.sleep(msecToSleep);
      msecToWait -= msecToSleep;
    }
    Assert.assertEquals(attempt.getAppAttemptState(),
        RMAppAttemptState.LAUNCHED);

    // Create a client to the RM.
    final YarnRPC rpc = YarnRPC.create(conf);

    UserGroupInformation currentUser =
        UserGroupInformation.createRemoteUser(applicationAttemptId.toString());
    Credentials credentials = containerManager.getContainerCredentials();
    final InetSocketAddress rmBindAddress =
        rm.getApplicationMasterService().getBindAddress();
    Token<? extends TokenIdentifier> amRMToken =
        MockRMWithAMS.setupAndReturnAMRMToken(rmBindAddress,
          credentials.getAllTokens());
    currentUser.addToken(amRMToken);
    ApplicationMasterProtocol client =
        currentUser.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) rpc.getProxy(
              ApplicationMasterProtocol.class, rmBindAddress, conf);
          }
        });

    RegisterApplicationMasterRequest request =
        RegisterApplicationMasterRequest.newInstance("localhost", 12345, "");
    client.registerApplicationMaster(request);

    // grab the scheduler lock from another thread
    // and verify an allocate call in this thread doesn't block on it
    final CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    final CyclicBarrier barrier = new CyclicBarrier(2);
    Thread otherThread = new Thread(new Runnable() {
      @Override
      public void run() {
        synchronized(cs) {
          try {
            barrier.await();
            barrier.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (BrokenBarrierException e) {
            e.printStackTrace();
          }
        }
      }
    });
    otherThread.start();
    barrier.await();
    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(0, 0.0f, null, null, null);
    client.allocate(allocateRequest);
    barrier.await();
    otherThread.join();

    rm.stop();
  }

  @Test
  public void testNumClusterNodes() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(conf);
    RMContext rmContext = TestUtils.getMockRMContext();
    cs.setRMContext(rmContext);
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    cs.init(csConf);
    cs.start();
    assertEquals(0, cs.getNumClusterNodes());

    RMNode n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1);
    RMNode n2 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 2);
    cs.handle(new NodeAddedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n2));
    assertEquals(2, cs.getNumClusterNodes());

    cs.handle(new NodeRemovedSchedulerEvent(n1));
    assertEquals(1, cs.getNumClusterNodes());
    cs.handle(new NodeAddedSchedulerEvent(n1));
    assertEquals(2, cs.getNumClusterNodes());
    cs.handle(new NodeRemovedSchedulerEvent(n2));
    cs.handle(new NodeRemovedSchedulerEvent(n1));
    assertEquals(0, cs.getNumClusterNodes());

    cs.stop();
  }

  @Test(timeout = 120000)
  public void testPreemptionInfo() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 3);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    int CONTAINER_MEMORY = 1024; // start RM
    MockRM rm1 = new MockRM(conf);
    rm1.start();

    // get scheduler
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    // start NM
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(CONTAINER_MEMORY);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // get scheduler app
    FiCaSchedulerApp schedulerAppAttempt =
        cs.getSchedulerApplications().get(app0.getApplicationId())
            .getCurrentAppAttempt();

    // allocate some containers and launch them
    List<Container> allocatedContainers =
        am0.allocateAndWaitForContainers(3, CONTAINER_MEMORY, nm1);

    // kill the 3 containers
    for (Container c : allocatedContainers) {
      cs.killContainer(schedulerAppAttempt.getRMContainer(c.getId()));
    }

    // check values
    waitForAppPreemptionInfo(app0,
        Resource.newInstance(CONTAINER_MEMORY * 3, 3), 0, 3,
        Resource.newInstance(CONTAINER_MEMORY * 3, 3), false, 3);

    // kill app0-attempt0 AM container
    cs.killContainer(schedulerAppAttempt.getRMContainer(app0
        .getCurrentAppAttempt().getMasterContainer().getId()));

    // wait for app0 failed
    waitForNewAttemptCreated(app0, am0.getApplicationAttemptId());

    // check values
    waitForAppPreemptionInfo(app0,
        Resource.newInstance(CONTAINER_MEMORY * 4, 4), 1, 3,
        Resource.newInstance(0, 0), false, 0);

    // launch app0-attempt1
    MockAM am1 = launchAM(app0, rm1, nm1);
    schedulerAppAttempt =
        cs.getSchedulerApplications().get(app0.getApplicationId())
            .getCurrentAppAttempt();

    // allocate some containers and launch them
    allocatedContainers =
        am1.allocateAndWaitForContainers(3, CONTAINER_MEMORY, nm1);
    for (Container c : allocatedContainers) {
      cs.killContainer(schedulerAppAttempt.getRMContainer(c.getId()));
    }

    // check values
    waitForAppPreemptionInfo(app0,
        Resource.newInstance(CONTAINER_MEMORY * 7, 7), 1, 6,
        Resource.newInstance(CONTAINER_MEMORY * 3, 3), false, 3);

    rm1.stop();
  }
  
  @Test(timeout = 30000)
  public void testRecoverRequestAfterPreemption() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8000);
    RMApp app1 = rm1.submitApp(1024);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId1 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId1, RMContainerState.ALLOCATED);

    RMContainer rmContainer = cs.getRMContainer(containerId1);
    List<ResourceRequest> requests = rmContainer.getResourceRequests();
    FiCaSchedulerApp app = cs.getApplicationAttempt(am1
        .getApplicationAttemptId());

    FiCaSchedulerNode node = cs.getNode(rmContainer.getAllocatedNode());
    for (ResourceRequest request : requests) {
      // Skip the OffRack and RackLocal resource requests.
      if (request.getResourceName().equals(node.getRackName())
          || request.getResourceName().equals(ResourceRequest.ANY)) {
        continue;
      }

      // Already the node local resource request is cleared from RM after
      // allocation.
      Assert.assertNull(app.getResourceRequest(request.getPriority(),
          request.getResourceName()));
    }

    // Call killContainer to preempt the container
    cs.killContainer(rmContainer);

    Assert.assertEquals(3, requests.size());
    for (ResourceRequest request : requests) {
      // Resource request must have added back in RM after preempt event
      // handling.
      Assert.assertEquals(
          1,
          app.getResourceRequest(request.getPriority(),
              request.getResourceName()).getNumContainers());
    }

    // New container will be allocated and will move to ALLOCATED state
    ContainerId containerId2 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 3);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    // allocate container
    List<Container> containers = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();

    // Now with updated ResourceRequest, a container is allocated for AM.
    Assert.assertTrue(containers.size() == 1);
  }

  private MockRM setUpMove() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    return rm;
  }

  @Test
  public void testMoveAppBasic() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals("a1"));

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    scheduler.moveApplication(app.getApplicationId(), "b1");

    // check postconditions
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInB1.size());
    queue =
        scheduler.getApplicationAttempt(appsInB1.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals("b1"));

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.contains(appAttemptId));
    assertEquals(1, appsInB.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAppSameParent() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals("a1"));

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInA2 = scheduler.getAppsInQueue("a2");
    assertTrue(appsInA2.isEmpty());

    // now move the app
    scheduler.moveApplication(app.getApplicationId(), "a2");

    // check postconditions
    appsInA2 = scheduler.getAppsInQueue("a2");
    assertEquals(1, appsInA2.size());
    queue =
        scheduler.getApplicationAttempt(appsInA2.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals("a2"));

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    rm.stop();
  }

  @Test
  public void testMoveAppForMoveToQueueWithFreeCap() throws Exception {

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();
    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(4 * GB, 1));

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(2 * GB, 1));

    // ResourceRequest priorities
    Priority priority_0 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(0);
    Priority priority_1 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(1 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0, host_1 });
    application_0.addTask(task_0_0);

    // Submit application_1
    Application application_1 =
        new Application("user_1", "b2", resourceManager);
    application_1.submit(); // app + app attempt event sent to scheduler

    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);

    Resource capability_1_0 = Resources.createResource(1 * GB, 1);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);

    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 =
        new Task(application_1, priority_1, new String[] { host_0, host_1 });
    application_1.addTask(task_1_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate
    application_1.schedule(); // allocate

    // task_0_0 task_1_0 allocated, used=2G
    nodeUpdate(nm_0);

    // nothing allocated
    nodeUpdate(nm_1);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(1 * GB, application_0);

    application_1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application_1);

    checkNodeResourceUsage(2 * GB, nm_0); // task_0_0 (1G) and task_1_0 (1G) 2G
                                          // available
    checkNodeResourceUsage(0 * GB, nm_1); // no tasks, 2G available

    // move app from a1(30% cap of total 10.5% cap) to b1(79,2% cap of 89,5%
    // total cap)
    scheduler.moveApplication(application_0.getApplicationId(), "b1");

    // 2GB 1C
    Task task_1_1 =
        new Task(application_1, priority_0,
            new String[] { ResourceRequest.ANY });
    application_1.addTask(task_1_1);

    application_1.schedule();

    // 2GB 1C
    Task task_0_1 =
        new Task(application_0, priority_0, new String[] { host_0, host_1 });
    application_0.addTask(task_0_1);

    application_0.schedule();

    // prev 2G used free 2G
    nodeUpdate(nm_0);

    // prev 0G used free 2G
    nodeUpdate(nm_1);

    // Get allocations from the scheduler
    application_1.schedule();
    checkApplicationResourceUsage(3 * GB, application_1);

    // Get allocations from the scheduler
    application_0.schedule();
    checkApplicationResourceUsage(3 * GB, application_0);

    checkNodeResourceUsage(4 * GB, nm_0);
    checkNodeResourceUsage(2 * GB, nm_1);

  }

  @Test
  public void testMoveAppSuccess() throws Exception {

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1));

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1));

    // ResourceRequest priorities
    Priority priority_0 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(0);
    Priority priority_1 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(3 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0, host_1 });
    application_0.addTask(task_0_0);

    // Submit application_1
    Application application_1 =
        new Application("user_1", "b2", resourceManager);
    application_1.submit(); // app + app attempt event sent to scheduler

    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);

    Resource capability_1_0 = Resources.createResource(1 * GB, 1);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);

    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 =
        new Task(application_1, priority_1, new String[] { host_0, host_1 });
    application_1.addTask(task_1_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate
    application_1.schedule(); // allocate

    // b2 can only run 1 app at a time
    scheduler.moveApplication(application_0.getApplicationId(), "b2");

    nodeUpdate(nm_0);

    nodeUpdate(nm_1);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(0 * GB, application_0);

    application_1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application_1);

    // task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
    // not scheduled
    checkNodeResourceUsage(1 * GB, nm_0);
    checkNodeResourceUsage(0 * GB, nm_1);

    // lets move application_0 to a queue where it can run
    scheduler.moveApplication(application_0.getApplicationId(), "a2");
    application_0.schedule();

    nodeUpdate(nm_1);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application_0);

    checkNodeResourceUsage(1 * GB, nm_0);
    checkNodeResourceUsage(3 * GB, nm_1);

  }

  @Test(expected = YarnException.class)
  public void testMoveAppViolateQueueState() throws Exception {
    resourceManager = new ResourceManager() {
       @Override
        protected RMNodeLabelsManager createNodeLabelManager() {
          RMNodeLabelsManager mgr = new MemoryRMNodeLabelsManager();
          mgr.init(getConfig());
          return mgr;
        }
    };
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    StringBuilder qState = new StringBuilder();
    qState.append(CapacitySchedulerConfiguration.PREFIX).append(B)
        .append(CapacitySchedulerConfiguration.DOT)
        .append(CapacitySchedulerConfiguration.STATE);
    csConf.set(qState.toString(), QueueState.STOPPED.name());
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager()
        .rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
    mockContext = mock(RMContext.class);
    when(mockContext.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(6 * GB, 1));

    // ResourceRequest priorities
    Priority priority_0 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(0);
    Priority priority_1 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);

    Resource capability_0_0 = Resources.createResource(3 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0 });
    application_0.addTask(task_0_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate

    // task_0_0 allocated
    nodeUpdate(nm_0);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application_0);

    checkNodeResourceUsage(3 * GB, nm_0);
    // b2 queue contains 3GB consumption app,
    // add another 3GB will hit max capacity limit on queue b
    scheduler.moveApplication(application_0.getApplicationId(), "b1");

  }

  @Test
  public void testMoveAppQueueMetricsCheck() throws Exception {
    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1));

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1));

    // ResourceRequest priorities
    Priority priority_0 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(0);
    Priority priority_1 =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Priority
            .create(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(3 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0, host_1 });
    application_0.addTask(task_0_0);

    // Submit application_1
    Application application_1 =
        new Application("user_1", "b2", resourceManager);
    application_1.submit(); // app + app attempt event sent to scheduler

    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);

    Resource capability_1_0 = Resources.createResource(1 * GB, 1);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);

    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 =
        new Task(application_1, priority_1, new String[] { host_0, host_1 });
    application_1.addTask(task_1_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate
    application_1.schedule(); // allocate

    nodeUpdate(nm_0);

    nodeUpdate(nm_1);

    CapacityScheduler cs =
        (CapacityScheduler) resourceManager.getResourceScheduler();
    CSQueue origRootQ = cs.getRootQueue();
    CapacitySchedulerInfo oldInfo = new CapacitySchedulerInfo(origRootQ);
    int origNumAppsA = getNumAppsInQueue("a", origRootQ.getChildQueues());
    int origNumAppsRoot = origRootQ.getNumApplications();

    scheduler.moveApplication(application_0.getApplicationId(), "a2");

    CSQueue newRootQ = cs.getRootQueue();
    int newNumAppsA = getNumAppsInQueue("a", newRootQ.getChildQueues());
    int newNumAppsRoot = newRootQ.getNumApplications();
    CapacitySchedulerInfo newInfo = new CapacitySchedulerInfo(newRootQ);
    CapacitySchedulerLeafQueueInfo origOldA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo origNewA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", newInfo.getQueues());
    CapacitySchedulerLeafQueueInfo targetOldA2 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a2", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo targetNewA2 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a2", newInfo.getQueues());
    // originally submitted here
    assertEquals(1, origOldA1.getNumApplications());
    assertEquals(1, origNumAppsA);
    assertEquals(2, origNumAppsRoot);
    // after the move
    assertEquals(0, origNewA1.getNumApplications());
    assertEquals(1, newNumAppsA);
    assertEquals(2, newNumAppsRoot);
    // original consumption on a1
    assertEquals(3 * GB, origOldA1.getResourcesUsed().getMemory());
    assertEquals(1, origOldA1.getResourcesUsed().getvCores());
    assertEquals(0, origNewA1.getResourcesUsed().getMemory()); // after the move
    assertEquals(0, origNewA1.getResourcesUsed().getvCores()); // after the move
    // app moved here with live containers
    assertEquals(3 * GB, targetNewA2.getResourcesUsed().getMemory());
    assertEquals(1, targetNewA2.getResourcesUsed().getvCores());
    // it was empty before the move
    assertEquals(0, targetOldA2.getNumApplications());
    assertEquals(0, targetOldA2.getResourcesUsed().getMemory());
    assertEquals(0, targetOldA2.getResourcesUsed().getvCores());
    // after the app moved here
    assertEquals(1, targetNewA2.getNumApplications());
    // 1 container on original queue before move
    assertEquals(1, origOldA1.getNumContainers());
    // after the move the resource released
    assertEquals(0, origNewA1.getNumContainers());
    // and moved to the new queue
    assertEquals(1, targetNewA2.getNumContainers());
    // which originally didn't have any
    assertEquals(0, targetOldA2.getNumContainers());
    // 1 user with 3GB
    assertEquals(3 * GB, origOldA1.getUsers().getUsersList().get(0)
        .getResourcesUsed().getMemory());
    // 1 user with 1 core
    assertEquals(1, origOldA1.getUsers().getUsersList().get(0)
        .getResourcesUsed().getvCores());
    // user ha no more running app in the orig queue
    assertEquals(0, origNewA1.getUsers().getUsersList().size());
    // 1 user with 3GB
    assertEquals(3 * GB, targetNewA2.getUsers().getUsersList().get(0)
        .getResourcesUsed().getMemory());
    // 1 user with 1 core
    assertEquals(1, targetNewA2.getUsers().getUsersList().get(0)
        .getResourcesUsed().getvCores());

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application_0);

    application_1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application_1);

    // task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
    // not scheduled
    checkNodeResourceUsage(4 * GB, nm_0);
    checkNodeResourceUsage(0 * GB, nm_1);

  }

  private int getNumAppsInQueue(String name, List<CSQueue> queues) {
    for (CSQueue queue : queues) {
      if (queue.getQueueName().equals(name)) {
        return queue.getNumApplications();
      }
    }
    return -1;
  }

  private CapacitySchedulerQueueInfo getQueueInfo(String name,
      CapacitySchedulerQueueInfoList info) {
    if (info != null) {
      for (CapacitySchedulerQueueInfo queueInfo : info.getQueueInfoList()) {
        if (queueInfo.getQueueName().equals(name)) {
          return queueInfo;
        } else {
          CapacitySchedulerQueueInfo result =
              getQueueInfo(name, queueInfo.getQueues());
          if (result == null) {
            continue;
          }
          return result;
        }
      }
    }
    return null;
  }

  @Test
  public void testMoveAllApps() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals("a1"));

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    scheduler.moveAllApps("a1", "b1");

    // check postconditions
    Thread.sleep(1000);
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInB1.size());
    queue =
        scheduler.getApplicationAttempt(appsInB1.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals("b1"));

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.contains(appAttemptId));
    assertEquals(1, appsInB.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAllAppsInvalidDestination() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    try {
      scheduler.moveAllApps("a1", "DOES_NOT_EXIST");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check postconditions, app should still be in a1
    appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAllAppsInvalidSource() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    try {
      scheduler.moveAllApps("DOES_NOT_EXIST", "b1");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check postconditions, app should still be in a1
    appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    rm.stop();
  }

  @Test
  public void testKillAllAppsInQueue() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertTrue(queue.equals("a1"));

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // now kill the app
    scheduler.killAllAppsInQueue("a1");

    // check postconditions
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);
    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.isEmpty());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testKillAllAppsInvalidSource() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    RMApp app = rm.submitApp(GB, "test-move-1", "user_0", null, "a1");
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // now kill the app
    try {
      scheduler.killAllAppsInQueue("DOES_NOT_EXIST");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check postconditions, app should still be in a1
    appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    rm.stop();
  }

  // Test to ensure that we don't carry out reservation on nodes
  // that have no CPU available when using the DominantResourceCalculator
  @Test(timeout = 30000)
  public void testAppReservationWithDominantResourceCalculator() throws Exception {
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DominantResourceCalculator.class);

    YarnConfiguration conf = new YarnConfiguration(csconf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB, 1);

    // register extra nodes to bump up cluster resource
    MockNM nm2 = rm.registerNode("127.0.0.1:1235", 10 * GB, 4);
    rm.registerNode("127.0.0.1:1236", 10 * GB, 4);

    RMApp app1 = rm.submitApp(1024);
    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    // check node report
    Assert.assertEquals(1 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(9 * GB, report_nm1.getAvailableResource().getMemory());

    // add request for containers
    am1.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, 1 * GB, 1, 1);
    am1.schedule(); // send the request

    // kick the scheduler, container reservation should not happen
    nm1.nodeHeartbeat(true);
    Thread.sleep(1000);
    AllocateResponse allocResponse = am1.schedule();
    ApplicationResourceUsageReport report =
        rm.getResourceScheduler().getAppResourceUsageReport(
          attempt1.getAppAttemptId());
    Assert.assertEquals(0, allocResponse.getAllocatedContainers().size());
    Assert.assertEquals(0, report.getNumReservedContainers());

    // container should get allocated on this node
    nm2.nodeHeartbeat(true);

    while (allocResponse.getAllocatedContainers().size() == 0) {
      Thread.sleep(100);
      allocResponse = am1.schedule();
    }
    report =
        rm.getResourceScheduler().getAppResourceUsageReport(
          attempt1.getAppAttemptId());
    Assert.assertEquals(1, allocResponse.getAllocatedContainers().size());
    Assert.assertEquals(0, report.getNumReservedContainers());
    rm.stop();
  }
}
