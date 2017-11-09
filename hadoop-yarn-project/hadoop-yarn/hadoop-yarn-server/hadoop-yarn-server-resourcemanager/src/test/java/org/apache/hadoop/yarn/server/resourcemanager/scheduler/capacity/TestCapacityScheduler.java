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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
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
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
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
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.
    ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestCapacityScheduler {
  private static final Log LOG = LogFactory.getLog(TestCapacityScheduler.class);
  private final int GB = 1024;
  private final static ContainerUpdates NULL_UPDATE_REQUESTS =
      new ContainerUpdates();

  private static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String A1 = A + ".a1";
  private static final String A2 = A + ".a2";
  private static final String B1 = B + ".b1";
  private static final String B2 = B + ".b2";
  private static final String B3 = B + ".b3";
  private static float A_CAPACITY = 10.5f;
  private static float B_CAPACITY = 89.5f;
  private static final String P1 = CapacitySchedulerConfiguration.ROOT + ".p1";
  private static final String P2 = CapacitySchedulerConfiguration.ROOT + ".p2";
  private static final String X1 = P1 + ".x1";
  private static final String X2 = P1 + ".x2";
  private static final String Y1 = P2 + ".y1";
  private static final String Y2 = P2 + ".y2";
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
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
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

  private NodeManager registerNode(ResourceManager rm, String hostName,
      int containerManagerPort, int httpPort, String rackName,
          Resource capability) throws IOException, YarnException {
    NodeManager nm = new NodeManager(hostName,
        containerManagerPort, httpPort, rackName, capability, rm);
    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(rm.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    rm.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
    CapacityScheduler scheduler = new CapacityScheduler();
    scheduler.setRMContext(resourceManager.getRMContext());
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2048);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    try {
      scheduler.init(conf);
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

  private NodeManager
      registerNode(String hostName, int containerManagerPort, int httpPort,
          String rackName, Resource capability)
          throws IOException, YarnException {
    NodeManager nm =
        new NodeManager(
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
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

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

  @Test
  public void testNotAssignMultiple() throws Exception {
    LOG.info("--- START: testNotAssignMultiple ---");
    ResourceManager rm = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setBoolean(
        CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, false);
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    rm.init(conf);
    rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) rm.getRMContext().getDispatcher()).start();
    RMContext mC = mock(RMContext.class);
    when(mC.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(rm, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(10 * GB, 10));

    // ResourceRequest priorities
    Priority priority0 = Priority.newInstance(0);
    Priority priority1 = Priority.newInstance(1);

    // Submit an application
    Application application0 = new Application("user_0", "a1", rm);
    application0.submit();
    application0.addNodeManager(host0, 1234, nm0);

    Resource capability00 = Resources.createResource(1 * GB, 1);
    application0.addResourceRequestSpec(priority0, capability00);

    Resource capability01 = Resources.createResource(2 * GB, 1);
    application0.addResourceRequestSpec(priority1, capability01);

    Task task00 =
        new Task(application0, priority0, new String[] {host0});
    Task task01 =
        new Task(application0, priority1, new String[] {host0});
    application0.addTask(task00);
    application0.addTask(task01);

    // Submit another application
    Application application1 = new Application("user_1", "b2", rm);
    application1.submit();
    application1.addNodeManager(host0, 1234, nm0);

    Resource capability10 = Resources.createResource(3 * GB, 1);
    application1.addResourceRequestSpec(priority0, capability10);

    Resource capability11 = Resources.createResource(4 * GB, 1);
    application1.addResourceRequestSpec(priority1, capability11);

    Task task10 = new Task(application1, priority0, new String[] {host0});
    Task task11 = new Task(application1, priority1, new String[] {host0});
    application1.addTask(task10);
    application1.addTask(task11);

    // Send resource requests to the scheduler
    application0.schedule();

    application1.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Kick!");

    // task00, used=1G
    nodeUpdate(rm, nm0);

    // Get allocations from the scheduler
    application0.schedule();
    application1.schedule();
    // 1 Task per heart beat should be scheduled
    checkNodeResourceUsage(3 * GB, nm0); // task00 (1G)
    checkApplicationResourceUsage(0 * GB, application0);
    checkApplicationResourceUsage(3 * GB, application1);

    // Another heartbeat
    nodeUpdate(rm, nm0);
    application0.schedule();
    checkApplicationResourceUsage(1 * GB, application0);
    application1.schedule();
    checkApplicationResourceUsage(3 * GB, application1);
    checkNodeResourceUsage(4 * GB, nm0);
    LOG.info("--- START: testNotAssignMultiple ---");
  }

  @Test
  public void testAssignMultiple() throws Exception {
    LOG.info("--- START: testAssignMultiple ---");
    ResourceManager rm = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setBoolean(
        CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, true);
    // Each heartbeat will assign 2 containers at most
    csConf.setInt(CapacitySchedulerConfiguration.MAX_ASSIGN_PER_HEARTBEAT, 2);
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    rm.init(conf);
    rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) rm.getRMContext().getDispatcher()).start();
    RMContext mC = mock(RMContext.class);
    when(mC.getConfigurationProvider()).thenReturn(
            new LocalConfigurationProvider());

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(rm, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(10 * GB, 10));

    // ResourceRequest priorities
    Priority priority0 = Priority.newInstance(0);
    Priority priority1 = Priority.newInstance(1);

    // Submit an application
    Application application0 = new Application("user_0", "a1", rm);
    application0.submit();
    application0.addNodeManager(host0, 1234, nm0);

    Resource capability00 = Resources.createResource(1 * GB, 1);
    application0.addResourceRequestSpec(priority0, capability00);

    Resource capability01 = Resources.createResource(2 * GB, 1);
    application0.addResourceRequestSpec(priority1, capability01);

    Task task00 = new Task(application0, priority0, new String[] {host0});
    Task task01 = new Task(application0, priority1, new String[] {host0});
    application0.addTask(task00);
    application0.addTask(task01);

    // Submit another application
    Application application1 = new Application("user_1", "b2", rm);
    application1.submit();
    application1.addNodeManager(host0, 1234, nm0);

    Resource capability10 = Resources.createResource(3 * GB, 1);
    application1.addResourceRequestSpec(priority0, capability10);

    Resource capability11 = Resources.createResource(4 * GB, 1);
    application1.addResourceRequestSpec(priority1, capability11);

    Task task10 =
            new Task(application1, priority0, new String[] {host0});
    Task task11 =
            new Task(application1, priority1, new String[] {host0});
    application1.addTask(task10);
    application1.addTask(task11);

    // Send resource requests to the scheduler
    application0.schedule();

    application1.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    LOG.info("Kick!");

    // task_0_0, used=1G
    nodeUpdate(rm, nm0);

    // Get allocations from the scheduler
    application0.schedule();
    application1.schedule();
    // 1 Task per heart beat should be scheduled
    checkNodeResourceUsage(4 * GB, nm0); // task00 (1G)
    checkApplicationResourceUsage(1 * GB, application0);
    checkApplicationResourceUsage(3 * GB, application1);

    // Another heartbeat
    nodeUpdate(rm, nm0);
    application0.schedule();
    checkApplicationResourceUsage(3 * GB, application0);
    application1.schedule();
    checkApplicationResourceUsage(7 * GB, application1);
    checkNodeResourceUsage(10 * GB, nm0);
    LOG.info("--- START: testAssignMultiple ---");
  }

  private void nodeUpdate(ResourceManager rm, NodeManager nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  private void nodeUpdate(NodeManager nm) {
    RMNode node = resourceManager.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);
  }

  /**
   * @param conf
   * @return
   *           root
   *          /      \
   *        a         b
   *       / \     /  |  \
   *      a1  a2  b1  b2 b3
   *
   */
  private CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

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
    return conf;
  }

  /**
   * @param conf, to be modified
   * @return, CS configuration which has deleted all childred of queue(b)
   *           root
   *          /     \
   *        a        b
   *       / \
   *      a1  a2
   */
  private CapacitySchedulerConfiguration setupQueueConfWithOutChildrenOfB(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a","b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1","a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    LOG.info("Setup top-level queues a and b (without children)");
    return conf;
  }

  /**
   * @param conf, to be modified
   * @return, CS configuration which has deleted a queue(b1)
   *           root
   *          /     \
   *        a        b
   *       / \       | \
   *      a1  a2    b2  b3
   */
  private CapacitySchedulerConfiguration setupQueueConfigurationWithOutB1(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[] { "b2", "b3" });
    conf.setCapacity(B2, B2_CAPACITY + B1_CAPACITY); //as B1 is deleted
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    LOG.info("Setup top-level queues a and b (without b3)");
    return conf;
  }

  /**
   * @param conf, to be modified
   * @return, CS configuration which has converted b1 to parent queue
   *           root
   *          /     \
   *        a        b
   *       / \    /  |  \
   *      a1  a2 b1  b2  b3
   *              |
   *             b11
   */
  private CapacitySchedulerConfiguration
      setupQueueConfigurationWithB1AsParentQueue(
          CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[] {"b1","b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    // Set childQueue for B1
    conf.setQueues(B1, new String[] {"b11"});
    String B11 = B1 + ".b11";
    conf.setCapacity(B11, 100.0f);
    conf.setUserLimitFactor(B11, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return, CS configuration which has deleted a
   *          Parent queue(b)
   */
  private CapacitySchedulerConfiguration setupQueueConfigurationWithOutB(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a" });

    conf.setCapacity(A, A_CAPACITY + B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    LOG.info("Setup top-level queues a");
    return conf;
  }


  private CapacitySchedulerConfiguration setupBlockedQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    conf.setUserLimitFactor(A, 100);
    conf.setUserLimitFactor(B, 100);
    conf.setMaximumCapacity(A, 100);
    conf.setMaximumCapacity(B, 100);
    LOG.info("Setup top-level queues a and b");
    return conf;
  }

  private CapacitySchedulerConfiguration setupOtherBlockedQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"p1", "p2"});

    conf.setCapacity(P1, 50f);
    conf.setMaximumCapacity(P1, 50f);
    conf.setCapacity(P2, 50f);
    conf.setMaximumCapacity(P2, 100f);
    // Define 2nd-level queues
    conf.setQueues(P1, new String[] {"x1", "x2"});
    conf.setCapacity(X1, 80f);
    conf.setMaximumCapacity(X1, 100f);
    conf.setUserLimitFactor(X1, 2f);
    conf.setCapacity(X2, 20f);
    conf.setMaximumCapacity(X2, 100f);
    conf.setUserLimitFactor(X2, 2f);

    conf.setQueues(P2, new String[]{"y1", "y2"});
    conf.setCapacity(Y1, 80f);
    conf.setUserLimitFactor(Y1, 2f);
    conf.setCapacity(Y2, 20f);
    conf.setUserLimitFactor(Y2, 2f);
    return conf;
  }

  @Test
  public void testMaximumCapacitySetup() {
    float delta = 0.0000001f;
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    assertEquals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE,conf.getNonLabeledQueueMaximumCapacity(A),delta);
    conf.setMaximumCapacity(A, 50.0f);
    assertEquals(50.0f, conf.getNonLabeledQueueMaximumCapacity(A),delta);
    conf.setMaximumCapacity(A, -1);
    assertEquals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE,conf.getNonLabeledQueueMaximumCapacity(A),delta);
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
    Assert.assertEquals(expected, application.getUsedResources().getMemorySize());
  }

  private void checkNodeResourceUsage(int expected, NodeManager node) {
    Assert.assertEquals(expected, node.getUsed().getMemorySize());
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
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.init(conf);
    cs.start();

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

    Assert.assertEquals(6 * GB, cs.getClusterResource().getMemorySize());

    // reconnect n1 with downgraded memory
    n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 1);
    cs.handle(new NodeRemovedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n1));

    Assert.assertEquals(4 * GB, cs.getClusterResource().getMemorySize());
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

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext = mock(
        ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);

    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "default", "user");
    cs.handle(addAppEvent);
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);

    // Verify the blacklist can be updated independent of requesting containers
    cs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null, NULL_UPDATE_REQUESTS);
    Assert.assertTrue(cs.getApplicationAttempt(appAttemptId)
        .isPlaceBlacklisted(host));
    cs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host), NULL_UPDATE_REQUESTS);
    Assert.assertFalse(cs.getApplicationAttempt(appAttemptId)
        .isPlaceBlacklisted(host));
    rm.stop();
  }

  @Test
  public void testAllocateReorder() throws Exception {

    //Confirm that allocation (resource request) alone will trigger a change in
    //application ordering where appropriate

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue q = (LeafQueue) cs.getQueue("default");
    Assert.assertNotNull(q);

    FairOrderingPolicy fop = new FairOrderingPolicy();
    fop.setSizeBasedWeight(true);
    q.setOrderingPolicy(fop);

    String host = "127.0.0.1";
    RMNode node =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, host);
    cs.handle(new NodeAddedSchedulerEvent(node));

    //add app begin
    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, 1);

    RMAppAttemptMetrics attemptMetric1 =
        new RMAppAttemptMetrics(appAttemptId1, rm.getRMContext());
    RMAppImpl app1 = mock(RMAppImpl.class);
    when(app1.getApplicationId()).thenReturn(appId1);
    RMAppAttemptImpl attempt1 = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt1.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext = mock(
        ApplicationSubmissionContext.class);
    when(attempt1.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt1.getAppAttemptId()).thenReturn(appAttemptId1);
    when(attempt1.getRMAppAttemptMetrics()).thenReturn(attemptMetric1);
    when(app1.getCurrentAppAttempt()).thenReturn(attempt1);

    rm.getRMContext().getRMApps().put(appId1, app1);

    SchedulerEvent addAppEvent1 =
        new AppAddedSchedulerEvent(appId1, "default", "user");
    cs.handle(addAppEvent1);
    SchedulerEvent addAttemptEvent1 =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    cs.handle(addAttemptEvent1);
    //add app end

    //add app begin
    ApplicationId appId2 = BuilderUtils.newApplicationId(100, 2);
    ApplicationAttemptId appAttemptId2 = BuilderUtils.newApplicationAttemptId(
        appId2, 1);

    RMAppAttemptMetrics attemptMetric2 =
        new RMAppAttemptMetrics(appAttemptId2, rm.getRMContext());
    RMAppImpl app2 = mock(RMAppImpl.class);
    when(app2.getApplicationId()).thenReturn(appId2);
    RMAppAttemptImpl attempt2 = mock(RMAppAttemptImpl.class);
    when(attempt2.getMasterContainer()).thenReturn(container);
    when(attempt2.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt2.getAppAttemptId()).thenReturn(appAttemptId2);
    when(attempt2.getRMAppAttemptMetrics()).thenReturn(attemptMetric2);
    when(app2.getCurrentAppAttempt()).thenReturn(attempt2);

    rm.getRMContext().getRMApps().put(appId2, app2);

    SchedulerEvent addAppEvent2 =
        new AppAddedSchedulerEvent(appId2, "default", "user");
    cs.handle(addAppEvent2);
    SchedulerEvent addAttemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, false);
    cs.handle(addAttemptEvent2);
    //add app end

    RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

    Priority priority = TestUtils.createMockPriority(1);
    ResourceRequest r1 = TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true, priority, recordFactory);

    //This will allocate for app1
    cs.allocate(appAttemptId1,
        Collections.<ResourceRequest>singletonList(r1),
        Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS);

    //And this will result in container assignment for app1
    CapacityScheduler.schedule(cs);

    //Verify that app1 is still first in assignment order
    //This happens because app2 has no demand/a magnitude of NaN, which
    //results in app1 and app2 being equal in the fairness comparison and
    //failling back to fifo (start) ordering
    assertEquals(q.getOrderingPolicy().getAssignmentIterator().next().getId(),
      appId1.toString());

    //Now, allocate for app2 (this would be the first/AM allocation)
    ResourceRequest r2 = TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true, priority, recordFactory);
    cs.allocate(appAttemptId2,
        Collections.<ResourceRequest>singletonList(r2),
        Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS);

    //In this case we do not perform container assignment because we want to
    //verify re-ordering based on the allocation alone

    //Now, the first app for assignment is app2
    assertEquals(q.getOrderingPolicy().getAssignmentIterator().next().getId(),
      appId2.toString());

    rm.stop();
  }

  @Test
  public void testResourceOverCommit() throws Exception {
    int waitCount;
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
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(2 * GB, report_nm1.getAvailableResource().getMemorySize());

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
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemorySize());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());

    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    // check node report, 4 GB used and 0 GB available
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemorySize());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemorySize());

    // check container is assigned with 2 GB.
    Container c1 = allocated1.get(0);
    Assert.assertEquals(2 * GB, c1.getResource().getMemorySize());

    // update node resource to 2 GB, so resource is over-consumed.
    Map<NodeId, ResourceOption> nodeResourceMap =
        new HashMap<NodeId, ResourceOption>();
    nodeResourceMap.put(nm1.getNodeId(),
        ResourceOption.newInstance(Resource.newInstance(2 * GB, 1), -1));
    UpdateNodeResourceRequest request =
        UpdateNodeResourceRequest.newInstance(nodeResourceMap);
    AdminService as = ((MockRM)rm).getAdminService();
    as.updateNodeResource(request);

    waitCount = 0;
    while (waitCount++ != 20) {
      report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
      if (report_nm1.getAvailableResource().getMemorySize() != 0) {
        break;
      }
      LOG.info("Waiting for RMNodeResourceUpdateEvent to be handled... Tried "
          + waitCount + " times already..");
      Thread.sleep(1000);
    }
    // Now, the used resource is still 4 GB, and available resource is minus value.
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(-2 * GB, report_nm1.getAvailableResource().getMemorySize());

    // Check container can complete successfully in case of resource over-commitment.
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        c1.getId(), ContainerState.COMPLETE, "", 0, c1.getResource());
    nm1.containerStatus(containerStatus);
    waitCount = 0;
    while (attempt1.getJustFinishedContainers().size() < 1
        && waitCount++ != 20) {
      LOG.info("Waiting for containers to be finished for app 1... Tried "
          + waitCount + " times already..");
      Thread.sleep(100);
    }
    Assert.assertEquals(1, attempt1.getJustFinishedContainers().size());
    Assert.assertEquals(1, am1.schedule().getCompletedContainersStatuses().size());
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemorySize());
    // As container return 2 GB back, the available resource becomes 0 again.
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemorySize());

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

    // Allocate a container
    List<ResourceRequest> asks = Collections.singletonList(
        ResourceRequest.newInstance(
            Priority.newInstance(1), "*", Resources.createResource(2 * GB), 1));
    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(0, 0.0f, asks, null, null);
    client.allocate(allocateRequest);

    // Make sure the container is allocated in RM
    nm1.nodeHeartbeat(true);
    ContainerId containerId2 =
        ContainerId.newContainerId(applicationAttemptId, 2);
    Assert.assertTrue(rm.waitForState(nm1, containerId2,
        RMContainerState.ALLOCATED));

    // Acquire the container
    allocateRequest = AllocateRequest.newInstance(1, 0.0f, null, null, null);
    client.allocate(allocateRequest);

    // Launch the container
    final CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    RMContainer rmContainer = cs.getRMContainer(containerId2);
    rmContainer.handle(
        new RMContainerEvent(containerId2, RMContainerEventType.LAUNCHED));

    // grab the scheduler lock from another thread
    // and verify an allocate call in this thread doesn't block on it
    final CyclicBarrier barrier = new CyclicBarrier(2);
    Thread otherThread = new Thread(new Runnable() {
      @Override
      public void run() {
        synchronized(cs) {
          try {
            barrier.await();
            barrier.await();
          } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
          }
        }
      }
    });
    otherThread.start();
    barrier.await();
    List<ContainerId> release = Collections.singletonList(containerId2);
    allocateRequest =
        AllocateRequest.newInstance(2, 0.0f, null, release, null);
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
    MockAM am0 = MockRM.launchAM(app0, rm1, nm1);
    am0.registerAppAttempt();

    // get scheduler app
    FiCaSchedulerApp schedulerAppAttempt =
        cs.getSchedulerApplications().get(app0.getApplicationId())
            .getCurrentAppAttempt();

    // allocate some containers and launch them
    List<Container> allocatedContainers =
        am0.allocateAndWaitForContainers(3, CONTAINER_MEMORY, nm1);

    // kill the 3 containers
    for (Container c : allocatedContainers) {
      cs.markContainerForKillable(schedulerAppAttempt.getRMContainer(c.getId()));
    }

    // check values
    waitForAppPreemptionInfo(app0,
        Resource.newInstance(CONTAINER_MEMORY * 3, 3), 0, 3,
        Resource.newInstance(CONTAINER_MEMORY * 3, 3), false, 3);

    // kill app0-attempt0 AM container
    cs.markContainerForKillable(schedulerAppAttempt.getRMContainer(app0
        .getCurrentAppAttempt().getMasterContainer().getId()));

    // wait for app0 failed
    waitForNewAttemptCreated(app0, am0.getApplicationAttemptId());

    // check values
    waitForAppPreemptionInfo(app0,
        Resource.newInstance(CONTAINER_MEMORY * 4, 4), 1, 3,
        Resource.newInstance(0, 0), false, 0);

    // launch app0-attempt1
    MockAM am1 = MockRM.launchAM(app0, rm1, nm1);
    am1.registerAppAttempt();

    schedulerAppAttempt =
        cs.getSchedulerApplications().get(app0.getApplicationId())
            .getCurrentAppAttempt();

    // allocate some containers and launch them
    allocatedContainers =
        am1.allocateAndWaitForContainers(3, CONTAINER_MEMORY, nm1);
    for (Container c : allocatedContainers) {
      cs.markContainerForKillable(schedulerAppAttempt.getRMContainer(c.getId()));
    }

    // check values
    waitForAppPreemptionInfo(app0,
        Resource.newInstance(CONTAINER_MEMORY * 7, 7), 1, 6,
        Resource.newInstance(CONTAINER_MEMORY * 3, 3), false, 3);

    rm1.stop();
  }

  @Test(timeout = 300000)
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
      Assert.assertEquals(0,
          app.getOutstandingAsksCount(SchedulerRequestKey.create(request),
              request.getResourceName()));
    }

    // Call killContainer to preempt the container
    cs.markContainerForKillable(rmContainer);

    Assert.assertEquals(3, requests.size());
    for (ResourceRequest request : requests) {
      // Resource request must have added back in RM after preempt event
      // handling.
      Assert.assertEquals(1,
          app.getOutstandingAsksCount(SchedulerRequestKey.create(request),
              request.getResourceName()));
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
    return setUpMove(conf);
  }

  private MockRM setUpMove(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
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
    Assert.assertEquals("a1", queue);

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
    Assert.assertEquals("b1", queue);

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
    Assert.assertEquals("a1", queue);

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
    Assert.assertEquals("a2", queue);

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
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

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
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

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
          RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
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
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

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
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

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
    CapacitySchedulerInfo oldInfo =
        new CapacitySchedulerInfo(origRootQ, cs);
    int origNumAppsA = getNumAppsInQueue("a", origRootQ.getChildQueues());
    int origNumAppsRoot = origRootQ.getNumApplications();

    scheduler.moveApplication(application_0.getApplicationId(), "a2");

    CSQueue newRootQ = cs.getRootQueue();
    int newNumAppsA = getNumAppsInQueue("a", newRootQ.getChildQueues());
    int newNumAppsRoot = newRootQ.getNumApplications();
    CapacitySchedulerInfo newInfo =
        new CapacitySchedulerInfo(newRootQ, cs);
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
    assertEquals(3 * GB, origOldA1.getResourcesUsed().getMemorySize());
    assertEquals(1, origOldA1.getResourcesUsed().getvCores());
    assertEquals(0, origNewA1.getResourcesUsed().getMemorySize()); // after the move
    assertEquals(0, origNewA1.getResourcesUsed().getvCores()); // after the move
    // app moved here with live containers
    assertEquals(3 * GB, targetNewA2.getResourcesUsed().getMemorySize());
    assertEquals(1, targetNewA2.getResourcesUsed().getvCores());
    // it was empty before the move
    assertEquals(0, targetOldA2.getNumApplications());
    assertEquals(0, targetOldA2.getResourcesUsed().getMemorySize());
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
        .getResourcesUsed().getMemorySize());
    // 1 user with 1 core
    assertEquals(1, origOldA1.getUsers().getUsersList().get(0)
        .getResourcesUsed().getvCores());
    // user ha no more running app in the orig queue
    assertEquals(0, origNewA1.getUsers().getUsersList().size());
    // 1 user with 3GB
    assertEquals(3 * GB, targetNewA2.getUsers().getUsersList().get(0)
        .getResourcesUsed().getMemorySize());
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
    Assert.assertEquals("a1", queue);

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
    Assert.assertEquals("b1", queue);

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
    YarnScheduler scheduler = rm.getResourceScheduler();

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
    YarnScheduler scheduler = rm.getResourceScheduler();

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

  @Test(timeout = 60000)
  public void testMoveAttemptNotAdded() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(getCapacityConfiguration(conf));
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);

    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "a1", "user");
    try {
      cs.moveApplication(appId, "b1");
      fail("Move should throw exception app not available");
    } catch (YarnException e) {
      assertEquals("App to be moved application_100_0001 not found.",
          e.getMessage());
    }
    cs.handle(addAppEvent);
    cs.moveApplication(appId, "b1");
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);
    CSQueue rootQ = cs.getRootQueue();
    CSQueue queueB = cs.getQueue("b");
    CSQueue queueA = cs.getQueue("a");
    CSQueue queueA1 = cs.getQueue("a1");
    CSQueue queueB1 = cs.getQueue("b1");
    Assert.assertEquals(1, rootQ.getNumApplications());
    Assert.assertEquals(0, queueA.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB1.getNumApplications());

    rm.close();
  }

  @Test
  public void testRemoveAttemptMoveAdded() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        CapacityScheduler.class);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    // Create Mock RM
    MockRM rm = new MockRM(getCapacityConfiguration(conf));
    CapacityScheduler sch = (CapacityScheduler) rm.getResourceScheduler();
    // add node
    Resource newResource = Resource.newInstance(4 * GB, 1);
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    SchedulerEvent addNode = new NodeAddedSchedulerEvent(node);
    sch.handle(addNode);
    // create appid
    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);
    // Add application
    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "a1", "user");
    sch.handle(addAppEvent);
    // Add application attempt
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    sch.handle(addAttemptEvent);
    // get Queues
    CSQueue queueA1 = sch.getQueue("a1");
    CSQueue queueB = sch.getQueue("b");
    CSQueue queueB1 = sch.getQueue("b1");

    // add Running rm container and simulate live containers to a1
    ContainerId newContainerId = ContainerId.newContainerId(appAttemptId, 2);
    RMContainerImpl rmContainer = mock(RMContainerImpl.class);
    when(rmContainer.getState()).thenReturn(RMContainerState.RUNNING);
    Container container2 = mock(Container.class);
    when(rmContainer.getContainer()).thenReturn(container2);
    Resource resource = Resource.newInstance(1024, 1);
    when(container2.getResource()).thenReturn(resource);
    when(rmContainer.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container2.getNodeId()).thenReturn(node.getNodeID());
    when(container2.getId()).thenReturn(newContainerId);
    when(rmContainer.getNodeLabelExpression())
        .thenReturn(RMNodeLabelsManager.NO_LABEL);
    when(rmContainer.getContainerId()).thenReturn(newContainerId);
    sch.getApplicationAttempt(appAttemptId).getLiveContainersMap()
        .put(newContainerId, rmContainer);
    QueueMetrics queueA1M = queueA1.getMetrics();
    queueA1M.incrPendingResources(rmContainer.getNodeLabelExpression(),
        "user1", 1, resource);
    queueA1M.allocateResources(rmContainer.getNodeLabelExpression(),
        "user1", resource);
    // remove attempt
    sch.handle(new AppAttemptRemovedSchedulerEvent(appAttemptId,
        RMAppAttemptState.KILLED, true));
    // Move application to queue b1
    sch.moveApplication(appId, "b1");
    // Check queue metrics after move
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(0, queueB1.getNumApplications());

    // Release attempt add event
    ApplicationAttemptId appAttemptId2 =
        BuilderUtils.newApplicationAttemptId(appId, 2);
    SchedulerEvent addAttemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, true);
    sch.handle(addAttemptEvent2);

    // Check metrics after attempt added
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(1, queueB1.getNumApplications());


    QueueMetrics queueB1M = queueB1.getMetrics();
    QueueMetrics queueBM = queueB.getMetrics();
    // Verify allocation MB of current state
    Assert.assertEquals(0, queueA1M.getAllocatedMB());
    Assert.assertEquals(0, queueA1M.getAllocatedVirtualCores());
    Assert.assertEquals(1024, queueB1M.getAllocatedMB());
    Assert.assertEquals(1, queueB1M.getAllocatedVirtualCores());

    // remove attempt
    sch.handle(new AppAttemptRemovedSchedulerEvent(appAttemptId2,
        RMAppAttemptState.FINISHED, false));

    Assert.assertEquals(0, queueA1M.getAllocatedMB());
    Assert.assertEquals(0, queueA1M.getAllocatedVirtualCores());
    Assert.assertEquals(0, queueB1M.getAllocatedMB());
    Assert.assertEquals(0, queueB1M.getAllocatedVirtualCores());

    verifyQueueMetrics(queueB1M);
    verifyQueueMetrics(queueBM);
    // Verify queue A1 metrics
    verifyQueueMetrics(queueA1M);
    rm.close();
  }

  private void verifyQueueMetrics(QueueMetrics queue) {
    Assert.assertEquals(0, queue.getPendingMB());
    Assert.assertEquals(0, queue.getActiveUsers());
    Assert.assertEquals(0, queue.getActiveApps());
    Assert.assertEquals(0, queue.getAppsPending());
    Assert.assertEquals(0, queue.getAppsRunning());
    Assert.assertEquals(0, queue.getAllocatedMB());
    Assert.assertEquals(0, queue.getAllocatedVirtualCores());
  }

  private Configuration getCapacityConfiguration(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b"});
    conf.setCapacity(A, 50);
    conf.setCapacity(B, 50);
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 50);
    conf.setCapacity(A2, 50);
    conf.setQueues(B, new String[] {"b1"});
    conf.setCapacity(B1, 100);
    return conf;
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
    Assert.assertEquals("a1", queue);

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // now kill the app
    scheduler.killAllAppsInQueue("a1");

    // check postconditions
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);
    rm.waitForAppRemovedFromScheduler(app.getApplicationId());
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
    YarnScheduler scheduler = rm.getResourceScheduler();

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
    Assert.assertEquals(1 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(9 * GB, report_nm1.getAvailableResource().getMemorySize());

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

  @Test
  public void testPreemptionDisabled() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
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

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueB2 = findQueue(queueB, B2);

    // When preemption turned on for the whole system
    // (yarn.resourcemanager.scheduler.monitor.enable=true), and with no other 
    // preemption properties set, queue root.b.b2 should be preemptable.
    assertFalse("queue " + B2 + " should default to preemptable",
               queueB2.getPreemptionDisabled());

    // Disable preemption at the root queue level.
    // The preemption property should be inherited from root all the
    // way down so that root.b.b2 should NOT be preemptable.
    conf.setPreemptionDisabled(rootQueue.getQueuePath(), true);
    cs.reinitialize(conf, rmContext);
    assertTrue(
        "queue " + B2 + " should have inherited non-preemptability from root",
        queueB2.getPreemptionDisabled());

    // Enable preemption for root (grandparent) but disable for root.b (parent).
    // root.b.b2 should inherit property from parent and NOT be preemptable
    conf.setPreemptionDisabled(rootQueue.getQueuePath(), false);
    conf.setPreemptionDisabled(queueB.getQueuePath(), true);
    cs.reinitialize(conf, rmContext);
    assertTrue(
        "queue " + B2 + " should have inherited non-preemptability from parent",
        queueB2.getPreemptionDisabled());

    // When preemption is turned on for root.b.b2, it should be preemptable
    // even though preemption is disabled on root.b (parent).
    conf.setPreemptionDisabled(queueB2.getQueuePath(), false);
    cs.reinitialize(conf, rmContext);
    assertFalse("queue " + B2 + " should have been preemptable",
        queueB2.getPreemptionDisabled());
  }

  @Test
  public void testRefreshQueuesMaxAllocationRefresh() throws Exception {
    // queue refresh should not allow changing the maximum allocation setting
    // per queue to be smaller than previous setting
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    assertEquals("max allocation in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation for A1",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        conf.getMaximumAllocationPerQueue(A1).getMemorySize());
    assertEquals("max allocation",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        conf.getMaximumAllocation().getMemorySize());

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueA1 = findQueue(queueA, A1);
    assertEquals("queue max allocation", ((LeafQueue) queueA1)
        .getMaximumAllocation().getMemorySize(), 8192);

    setMaxAllocMb(conf, A1, 4096);

    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }

    setMaxAllocMb(conf, A1, 8192);
    cs.reinitialize(conf, mockContext);

    setMaxAllocVcores(conf, A1,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES - 1);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }
  }

  @Test
  public void testRefreshQueuesMaxAllocationPerQueueLarge() throws Exception {
    // verify we can't set the allocation per queue larger then cluster setting
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.init(conf);
    cs.start();
    // change max allocation for B3 queue to be larger then cluster max
    setMaxAllocMb(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 2048);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("maximum allocation exception",
          e.getCause().getMessage().contains("maximum allocation"));
    }

    setMaxAllocMb(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    cs.reinitialize(conf, mockContext);

    setMaxAllocVcores(conf, B3,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES + 1);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("maximum allocation exception",
          e.getCause().getMessage().contains("maximum allocation"));
    }
  }

  @Test
  public void testRefreshQueuesMaxAllocationRefreshLarger() throws Exception {
    // queue refresh should allow max allocation per queue to go larger
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    setMaxAllocMb(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    setMaxAllocVcores(conf,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 2);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    assertEquals("max capability MB in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max capability vcores in CS",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores());
    assertEquals("max allocation MB A1",
        4096,
        conf.getMaximumAllocationPerQueue(A1).getMemorySize());
    assertEquals("max allocation vcores A1",
        2,
        conf.getMaximumAllocationPerQueue(A1).getVirtualCores());
    assertEquals("cluster max allocation MB",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        conf.getMaximumAllocation().getMemorySize());
    assertEquals("cluster max allocation vcores",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        conf.getMaximumAllocation().getVirtualCores());

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueA1 = findQueue(queueA, A1);
    assertEquals("queue max allocation", ((LeafQueue) queueA1)
        .getMaximumAllocation().getMemorySize(), 4096);

    setMaxAllocMb(conf, A1, 6144);
    setMaxAllocVcores(conf, A1, 3);
    cs.reinitialize(conf, null);
    // conf will have changed but we shouldn't be able to change max allocation
    // for the actual queue
    assertEquals("max allocation MB A1", 6144,
        conf.getMaximumAllocationPerQueue(A1).getMemorySize());
    assertEquals("max allocation vcores A1", 3,
        conf.getMaximumAllocationPerQueue(A1).getVirtualCores());
    assertEquals("max allocation MB cluster",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        conf.getMaximumAllocation().getMemorySize());
    assertEquals("max allocation vcores cluster",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        conf.getMaximumAllocation().getVirtualCores());
    assertEquals("queue max allocation MB", 6144,
        ((LeafQueue) queueA1).getMaximumAllocation().getMemorySize());
    assertEquals("queue max allocation vcores", 3,
        ((LeafQueue) queueA1).getMaximumAllocation().getVirtualCores());
    assertEquals("max capability MB cluster",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("cluster max capability vcores",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability().getVirtualCores());
  }

  @Test
  public void testRefreshQueuesMaxAllocationCSError() throws Exception {
    // Try to refresh the cluster level max allocation size to be smaller
    // and it should error out
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    setMaxAllocMb(conf, 10240);
    setMaxAllocVcores(conf, 10);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 4);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    assertEquals("max allocation MB in CS", 10240,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation vcores in CS", 10,
        cs.getMaximumResourceCapability().getVirtualCores());

    setMaxAllocMb(conf, 6144);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }

    setMaxAllocMb(conf, 10240);
    cs.reinitialize(conf, mockContext);

    setMaxAllocVcores(conf, 8);
    try {
      cs.reinitialize(conf, mockContext);
      fail("should have thrown exception");
    } catch (IOException e) {
      assertTrue("max allocation exception",
          e.getCause().toString().contains("not be decreased"));
    }
  }

  @Test
  public void testRefreshQueuesMaxAllocationCSLarger() throws Exception {
    // Try to refresh the cluster level max allocation size to be larger
    // and verify that if there is no setting per queue it uses the
    // cluster level setting.
    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    setMaxAllocMb(conf, 10240);
    setMaxAllocVcores(conf, 10);
    setMaxAllocMb(conf, A1, 4096);
    setMaxAllocVcores(conf, A1, 4);
    cs.init(conf);
    cs.start();
    cs.reinitialize(conf, mockContext);
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    assertEquals("max allocation MB in CS", 10240,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation vcores in CS", 10,
        cs.getMaximumResourceCapability().getVirtualCores());

    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueA1 = findQueue(queueA, A1);
    CSQueue queueA2 = findQueue(queueA, A2);
    CSQueue queueB2 = findQueue(queueB, B2);

    assertEquals("queue A1 max allocation MB", 4096,
        ((LeafQueue) queueA1).getMaximumAllocation().getMemorySize());
    assertEquals("queue A1 max allocation vcores", 4,
        ((LeafQueue) queueA1).getMaximumAllocation().getVirtualCores());
    assertEquals("queue A2 max allocation MB", 10240,
        ((LeafQueue) queueA2).getMaximumAllocation().getMemorySize());
    assertEquals("queue A2 max allocation vcores", 10,
        ((LeafQueue) queueA2).getMaximumAllocation().getVirtualCores());
    assertEquals("queue B2 max allocation MB", 10240,
        ((LeafQueue) queueB2).getMaximumAllocation().getMemorySize());
    assertEquals("queue B2 max allocation vcores", 10,
        ((LeafQueue) queueB2).getMaximumAllocation().getVirtualCores());

    setMaxAllocMb(conf, 12288);
    setMaxAllocVcores(conf, 12);
    cs.reinitialize(conf, null);
    // cluster level setting should change and any queues without
    // per queue setting
    assertEquals("max allocation MB in CS", 12288,
        cs.getMaximumResourceCapability().getMemorySize());
    assertEquals("max allocation vcores in CS", 12,
        cs.getMaximumResourceCapability().getVirtualCores());
    assertEquals("queue A1 max MB allocation", 4096,
        ((LeafQueue) queueA1).getMaximumAllocation().getMemorySize());
    assertEquals("queue A1 max vcores allocation", 4,
        ((LeafQueue) queueA1).getMaximumAllocation().getVirtualCores());
    assertEquals("queue A2 max MB allocation", 12288,
        ((LeafQueue) queueA2).getMaximumAllocation().getMemorySize());
    assertEquals("queue A2 max vcores allocation", 12,
        ((LeafQueue) queueA2).getMaximumAllocation().getVirtualCores());
    assertEquals("queue B2 max MB allocation", 12288,
        ((LeafQueue) queueB2).getMaximumAllocation().getMemorySize());
    assertEquals("queue B2 max vcores allocation", 12,
        ((LeafQueue) queueB2).getMaximumAllocation().getVirtualCores());
  }

  private void waitContainerAllocated(MockAM am, int mem, int nContainer,
      int startContainerId, MockRM rm, MockNM nm) throws Exception {
    for (int cId = startContainerId; cId < startContainerId + nContainer; cId++) {
      am.allocate("*", mem, 1, new ArrayList<ContainerId>());
      ContainerId containerId =
          ContainerId.newContainerId(am.getApplicationAttemptId(), cId);
      Assert.assertTrue(rm.waitForState(nm, containerId,
          RMContainerState.ALLOCATED));
    }
  }

  @Test
  public void testSchedulerKeyGarbageCollection() throws Exception {
    YarnConfiguration conf =
        new YarnConfiguration(new CapacitySchedulerConfiguration());
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);

    MockRM rm = new MockRM(conf);
    rm.start();

    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // All nodes 1 - 4 will be applicable for scheduling.
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    nm4.nodeHeartbeat(true);

    Thread.sleep(1000);

    AllocateResponse allocateResponse = am1.allocate(
        Arrays.asList(
            newResourceRequest(1, 1, ResourceRequest.ANY,
                Resources.createResource(3 * GB), 1, true,
                ExecutionType.GUARANTEED),
            newResourceRequest(2, 2, ResourceRequest.ANY,
                Resources.createResource(3 * GB), 1, true,
                ExecutionType.GUARANTEED),
            newResourceRequest(3, 3, ResourceRequest.ANY,
                Resources.createResource(3 * GB), 1, true,
                ExecutionType.GUARANTEED),
            newResourceRequest(4, 4, ResourceRequest.ANY,
                Resources.createResource(3 * GB), 1, true,
                ExecutionType.GUARANTEED)
        ),
        null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    Assert.assertEquals(0, allocatedContainers.size());

    Collection<SchedulerRequestKey> schedulerKeys =
        ((CapacityScheduler) scheduler).getApplicationAttempt(attemptId)
            .getAppSchedulingInfo().getSchedulerKeys();
    Assert.assertEquals(4, schedulerKeys.size());

    // Get a Node to HB... at which point 1 container should be
    // allocated
    nm1.nodeHeartbeat(true);
    Thread.sleep(200);
    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    allocatedContainers = allocateResponse.getAllocatedContainers();
    Assert.assertEquals(1, allocatedContainers.size());

    // Verify 1 outstanding schedulerKey is removed
    Assert.assertEquals(3, schedulerKeys.size());

    List <ResourceRequest> resReqs =
        ((CapacityScheduler) scheduler).getApplicationAttempt(attemptId)
            .getAppSchedulingInfo().getAllResourceRequests();

    // Verify 1 outstanding schedulerKey is removed from the
    // rrMap as well
    Assert.assertEquals(3, resReqs.size());

    // Verify One more container Allocation on node nm2
    // And ensure the outstanding schedulerKeys go down..
    nm2.nodeHeartbeat(true);
    Thread.sleep(200);

    // Update the allocateReq to send 0 numContainer req.
    // For the satisfied container...
    allocateResponse =  am1.allocate(Arrays.asList(
        newResourceRequest(1,
            allocatedContainers.get(0).getAllocationRequestId(),
            ResourceRequest.ANY,
            Resources.createResource(3 * GB), 0, true,
            ExecutionType.GUARANTEED)
        ),
        new ArrayList<>());
    allocatedContainers = allocateResponse.getAllocatedContainers();
    Assert.assertEquals(1, allocatedContainers.size());

    // Verify 1 outstanding schedulerKey is removed
    Assert.assertEquals(2, schedulerKeys.size());

    resReqs = ((CapacityScheduler) scheduler).getApplicationAttempt(attemptId)
        .getAppSchedulingInfo().getAllResourceRequests();
    // Verify the map size is not increased due to 0 req
    Assert.assertEquals(2, resReqs.size());

    // Now Verify that the AM can cancel 1 Ask:
    SchedulerRequestKey sk = schedulerKeys.iterator().next();
    am1.allocate(
        Arrays.asList(
            newResourceRequest(sk.getPriority().getPriority(),
                sk.getAllocationRequestId(),
                ResourceRequest.ANY, Resources.createResource(3 * GB), 0, true,
                ExecutionType.GUARANTEED)
        ),
        null);

    schedulerKeys =
        ((CapacityScheduler) scheduler).getApplicationAttempt(attemptId)
            .getAppSchedulingInfo().getSchedulerKeys();

    Thread.sleep(200);

    // Verify 1 outstanding schedulerKey is removed because of the
    // cancel ask
    Assert.assertEquals(1, schedulerKeys.size());

    // Now verify that after the next node heartbeat, we allocate
    // the last schedulerKey
    nm3.nodeHeartbeat(true);
    Thread.sleep(200);
    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    allocatedContainers = allocateResponse.getAllocatedContainers();
    Assert.assertEquals(1, allocatedContainers.size());

    // Verify no more outstanding schedulerKeys..
    Assert.assertEquals(0, schedulerKeys.size());
    resReqs =
        ((CapacityScheduler) scheduler).getApplicationAttempt(attemptId)
            .getAppSchedulingInfo().getAllResourceRequests();
    Assert.assertEquals(0, resReqs.size());
  }

  private static ResourceRequest newResourceRequest(int priority,
      long allocReqId, String rName, Resource resource, int numContainers,
      boolean relaxLoc, ExecutionType eType) {
    ResourceRequest rr = ResourceRequest.newInstance(
        Priority.newInstance(priority), rName, resource, numContainers,
        relaxLoc, null, ExecutionTypeRequest.newInstance(eType, true));
    rr.setAllocationRequestId(allocReqId);
    return rr;
  }

  @Test
  public void testHierarchyQueuesCurrentLimits() throws Exception {
    /*
     * Queue tree:
     *          Root
     *        /     \
     *       A       B
     *      / \    / | \
     *     A1 A2  B1 B2 B3
     */
    YarnConfiguration conf =
        new YarnConfiguration(
            setupQueueConfiguration(new CapacitySchedulerConfiguration()));
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 100 * GB, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    waitContainerAllocated(am1, 1 * GB, 1, 2, rm1, nm1);

    // Maximum resoure of b1 is 100 * 0.895 * 0.792 = 71 GB
    // 2 GBs used by am, so it's 71 - 2 = 69G.
    Assert.assertEquals(69 * GB,
        am1.doHeartbeat().getAvailableResources().getMemorySize());

    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "b2");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    // Allocate 5 containers, each one is 8 GB in am2 (40 GB in total)
    waitContainerAllocated(am2, 8 * GB, 5, 2, rm1, nm1);

    // Allocated one more container with 1 GB resource in b1
    waitContainerAllocated(am1, 1 * GB, 1, 3, rm1, nm1);

    // Total is 100 GB, 
    // B2 uses 41 GB (5 * 8GB containers and 1 AM container)
    // B1 uses 3 GB (2 * 1GB containers and 1 AM container)
    // Available is 100 - 41 - 3 = 56 GB
    Assert.assertEquals(56 * GB,
        am1.doHeartbeat().getAvailableResources().getMemorySize());

    // Now we submit app3 to a1 (in higher level hierarchy), to see if headroom
    // of app1 (in queue b1) updated correctly
    RMApp app3 = rm1.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm1);

    // Allocate 3 containers, each one is 8 GB in am3 (24 GB in total)
    waitContainerAllocated(am3, 8 * GB, 3, 2, rm1, nm1);

    // Allocated one more container with 4 GB resource in b1
    waitContainerAllocated(am1, 1 * GB, 1, 4, rm1, nm1);

    // Total is 100 GB, 
    // B2 uses 41 GB (5 * 8GB containers and 1 AM container)
    // B1 uses 4 GB (3 * 1GB containers and 1 AM container)
    // A1 uses 25 GB (3 * 8GB containers and 1 AM container)
    // Available is 100 - 41 - 4 - 25 = 30 GB
    Assert.assertEquals(30 * GB,
        am1.doHeartbeat().getAvailableResources().getMemorySize());
  }

  @Test
  public void testParentQueueMaxCapsAreRespected() throws Exception {
    /*
     * Queue tree:
     *          Root
     *        /     \
     *       A       B
     *      / \
     *     A1 A2 
     */
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    csConf.setCapacity(A, 50);
    csConf.setMaximumCapacity(A, 50);
    csConf.setCapacity(B, 50);

    // Define 2nd-level queues
    csConf.setQueues(A, new String[] {"a1", "a2"});
    csConf.setCapacity(A1, 50);
    csConf.setUserLimitFactor(A1, 100.0f);
    csConf.setCapacity(A2, 50);
    csConf.setUserLimitFactor(A2, 100.0f);
    csConf.setCapacity(B1, B1_CAPACITY);
    csConf.setUserLimitFactor(B1, 100.0f);

    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);

    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 24 * GB, rm1.getResourceTrackerService());
    nm1.registerNode();

    // Launch app1 in a1, resource usage is 1GB (am) + 4GB * 2 = 9GB 
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    waitContainerAllocated(am1, 4 * GB, 2, 2, rm1, nm1);

    // Try to launch app2 in a2, asked 2GB, should success 
    RMApp app2 = rm1.submitApp(2 * GB, "app", "user", null, "a2");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);
    try {
      // Try to allocate a container, a's usage=11G/max=12
      // a1's usage=9G/max=12
      // a2's usage=2G/max=12
      // In this case, if a2 asked 2G, should fail.
      waitContainerAllocated(am2, 2 * GB, 1, 2, rm1, nm1);
    } catch (AssertionError failure) {
      // Expected, return;
      return;
    }
    Assert.fail("Shouldn't successfully allocate containers for am2, "
        + "queue-a's max capacity will be violated if container allocated");
  }

  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }

  @Test
  public void testQueueHierarchyPendingResourceUpdate() throws Exception {
    Configuration conf =
        TestUtils.getConfigurationWithQueueLabels(new Configuration(false));
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);

    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    MockRM rm = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.start();
    MockNM nm1 = // label = x
        new MockNM("h1:1234", 200 * GB, rm.getResourceTrackerService());
    nm1.registerNode();

    MockNM nm2 = // label = ""
        new MockNM("h2:1234", 200 * GB, rm.getResourceTrackerService());
    nm2.registerNode();

    // Launch app1 in queue=a1
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    // Launch app2 in queue=b1  
    RMApp app2 = rm.submitApp(8 * GB, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

    // am1 asks for 8 * 1GB container for no label
    am1.allocate(Arrays.asList(ResourceRequest.newInstance(
        Priority.newInstance(1), "*", Resources.createResource(1 * GB), 8)),
        null);

    checkPendingResource(rm, "a1", 8 * GB, null);
    checkPendingResource(rm, "a", 8 * GB, null);
    checkPendingResource(rm, "root", 8 * GB, null);

    // am2 asks for 8 * 1GB container for no label
    am2.allocate(Arrays.asList(ResourceRequest.newInstance(
        Priority.newInstance(1), "*", Resources.createResource(1 * GB), 8)),
        null);

    checkPendingResource(rm, "a1", 8 * GB, null);
    checkPendingResource(rm, "a", 8 * GB, null);
    checkPendingResource(rm, "b1", 8 * GB, null);
    checkPendingResource(rm, "b", 8 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 16 * GB, null);

    // am2 asks for 8 * 1GB container in another priority for no label
    am2.allocate(Arrays.asList(ResourceRequest.newInstance(
        Priority.newInstance(2), "*", Resources.createResource(1 * GB), 8)),
        null);

    checkPendingResource(rm, "a1", 8 * GB, null);
    checkPendingResource(rm, "a", 8 * GB, null);
    checkPendingResource(rm, "b1", 16 * GB, null);
    checkPendingResource(rm, "b", 16 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 24 * GB, null);

    // am1 asks 4 GB resource instead of 8 * GB for priority=1
    am1.allocate(Arrays.asList(ResourceRequest.newInstance(
        Priority.newInstance(1), "*", Resources.createResource(4 * GB), 1)),
        null);

    checkPendingResource(rm, "a1", 4 * GB, null);
    checkPendingResource(rm, "a", 4 * GB, null);
    checkPendingResource(rm, "b1", 16 * GB, null);
    checkPendingResource(rm, "b", 16 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 20 * GB, null);

    // am1 asks 8 * GB resource which label=x
    am1.allocate(Arrays.asList(ResourceRequest.newInstance(
        Priority.newInstance(2), "*", Resources.createResource(8 * GB), 1,
        true, "x")), null);

    checkPendingResource(rm, "a1", 4 * GB, null);
    checkPendingResource(rm, "a", 4 * GB, null);
    checkPendingResource(rm, "a1", 8 * GB, "x");
    checkPendingResource(rm, "a", 8 * GB, "x");
    checkPendingResource(rm, "b1", 16 * GB, null);
    checkPendingResource(rm, "b", 16 * GB, null);
    // root = a + b
    checkPendingResource(rm, "root", 20 * GB, null);
    checkPendingResource(rm, "root", 8 * GB, "x");

    // some containers allocated for am1, pending resource should decrease
    ContainerId containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    containerId = ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    Assert.assertTrue(rm.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));

    checkPendingResource(rm, "a1", 0 * GB, null);
    checkPendingResource(rm, "a", 0 * GB, null);
    checkPendingResource(rm, "a1", 0 * GB, "x");
    checkPendingResource(rm, "a", 0 * GB, "x");
    // some containers could be allocated for am2 when we allocating containers
    // for am1, just check if pending resource of b1/b/root > 0 
    checkPendingResourceGreaterThanZero(rm, "b1", null);
    checkPendingResourceGreaterThanZero(rm, "b", null);
    // root = a + b
    checkPendingResourceGreaterThanZero(rm, "root", null);
    checkPendingResource(rm, "root", 0 * GB, "x");

    // complete am2, pending resource should be 0 now
    AppAttemptRemovedSchedulerEvent appRemovedEvent =
        new AppAttemptRemovedSchedulerEvent(
          am2.getApplicationAttemptId(), RMAppAttemptState.FINISHED, false);
    rm.getResourceScheduler().handle(appRemovedEvent);

    checkPendingResource(rm, "a1", 0 * GB, null);
    checkPendingResource(rm, "a", 0 * GB, null);
    checkPendingResource(rm, "a1", 0 * GB, "x");
    checkPendingResource(rm, "a", 0 * GB, "x");
    checkPendingResource(rm, "b1", 0 * GB, null);
    checkPendingResource(rm, "b", 0 * GB, null);
    checkPendingResource(rm, "root", 0 * GB, null);
    checkPendingResource(rm, "root", 0 * GB, "x");
  }

  private void checkPendingResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(
        memory,
        queue.getQueueResourceUsage()
            .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
            .getMemorySize());
  }

  private void checkPendingResourceGreaterThanZero(MockRM rm, String queueName,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertTrue(queue.getQueueResourceUsage()
        .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
        .getMemorySize() > 0);
  }

  // Test verifies AM Used resource for LeafQueue when AM ResourceRequest is
  // lesser than minimumAllocation
  @Test(timeout = 30000)
  public void testAMUsedResource() throws Exception {
    MockRM rm = setUpMove();
    rm.registerNode("127.0.0.1:1234", 4 * GB);

    Configuration conf = rm.getConfig();
    int minAllocMb =
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int amMemory = 50;
    assertTrue("AM memory is greater than or equal to minAllocation",
        amMemory < minAllocMb);
    Resource minAllocResource = Resource.newInstance(minAllocMb, 1);
    String queueName = "a1";
    RMApp rmApp = rm.submitApp(amMemory, "app-1", "user_0", null, queueName);

    assertEquals("RMApp does not containes minimum allocation",
        minAllocResource, rmApp.getAMResourceRequests().get(0).getCapability());

    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    LeafQueue queueA =
        (LeafQueue) ((CapacityScheduler) scheduler).getQueue(queueName);
    assertEquals("Minimum Resource for AM is incorrect", minAllocResource,
        queueA.getUser("user_0").getResourceUsage().getAMUsed());
    rm.stop();
  }

  // Verifies headroom passed to ApplicationMaster has been updated in
  // RMAppAttemptMetrics
  @Test
  public void testApplicationHeadRoom() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext = mock(
        ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);

    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "default", "user");
    cs.handle(addAppEvent);
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);

    Allocation allocate =
        cs.allocate(appAttemptId, Collections.<ResourceRequest> emptyList(),
            Collections.<ContainerId> emptyList(), null, null,
            NULL_UPDATE_REQUESTS);

    Assert.assertNotNull(attempt);

    Assert
        .assertEquals(Resource.newInstance(0, 0), allocate.getResourceLimit());
    Assert.assertEquals(Resource.newInstance(0, 0),
        attemptMetric.getApplicationAttemptHeadroom());

    // Add a node to cluster
    Resource newResource = Resource.newInstance(4 * GB, 1);
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    cs.handle(new NodeAddedSchedulerEvent(node));

    allocate =
        cs.allocate(appAttemptId, Collections.<ResourceRequest> emptyList(),
            Collections.<ContainerId> emptyList(), null, null,
            NULL_UPDATE_REQUESTS);

    // All resources should be sent as headroom
    Assert.assertEquals(newResource, allocate.getResourceLimit());
    Assert.assertEquals(newResource,
        attemptMetric.getApplicationAttemptHeadroom());

    rm.stop();
  }


  @Test
  public void testHeadRoomCalculationWithDRC() throws Exception {
    // test with total cluster resource of 20GB memory and 20 vcores.
    // the queue where two apps running has user limit 0.8
    // allocate 10GB memory and 1 vcore to app 1.
    // app 1 should have headroom
    // 20GB*0.8 - 10GB = 6GB memory available and 15 vcores.
    // allocate 1GB memory and 1 vcore to app2.
    // app 2 should have headroom 20GB - 10 - 1 = 1GB memory,
    // and 20*0.8 - 1 = 15 vcores.

    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DominantResourceCalculator.class);

    YarnConfiguration conf = new YarnConfiguration(csconf);
        conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    LeafQueue qb = (LeafQueue)cs.getQueue("default");
    qb.setUserLimitFactor((float)0.8);

    // add app 1
    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId =
    BuilderUtils.newApplicationAttemptId(appId, 1);

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext = mock(
        ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);

    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "default", "user1");
    cs.handle(addAppEvent);
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);

    // add app 2
    ApplicationId appId2 = BuilderUtils.newApplicationId(100, 2);
    ApplicationAttemptId appAttemptId2 =
    BuilderUtils.newApplicationAttemptId(appId2, 1);

    RMAppAttemptMetrics attemptMetric2 =
        new RMAppAttemptMetrics(appAttemptId2, rm.getRMContext());
    RMAppImpl app2 = mock(RMAppImpl.class);
    when(app2.getApplicationId()).thenReturn(appId2);
    RMAppAttemptImpl attempt2 = mock(RMAppAttemptImpl.class);
    when(attempt2.getMasterContainer()).thenReturn(container);
    when(attempt2.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt2.getAppAttemptId()).thenReturn(appAttemptId2);
    when(attempt2.getRMAppAttemptMetrics()).thenReturn(attemptMetric2);
    when(app2.getCurrentAppAttempt()).thenReturn(attempt2);

    rm.getRMContext().getRMApps().put(appId2, app2);
    addAppEvent =
        new AppAddedSchedulerEvent(appId2, "default", "user2");
    cs.handle(addAppEvent);
    addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, false);
    cs.handle(addAttemptEvent);

    // add nodes  to cluster, so cluster have 20GB and 20 vcores
    Resource newResource = Resource.newInstance(10 * GB, 10);
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    cs.handle(new NodeAddedSchedulerEvent(node));

    Resource newResource2 = Resource.newInstance(10 * GB, 10);
    RMNode node2 = MockNodes.newNodeInfo(0, newResource2, 1, "127.0.0.2");
    cs.handle(new NodeAddedSchedulerEvent(node2));

    FiCaSchedulerApp fiCaApp1 =
            cs.getSchedulerApplications().get(app.getApplicationId())
                .getCurrentAppAttempt();

    FiCaSchedulerApp fiCaApp2 =
            cs.getSchedulerApplications().get(app2.getApplicationId())
                .getCurrentAppAttempt();
    Priority u0Priority = TestUtils.createMockPriority(1);
    RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

    // allocate container for app1 with 10GB memory and 1 vcore
    fiCaApp1.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 10*GB, 1, true,
            u0Priority, recordFactory)));
    cs.handle(new NodeUpdateSchedulerEvent(node));
    cs.handle(new NodeUpdateSchedulerEvent(node2));
    assertEquals(6*GB, fiCaApp1.getHeadroom().getMemorySize());
    assertEquals(15, fiCaApp1.getHeadroom().getVirtualCores());

    // allocate container for app2 with 1GB memory and 1 vcore
    fiCaApp2.updateResourceRequests(Collections.singletonList(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true,
            u0Priority, recordFactory)));
    cs.handle(new NodeUpdateSchedulerEvent(node));
    cs.handle(new NodeUpdateSchedulerEvent(node2));
    assertEquals(9*GB, fiCaApp2.getHeadroom().getMemorySize());
    assertEquals(15, fiCaApp2.getHeadroom().getVirtualCores());
  }

  @Test
  public void testDefaultNodeLabelExpressionQueueConfig() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setDefaultNodeLabelExpression("root.a", " x");
    conf.setDefaultNodeLabelExpression("root.b", " y ");
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();

    QueueInfo queueInfoA = cs.getQueueInfo("a", true, false);
    Assert.assertEquals(queueInfoA.getQueueName(), "a");
    Assert.assertEquals(queueInfoA.getDefaultNodeLabelExpression(), "x");

    QueueInfo queueInfoB = cs.getQueueInfo("b", true, false);
    Assert.assertEquals(queueInfoB.getQueueName(), "b");
    Assert.assertEquals(queueInfoB.getDefaultNodeLabelExpression(), "y");
  }

  @Test(timeout = 60000)
  public void testAMLimitUsage() throws Exception {

    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();

    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DefaultResourceCalculator.class.getName());
    verifyAMLimitForLeafQueue(config);

    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    verifyAMLimitForLeafQueue(config);
  }

  private FiCaSchedulerApp getFiCaSchedulerApp(MockRM rm,
      ApplicationId appId) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    return cs.getSchedulerApplications().get(appId).getCurrentAppAttempt();
  }

  @Test
  public void testPendingResourceUpdatedAccordingToIncreaseRequestChanges()
      throws Exception {
    Configuration conf =
        TestUtils.getConfigurationWithQueueLabels(new Configuration(false));
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);

    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);


    MockRM rm = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.start();

    MockNM nm1 = // label = ""
        new MockNM("h1:1234", 200 * GB, rm.getResourceTrackerService());
    nm1.registerNode();

    // Launch app1 in queue=a1
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // Allocate two more containers
    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(2 * GB), 2)),
        null);
    ContainerId containerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    ContainerId containerId3 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    Assert.assertTrue(rm.waitForState(nm1, containerId3,
        RMContainerState.ALLOCATED));
    // Acquire them
    am1.allocate(null, null);
    sentRMContainerLaunched(rm,
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1L));
    sentRMContainerLaunched(rm,
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2L));
    sentRMContainerLaunched(rm,
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3L));

    // am1 asks to change its AM container from 1GB to 3GB
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId1,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(3 * GB), null)));

    FiCaSchedulerApp app = getFiCaSchedulerApp(rm, app1.getApplicationId());

    Assert.assertEquals(2 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    checkPendingResource(rm, "a1", 2 * GB, null);
    checkPendingResource(rm, "a", 2 * GB, null);
    checkPendingResource(rm, "root", 2 * GB, null);

    // am1 asks to change containerId2 (2G -> 3G) and containerId3 (2G -> 5G)
    am1.sendContainerResizingRequest(Arrays.asList(
        UpdateContainerRequest
                .newInstance(0, containerId2,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(3 * GB), null),
        UpdateContainerRequest
                .newInstance(0, containerId3,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(5 * GB), null)));

    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    checkPendingResource(rm, "a1", 6 * GB, null);
    checkPendingResource(rm, "a", 6 * GB, null);
    checkPendingResource(rm, "root", 6 * GB, null);

    // am1 asks to change containerId1 (1G->3G), containerId2 (2G -> 4G) and
    // containerId3 (2G -> 2G)
    am1.sendContainerResizingRequest(Arrays.asList(
        UpdateContainerRequest
                .newInstance(0, containerId1,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(3 * GB), null),
        UpdateContainerRequest
                .newInstance(0, containerId2,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(4 * GB), null),
        UpdateContainerRequest
                .newInstance(0, containerId3,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(2 * GB), null)));
    Assert.assertEquals(4 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    checkPendingResource(rm, "a1", 4 * GB, null);
    checkPendingResource(rm, "a", 4 * GB, null);
    checkPendingResource(rm, "root", 4 * GB, null);
  }

  private void verifyAMLimitForLeafQueue(CapacitySchedulerConfiguration config)
      throws Exception {
    MockRM rm = setUpMove(config);
    int nodeMemory = 4 * GB;
    rm.registerNode("127.0.0.1:1234", nodeMemory);

    String queueName = "a1";
    String userName = "user_0";
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    LeafQueue queueA =
        (LeafQueue) ((CapacityScheduler) scheduler).getQueue(queueName);
    Resource amResourceLimit = queueA.getAMResourceLimit();

    Resource amResource1 =
        Resource.newInstance(amResourceLimit.getMemorySize() + 1024,
            amResourceLimit.getVirtualCores() + 1);
    Resource amResource2 =
        Resource.newInstance(amResourceLimit.getMemorySize() + 2048,
            amResourceLimit.getVirtualCores() + 1);

    // Wait for the scheduler to be updated with new node capacity
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return scheduler.getMaximumResourceCapability().getMemorySize() == nodeMemory;
        }
      }, 100, 60 * 1000);

    rm.submitApp(amResource1, "app-1", userName, null, queueName);

    rm.submitApp(amResource2, "app-2", userName, null, queueName);

    // When AM limit is exceeded, 1 applications will be activated.Rest all
    // applications will be in pending
    Assert.assertEquals("PendingApplications should be 1", 1,
        queueA.getNumPendingApplications());
    Assert.assertEquals("Active applications should be 1", 1,
        queueA.getNumActiveApplications());

    Assert.assertEquals("User PendingApplications should be 1", 1, queueA
        .getUser(userName).getPendingApplications());
    Assert.assertEquals("User Active applications should be 1", 1, queueA
        .getUser(userName).getActiveApplications());
    rm.stop();
  }

  private void setMaxAllocMb(Configuration conf, int maxAllocMb) {
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        maxAllocMb);
  }

  private void setMaxAllocMb(CapacitySchedulerConfiguration conf,
      String queueName, int maxAllocMb) {
    String propName = CapacitySchedulerConfiguration.getQueuePrefix(queueName)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB;
    conf.setInt(propName, maxAllocMb);
  }

  private void setMaxAllocVcores(Configuration conf, int maxAllocVcores) {
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        maxAllocVcores);
  }

  private void setMaxAllocVcores(CapacitySchedulerConfiguration conf,
      String queueName, int maxAllocVcores) {
    String propName = CapacitySchedulerConfiguration.getQueuePrefix(queueName)
        + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES;
    conf.setInt(propName, maxAllocVcores);
  }

  private void sentRMContainerLaunched(MockRM rm, ContainerId containerId) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    RMContainer rmContainer = cs.getRMContainer(containerId);
    if (rmContainer != null) {
      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED));
    } else {
      Assert.fail("Cannot find RMContainer");
    }
  }

  @Test
  public void testResourceUpdateDecommissioningNode() throws Exception {
    // Mock the RMNodeResourceUpdate event handler to update SchedulerNode
    // to have 0 available resource
    RMContext spyContext = Mockito.spy(resourceManager.getRMContext());
    Dispatcher mockDispatcher = mock(AsyncDispatcher.class);
    when(mockDispatcher.getEventHandler()).thenReturn(new EventHandler<Event>() {
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
    ((CapacityScheduler) resourceManager.getResourceScheduler())
        .setRMContext(spyContext);
    ((AsyncDispatcher) mockDispatcher).start();
    // Register node
    String host_0 = "host_0";
    org.apache.hadoop.yarn.server.resourcemanager.NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(8 * GB, 4));
    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);

    // Submit an application
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit();

    application_0.addNodeManager(host_0, 1234, nm_0);

    Resource capability_0_0 = Resources.createResource(1 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_0);

    Task task_0_0 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_0);

    // Send resource requests to the scheduler
    application_0.schedule();

    nodeUpdate(nm_0);
    // Kick off another heartbeat with the node state mocked to decommissioning
    // This should update the schedulernodes to have 0 available resource
    RMNode spyNode =
        Mockito.spy(resourceManager.getRMContext().getRMNodes()
            .get(nm_0.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));

    // Get allocations from the scheduler
    application_0.schedule();

    // Check the used resource is 1 GB 1 core
    Assert.assertEquals(1 * GB, nm_0.getUsed().getMemorySize());
    Resource usedResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getAllocatedResource();
    Assert.assertEquals(usedResource.getMemorySize(), 1 * GB);
    Assert.assertEquals(usedResource.getVirtualCores(), 1);
    // Check total resource of scheduler node is also changed to 1 GB 1 core
    Resource totalResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getTotalResource();
    Assert.assertEquals(totalResource.getMemorySize(), 1 * GB);
    Assert.assertEquals(totalResource.getVirtualCores(), 1);
    // Check the available resource is 0/0
    Resource availableResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getUnallocatedResource();
    Assert.assertEquals(availableResource.getMemorySize(), 0);
    Assert.assertEquals(availableResource.getVirtualCores(), 0);
  }

  @Test
  public void testSchedulingOnRemovedNode() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            false);

    MockRM rm = new MockRM(conf);
    rm.start();
    RMApp app = rm.submitApp(100);
    rm.drainEvents();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10240, 10);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);

    //remove nm2 to keep am alive
    MockNM nm2 = rm.registerNode("127.0.0.1:1235", 10240, 10);

    am.allocate(ResourceRequest.ANY, 2048, 1, null);

    CapacityScheduler scheduler =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    FiCaSchedulerNode node =
        (FiCaSchedulerNode)
            scheduler.getNodeTracker().getNode(nm2.getNodeId());
    scheduler.handle(new NodeRemovedSchedulerEvent(
        rm.getRMContext().getRMNodes().get(nm2.getNodeId())));
    // schedulerNode is removed, try allocate a container
    scheduler.allocateContainersToNode(new SimpleCandidateNodeSet<>(node),
        true);

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(
            am.getApplicationAttemptId(),
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
    rm.stop();
  }

  @Test
  public void testCSReservationWithRootUnblocked() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setResourceComparator(DominantResourceCalculator.class);
    setupOtherBlockedQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    ParentQueue q = (ParentQueue) cs.getQueue("p1");

    Assert.assertNotNull(q);
    String host = "127.0.0.1";
    String host1 = "test";
    RMNode node =
        MockNodes.newNodeInfo(0, Resource.newInstance(8 * GB, 8), 1, host);
    RMNode node1 =
        MockNodes.newNodeInfo(0, Resource.newInstance(8 * GB, 8), 2, host1);
    cs.handle(new NodeAddedSchedulerEvent(node));
    cs.handle(new NodeAddedSchedulerEvent(node1));
    ApplicationAttemptId appAttemptId1 =
        appHelper(rm, cs, 100, 1, "x1", "userX1");
    ApplicationAttemptId appAttemptId2 =
        appHelper(rm, cs, 100, 2, "x2", "userX2");
    ApplicationAttemptId appAttemptId3 =
        appHelper(rm, cs, 100, 3, "y1", "userY1");
    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(null);

    Priority priority = TestUtils.createMockPriority(1);
    ResourceRequest y1Req = null;
    ResourceRequest x1Req = null;
    ResourceRequest x2Req = null;
    for(int i=0; i < 4; i++) {
      y1Req = TestUtils.createResourceRequest(
          ResourceRequest.ANY, 1 * GB, 1, true, priority, recordFactory);
      cs.allocate(appAttemptId3,
          Collections.<ResourceRequest>singletonList(y1Req),
          Collections.<ContainerId>emptyList(),
          null, null, NULL_UPDATE_REQUESTS);
      CapacityScheduler.schedule(cs);
    }
    assertEquals("Y1 Used Resource should be 4 GB", 4 * GB,
        cs.getQueue("y1").getUsedResources().getMemorySize());
    assertEquals("P2 Used Resource should be 4 GB", 4 * GB,
        cs.getQueue("p2").getUsedResources().getMemorySize());

    for(int i=0; i < 7; i++) {
      x1Req = TestUtils.createResourceRequest(
          ResourceRequest.ANY, 1 * GB, 1, true, priority, recordFactory);
      cs.allocate(appAttemptId1,
          Collections.<ResourceRequest>singletonList(x1Req),
          Collections.<ContainerId>emptyList(),
          null, null, NULL_UPDATE_REQUESTS);
      CapacityScheduler.schedule(cs);
    }
    assertEquals("X1 Used Resource should be 7 GB", 7 * GB,
        cs.getQueue("x1").getUsedResources().getMemorySize());
    assertEquals("P1 Used Resource should be 7 GB", 7 * GB,
        cs.getQueue("p1").getUsedResources().getMemorySize());

    x2Req = TestUtils.createResourceRequest(
        ResourceRequest.ANY, 2 * GB, 1, true, priority, recordFactory);
    cs.allocate(appAttemptId2,
        Collections.<ResourceRequest>singletonList(x2Req),
        Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS);
    CapacityScheduler.schedule(cs);
    assertEquals("X2 Used Resource should be 0", 0,
        cs.getQueue("x2").getUsedResources().getMemorySize());
    assertEquals("P1 Used Resource should be 7 GB", 7 * GB,
        cs.getQueue("p1").getUsedResources().getMemorySize());
    //this assign should fail
    x1Req = TestUtils.createResourceRequest(
        ResourceRequest.ANY, 1 * GB, 1, true, priority, recordFactory);
    cs.allocate(appAttemptId1,
        Collections.<ResourceRequest>singletonList(x1Req),
        Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS);
    CapacityScheduler.schedule(cs);
    assertEquals("X1 Used Resource should be 7 GB", 7 * GB,
        cs.getQueue("x1").getUsedResources().getMemorySize());
    assertEquals("P1 Used Resource should be 7 GB", 7 * GB,
        cs.getQueue("p1").getUsedResources().getMemorySize());

    //this should get thru
    for (int i=0; i < 4; i++) {
      y1Req = TestUtils.createResourceRequest(
          ResourceRequest.ANY, 1 * GB, 1, true, priority, recordFactory);
      cs.allocate(appAttemptId3,
          Collections.<ResourceRequest>singletonList(y1Req),
          Collections.<ContainerId>emptyList(),
          null, null, NULL_UPDATE_REQUESTS);
      CapacityScheduler.schedule(cs);
    }
    assertEquals("P2 Used Resource should be 8 GB", 8 * GB,
        cs.getQueue("p2").getUsedResources().getMemorySize());

    //Free a container from X1
    ContainerId containerId = ContainerId.newContainerId(appAttemptId1, 2);
    cs.handle(new ContainerExpiredSchedulerEvent(containerId));

    //Schedule pending request
    CapacityScheduler.schedule(cs);
    assertEquals("X2 Used Resource should be 2 GB", 2 * GB,
        cs.getQueue("x2").getUsedResources().getMemorySize());
    assertEquals("P1 Used Resource should be 8 GB", 8 * GB,
        cs.getQueue("p1").getUsedResources().getMemorySize());
    assertEquals("P2 Used Resource should be 8 GB", 8 * GB,
        cs.getQueue("p2").getUsedResources().getMemorySize());
    assertEquals("Root Used Resource should be 16 GB", 16 * GB,
        cs.getRootQueue().getUsedResources().getMemorySize());
    rm.stop();
  }

  @Test
  public void testCSQueueBlocked() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupBlockedQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    LeafQueue q = (LeafQueue) cs.getQueue("a");

    Assert.assertNotNull(q);
    String host = "127.0.0.1";
    String host1 = "test";
    RMNode node =
        MockNodes.newNodeInfo(0, Resource.newInstance(8 * GB, 8), 1, host);
    RMNode node1 =
        MockNodes.newNodeInfo(0, Resource.newInstance(8 * GB, 8), 2, host1);
    cs.handle(new NodeAddedSchedulerEvent(node));
    cs.handle(new NodeAddedSchedulerEvent(node1));
    //add app begin
    ApplicationAttemptId appAttemptId1 =
        appHelper(rm, cs, 100, 1, "a", "user1");
    ApplicationAttemptId appAttemptId2 =
        appHelper(rm, cs, 100, 2, "b", "user2");
    //add app end

    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(null);

    Priority priority = TestUtils.createMockPriority(1);
    ResourceRequest r1 = TestUtils.createResourceRequest(
        ResourceRequest.ANY, 2 * GB, 1, true, priority, recordFactory);
    //This will allocate for app1
    cs.allocate(appAttemptId1, Collections.<ResourceRequest>singletonList(r1),
        Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS).getContainers().size();
    CapacityScheduler.schedule(cs);
    ResourceRequest r2 = null;
    for (int i =0; i < 13; i++) {
      r2 = TestUtils.createResourceRequest(
          ResourceRequest.ANY, 1 * GB, 1, true, priority, recordFactory);
      cs.allocate(appAttemptId2,
          Collections.<ResourceRequest>singletonList(r2),
          Collections.<ContainerId>emptyList(),
          null, null, NULL_UPDATE_REQUESTS);
      CapacityScheduler.schedule(cs);
    }
    assertEquals("A Used Resource should be 2 GB", 2 * GB,
        cs.getQueue("a").getUsedResources().getMemorySize());
    assertEquals("B Used Resource should be 13 GB", 13 * GB,
        cs.getQueue("b").getUsedResources().getMemorySize());
    r1 = TestUtils.createResourceRequest(
        ResourceRequest.ANY, 2 * GB, 1, true, priority, recordFactory);
    r2 = TestUtils.createResourceRequest(
        ResourceRequest.ANY, 1 * GB, 1, true, priority, recordFactory);
    cs.allocate(appAttemptId1, Collections.<ResourceRequest>singletonList(r1),
        Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS).getContainers().size();
    CapacityScheduler.schedule(cs);

    cs.allocate(appAttemptId2, Collections.<ResourceRequest>singletonList(r2),
        Collections.<ContainerId>emptyList(), null, null, NULL_UPDATE_REQUESTS);
    CapacityScheduler.schedule(cs);
    //Check blocked Resource
    assertEquals("A Used Resource should be 2 GB", 2 * GB,
        cs.getQueue("a").getUsedResources().getMemorySize());
    assertEquals("B Used Resource should be 13 GB", 13 * GB,
        cs.getQueue("b").getUsedResources().getMemorySize());

    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId2, 10);
    ContainerId containerId2 =ContainerId.newContainerId(appAttemptId2, 11);

    cs.handle(new ContainerExpiredSchedulerEvent(containerId1));
    rm.drainEvents();
    CapacityScheduler.schedule(cs);

    cs.handle(new ContainerExpiredSchedulerEvent(containerId2));
    CapacityScheduler.schedule(cs);
    rm.drainEvents();

    assertEquals("A Used Resource should be 4 GB", 4 * GB,
        cs.getQueue("a").getUsedResources().getMemorySize());
    assertEquals("B Used Resource should be 12 GB", 12 * GB,
        cs.getQueue("b").getUsedResources().getMemorySize());
    assertEquals("Used Resource on Root should be 16 GB", 16 * GB,
        cs.getRootQueue().getUsedResources().getMemorySize());
    rm.stop();
  }

  private ApplicationAttemptId appHelper(MockRM rm, CapacityScheduler cs,
                                         int clusterTs, int appId, String queue,
                                         String user) {
    ApplicationId appId1 = BuilderUtils.newApplicationId(clusterTs, appId);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, appId);

    RMAppAttemptMetrics attemptMetric1 =
        new RMAppAttemptMetrics(appAttemptId1, rm.getRMContext());
    RMAppImpl app1 = mock(RMAppImpl.class);
    when(app1.getApplicationId()).thenReturn(appId1);
    RMAppAttemptImpl attempt1 = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt1.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext = mock(
        ApplicationSubmissionContext.class);
    when(attempt1.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt1.getAppAttemptId()).thenReturn(appAttemptId1);
    when(attempt1.getRMAppAttemptMetrics()).thenReturn(attemptMetric1);
    when(app1.getCurrentAppAttempt()).thenReturn(attempt1);
    rm.getRMContext().getRMApps().put(appId1, app1);

    SchedulerEvent addAppEvent1 =
        new AppAddedSchedulerEvent(appId1, queue, user);
    cs.handle(addAppEvent1);
    SchedulerEvent addAttemptEvent1 =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    cs.handle(addAttemptEvent1);
    return appAttemptId1;
  }

  @Test
  public void testAppAttemptLocalityStatistics() throws Exception {
    Configuration conf =
        TestUtils.getConfigurationWithMultipleQueues(new Configuration(false));
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);

    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);

    MockRM rm = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.start();
    MockNM nm1 =
        new MockNM("h1:1234", 200 * GB, rm.getResourceTrackerService());
    nm1.registerNode();

    // Launch app1 in queue=a1
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a");

    // Got one offswitch request and offswitch allocation
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // am1 asks for 1 GB resource on h1/default-rack/offswitch
    am1.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 2), ResourceRequest
        .newInstance(Priority.newInstance(1), "/default-rack",
            Resources.createResource(1 * GB), 2), ResourceRequest
        .newInstance(Priority.newInstance(1), "h1",
            Resources.createResource(1 * GB), 1)), null);

    CapacityScheduler cs = (CapacityScheduler) rm.getRMContext().getScheduler();

    // Got one nodelocal request and nodelocal allocation
    cs.nodeUpdate(rm.getRMContext().getRMNodes().get(nm1.getNodeId()));

    // Got one nodelocal request and racklocal allocation
    cs.nodeUpdate(rm.getRMContext().getRMNodes().get(nm1.getNodeId()));

    RMAppAttemptMetrics attemptMetrics = rm.getRMContext().getRMApps().get(
        app1.getApplicationId()).getCurrentAppAttempt()
        .getRMAppAttemptMetrics();

    // We should get one node-local allocation, one rack-local allocation
    // And one off-switch allocation
    Assert.assertArrayEquals(new int[][] { { 1, 0, 0 }, { 0, 1, 0 }, { 0, 0, 1 } },
        attemptMetrics.getLocalityStatistics());
  }

  /**
   * Test for queue deletion.
   * @throws Exception
   */
  @Test
  public void testRefreshQueuesWithQueueDelete() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
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

    // test delete leaf queue when there is application running.
    Map<String, CSQueue> queues =
        cs.getCapacitySchedulerQueueManager().getQueues();
    String b1QTobeDeleted = "b1";
    LeafQueue csB1Queue = Mockito.spy((LeafQueue) queues.get(b1QTobeDeleted));
    when(csB1Queue.getState()).thenReturn(QueueState.DRAINING)
        .thenReturn(QueueState.STOPPED);
    queues.put(b1QTobeDeleted, csB1Queue);
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithOutB1(conf);
    try {
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to delete a"
          + " queue with running apps");
    } catch (IOException e) {
      // ignore
    }

    // test delete leaf queue(root.b.b1) when there is no application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithOutB1(conf);
    try {
      cs.reinitialize(conf, mockContext);
    } catch (IOException e) {
      LOG.error(
          "Expected to NOT throw exception when refresh queue tries to delete"
              + " a queue WITHOUT running apps",
          e);
      fail("Expected to NOT throw exception when refresh queue tries to delete"
          + " a queue WITHOUT running apps");
    }
    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueB3 = findQueue(queueB, B1);
    assertNull("Refresh needs to support delete of leaf queue ", queueB3);

    // reset back to default configuration for testing parent queue delete
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    cs.reinitialize(conf, rmContext);
    checkQueueCapacities(cs, A_CAPACITY, B_CAPACITY);

    // set the configurations such that it fails once but should be successfull
    // next time
    queues = cs.getCapacitySchedulerQueueManager().getQueues();
    CSQueue bQueue = Mockito.spy((ParentQueue) queues.get("b"));
    when(bQueue.getState()).thenReturn(QueueState.DRAINING)
        .thenReturn(QueueState.STOPPED);
    queues.put("b", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b1"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    queues.put("b1", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b2"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    queues.put("b2", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b3"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    queues.put("b3", bQueue);

    // test delete Parent queue when there is application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithOutB(conf);
    try {
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to delete a"
          + " parent queue with running apps in children queue");
    } catch (IOException e) {
      // ignore
    }

    // test delete Parent queue when there is no application running.
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithOutB(conf);
    try {
      cs.reinitialize(conf, mockContext);
    } catch (IOException e) {
      fail("Expected to not throw exception when refresh queue tries to delete"
          + " a queue without running apps");
    }
    rootQueue = cs.getRootQueue();
    queueB = findQueue(rootQueue, B);
    String message =
        "Refresh needs to support delete of Parent queue and its children.";
    assertNull(message, queueB);
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b1"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b2"));

    cs.stop();
  }

  /**
   * Test for all child queue deletion and thus making parent queue a child.
   * @throws Exception
   */
  @Test
  public void testRefreshQueuesWithAllChildQueuesDeleted() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
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

    // test delete all leaf queues when there is no application running.
    Map<String, CSQueue> queues =
        cs.getCapacitySchedulerQueueManager().getQueues();

    CSQueue bQueue = Mockito.spy((LeafQueue) queues.get("b1"));
    when(bQueue.getState()).thenReturn(QueueState.RUNNING)
        .thenReturn(QueueState.STOPPED);
    queues.put("b1", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b2"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    queues.put("b2", bQueue);

    bQueue = Mockito.spy((LeafQueue) queues.get("b3"));
    when(bQueue.getState()).thenReturn(QueueState.STOPPED);
    queues.put("b3", bQueue);

    conf = new CapacitySchedulerConfiguration();
    setupQueueConfWithOutChildrenOfB(conf);

    // test convert parent queue to leaf queue(root.b) when there is no
    // application running.
    try {
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to make parent"
          + " queue a child queue when one of its children is still running.");
    } catch (IOException e) {
      //do not do anything, expected exception
    }

    // test delete leaf queues(root.b.b1,b2,b3) when there is no application
    // running.
    try {
      cs.reinitialize(conf, mockContext);
    } catch (IOException e) {
      e.printStackTrace();
      fail("Expected to NOT throw exception when refresh queue tries to delete"
          + " all children of a parent queue(without running apps).");
    }
    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueB = findQueue(rootQueue, B);
    assertNotNull("Parent Queue B should not be deleted", queueB);
    Assert.assertTrue("As Queue'B children are not deleted",
        queueB instanceof LeafQueue);

    String message =
        "Refresh needs to support delete of all children of Parent queue.";
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b3"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b1"));
    assertNull(message,
        cs.getCapacitySchedulerQueueManager().getQueues().get("b2"));

    cs.stop();
  }

  /**
   * Test if we can convert a leaf queue to a parent queue
   * @throws Exception
   */
  @Test (timeout = 10000)
  public void testConvertLeafQueueToParentQueue() throws Exception {
    CapacityScheduler cs = new CapacityScheduler();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
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

    String targetQueue = "b1";
    CSQueue b1 = cs.getQueue(targetQueue);
    Assert.assertEquals(b1.getState(), QueueState.RUNNING);

    // test if we can convert a leaf queue which is in RUNNING state
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithB1AsParentQueue(conf);
    try {
      cs.reinitialize(conf, mockContext);
      fail("Expected to throw exception when refresh queue tries to convert"
          + " a child queue to a parent queue.");
    } catch (IOException e) {
      // ignore
    }

    // now set queue state for b1 to STOPPED
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.set("yarn.scheduler.capacity.root.b.b1.state", "STOPPED");
    cs.reinitialize(conf, mockContext);
    Assert.assertEquals(b1.getState(), QueueState.STOPPED);

    // test if we can convert a leaf queue which is in STOPPED state
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithB1AsParentQueue(conf);
    try {
      cs.reinitialize(conf, mockContext);
    } catch (IOException e) {
      fail("Expected to NOT throw exception when refresh queue tries"
          + " to convert a leaf queue WITHOUT running apps");
    }
    b1 = cs.getQueue(targetQueue);
    Assert.assertTrue(b1 instanceof ParentQueue);
    Assert.assertEquals(b1.getState(), QueueState.RUNNING);
    Assert.assertTrue(!b1.getChildQueues().isEmpty());
  }

  @Test(timeout = 30000)
  public void testAMLimitDouble() throws Exception {
    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();
    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
    conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
    MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("127.0.0.1:1234", 10 * GB);
    rm.registerNode("127.0.0.1:1235", 10 * GB);
    rm.registerNode("127.0.0.1:1236", 10 * GB);
    rm.registerNode("127.0.0.1:1237", 10 * GB);
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);
    LeafQueue queueA =
        (LeafQueue) ((CapacityScheduler) scheduler).getQueue("default");
    Resource amResourceLimit = queueA.getAMResourceLimit();
    Assert.assertEquals(4096, amResourceLimit.getMemorySize());
    Assert.assertEquals(4, amResourceLimit.getVirtualCores());
    rm.stop();
  }

  @Test(timeout = 30000)
  public void testcheckAndGetApplicationLifetime() throws Exception {
    long maxLifetime = 10;
    long defaultLifetime = 5;
    // positive integer value
    CapacityScheduler cs = setUpCSQueue(maxLifetime, defaultLifetime);
    Assert.assertEquals(maxLifetime,
        cs.checkAndGetApplicationLifetime("default", 100));
    Assert.assertEquals(9, cs.checkAndGetApplicationLifetime("default", 9));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", -1));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", 0));
    Assert.assertEquals(maxLifetime,
        cs.getMaximumApplicationLifetime("default"));

    maxLifetime = -1;
    defaultLifetime = -1;
    // test for default values
    cs = setUpCSQueue(maxLifetime, defaultLifetime);
    Assert.assertEquals(100, cs.checkAndGetApplicationLifetime("default", 100));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", -1));
    Assert.assertEquals(0, cs.checkAndGetApplicationLifetime("default", 0));
    Assert.assertEquals(maxLifetime,
        cs.getMaximumApplicationLifetime("default"));

    maxLifetime = 10;
    defaultLifetime = 10;
    cs = setUpCSQueue(maxLifetime, defaultLifetime);
    Assert.assertEquals(maxLifetime,
        cs.checkAndGetApplicationLifetime("default", 100));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", -1));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", 0));
    Assert.assertEquals(maxLifetime,
        cs.getMaximumApplicationLifetime("default"));

    maxLifetime = 0;
    defaultLifetime = 0;
    cs = setUpCSQueue(maxLifetime, defaultLifetime);
    Assert.assertEquals(100, cs.checkAndGetApplicationLifetime("default", 100));
    Assert.assertEquals(-1, cs.checkAndGetApplicationLifetime("default", -1));
    Assert.assertEquals(0, cs.checkAndGetApplicationLifetime("default", 0));

    maxLifetime = 10;
    defaultLifetime = -1;
    cs = setUpCSQueue(maxLifetime, defaultLifetime);
    Assert.assertEquals(maxLifetime,
        cs.checkAndGetApplicationLifetime("default", 100));
    Assert.assertEquals(maxLifetime,
        cs.checkAndGetApplicationLifetime("default", -1));
    Assert.assertEquals(maxLifetime,
        cs.checkAndGetApplicationLifetime("default", 0));

    maxLifetime = 5;
    defaultLifetime = 10;
    try {
      setUpCSQueue(maxLifetime, defaultLifetime);
      Assert.fail("Expected to fails since maxLifetime < defaultLifetime.");
    } catch (YarnRuntimeException ye) {
      Assert.assertTrue(
          ye.getMessage().contains("can't exceed maximum lifetime"));
    }
  }

  private CapacityScheduler setUpCSQueue(long maxLifetime,
      long defaultLifetime) {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"default"});
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".default", 100);
    csConf.setMaximumLifetimePerQueue(
        CapacitySchedulerConfiguration.ROOT + ".default", maxLifetime);
    csConf.setDefaultLifetimePerQueue(
        CapacitySchedulerConfiguration.ROOT + ".default", defaultLifetime);

    YarnConfiguration conf = new YarnConfiguration(csConf);
    CapacityScheduler cs = new CapacityScheduler();

    RMContext rmContext = TestUtils.getMockRMContext();
    cs.setConf(conf);
    cs.setRMContext(rmContext);
    cs.init(conf);

    return cs;
  }

  private void waitforNMRegistered(ResourceScheduler scheduler, int nodecount,
      int timesec) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timesec * 1000) {
      if (scheduler.getNumClusterNodes() < nodecount) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
  }
}
