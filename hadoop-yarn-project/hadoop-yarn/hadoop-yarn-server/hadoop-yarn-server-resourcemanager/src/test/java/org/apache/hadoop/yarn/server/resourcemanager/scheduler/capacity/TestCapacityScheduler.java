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

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.setMaxAllocMb;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.setMaxAllocVcores;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.setMinAllocMb;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.setMinAllocVcores;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.findQueue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupAdditionalQueues;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupBlockedQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupOtherBlockedQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B1_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.appHelper;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.checkApplicationResourceUsage;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.checkNodeResourceUsage;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.checkPendingResource;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.checkPendingResourceGreaterThanZero;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createMockRMContext;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.nodeUpdate;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.registerNode;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.setUpMove;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.stopResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.toSet;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.waitforNMRegistered;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerOvercommit.assertContainerKilled;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerOvercommit.assertMemory;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerOvercommit.assertNoPreemption;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerOvercommit.assertPreemption;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerOvercommit.assertTime;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerOvercommit.updateNodeResource;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerOvercommit.waitMemory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.TestGroupsCaching;
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
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.CSQueueMetricsForCustomResources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestQueueMetricsForCustomResources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.ContainerAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.IteratorSelector;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestCapacityScheduler {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCapacityScheduler.class);
  private final static ContainerUpdates NULL_UPDATE_REQUESTS =
      new ContainerUpdates();
  private ResourceManager resourceManager = null;
  private RMContext mockContext;

  private static final double DELTA = 0.0001;

  @Before
  public void setUp() throws Exception {
    resourceManager = createResourceManager();
    mockContext = createMockRMContext();
  }

  @After
  public void tearDown() throws Exception {
    stopResourceManager(resourceManager);
  }

  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
    CapacityScheduler scheduler = new CapacityScheduler();
    scheduler.setRMContext(resourceManager.getRMContext());
    Configuration conf = new YarnConfiguration();

    setMinAllocMb(conf, 2048);
    setMaxAllocMb(conf, 1024);
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
    setMinAllocVcores(conf, 2);
    setMaxAllocVcores(conf, 1);
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

  @Test
  public void testCapacityScheduler() throws Exception {

    LOG.info("--- START: testCapacityScheduler ---");

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(resourceManager, host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(4 * GB, 1), mockNodeStatus);

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(resourceManager, host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(2 * GB, 1), mockNodeStatus);

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
    nodeUpdate(resourceManager, nm_0);

    // nothing allocated
    nodeUpdate(resourceManager, nm_1);

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
    nodeUpdate(resourceManager, nm_0);

    LOG.info("Sending hb from " + nm_1.getHostName());
    // task_0_1 is prefer as locality, used=2G
    nodeUpdate(resourceManager, nm_1);

    // Get allocations from the scheduler
    LOG.info("Trying to allocate...");
    application_0.schedule();
    checkApplicationResourceUsage(1 * GB, application_0);

    application_1.schedule();
    checkApplicationResourceUsage(5 * GB, application_1);

    nodeUpdate(resourceManager, nm_0);
    nodeUpdate(resourceManager, nm_1);

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

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(rm, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
        Resources.createResource(10 * GB, 10), mockNodeStatus);

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
    LOG.info("--- END: testNotAssignMultiple ---");
    rm.stop();
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

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host0 = "host_0";
    NodeManager nm0 =
        registerNode(rm, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
        Resources.createResource(10 * GB, 10), mockNodeStatus);

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
    LOG.info("--- END: testAssignMultiple ---");
    rm.stop();
  }

  @Test
  public void testMaximumCapacitySetup() {
    float delta = 0.0000001f;
    QueuePath queuePathA = new QueuePath(A);
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    assertEquals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE,
            conf.getNonLabeledQueueMaximumCapacity(queuePathA), delta);
    conf.setMaximumCapacity(A, 50.0f);
    assertEquals(50.0f, conf.getNonLabeledQueueMaximumCapacity(queuePathA), delta);
    conf.setMaximumCapacity(A, -1);
    assertEquals(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE,
            conf.getNonLabeledQueueMaximumCapacity(queuePathA), delta);
  }

  @Test
  public void testQueueMaximumAllocations() {
    CapacityScheduler scheduler = new CapacityScheduler();
    scheduler.setConf(new YarnConfiguration());
    scheduler.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    setupQueueConfiguration(conf);
    setMaxAllocMb(conf, A1, 1024);
    setMaxAllocVcores(conf, A1, 1);

    scheduler.init(conf);
    scheduler.start();

    Resource maxAllocationForQueue =
        scheduler.getMaximumResourceCapability("a1");
    Resource maxAllocation1 = scheduler.getMaximumResourceCapability("");
    Resource maxAllocation2 = scheduler.getMaximumResourceCapability(null);
    Resource maxAllocation3 = scheduler.getMaximumResourceCapability();

    Assert.assertEquals(maxAllocation1, maxAllocation2);
    Assert.assertEquals(maxAllocation1, maxAllocation3);
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        maxAllocation1.getMemorySize());
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        maxAllocation1.getVirtualCores());

    Assert.assertEquals(1024, maxAllocationForQueue.getMemorySize());
    Assert.assertEquals(1, maxAllocationForQueue.getVirtualCores());
    scheduler.stop();
  }

  @Test
  public void testParseQueueWithAbsoluteResource() {
    String childQueue = "testQueue";
    String labelName = "testLabel";

    CapacityScheduler cs = new CapacityScheduler();
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    conf.setQueues("root", new String[] {childQueue});
    conf.setCapacity("root." + childQueue, "[memory=20480,vcores=200]");
    conf.setAccessibleNodeLabels("root." + childQueue,
        Sets.newHashSet(labelName));
    conf.setCapacityByLabel("root", labelName, "[memory=10240,vcores=100]");
    conf.setCapacityByLabel("root." + childQueue, labelName,
        "[memory=4096,vcores=10]");

    cs.init(conf);
    cs.start();

    Resource rootQueueLableCapacity =
        cs.getQueue("root").getQueueResourceQuotas()
            .getConfiguredMinResource(labelName);
    assertEquals(10240, rootQueueLableCapacity.getMemorySize());
    assertEquals(100, rootQueueLableCapacity.getVirtualCores());

    QueueResourceQuotas childQueueQuotas =
        cs.getQueue(childQueue).getQueueResourceQuotas();
    Resource childQueueCapacity = childQueueQuotas.getConfiguredMinResource();
    assertEquals(20480, childQueueCapacity.getMemorySize());
    assertEquals(200, childQueueCapacity.getVirtualCores());

    Resource childQueueLabelCapacity =
        childQueueQuotas.getConfiguredMinResource(labelName);
    assertEquals(4096, childQueueLabelCapacity.getMemorySize());
    assertEquals(10, childQueueLabelCapacity.getVirtualCores());
    cs.stop();
  }

  @Test
  public void testCapacitySchedulerInfo() throws Exception {
    QueueInfo queueInfo = resourceManager.getResourceScheduler().getQueueInfo("a", true, true);
    Assert.assertEquals("Queue Name should be a", "a",
        queueInfo.getQueueName());
    Assert.assertEquals("Queue Path should be root.a", "root.a",
        queueInfo.getQueuePath());
    Assert.assertEquals("Child Queues size should be 2", 2,
        queueInfo.getChildQueues().size());

    List<QueueUserACLInfo> userACLInfo = resourceManager.getResourceScheduler().getQueueUserAclInfo();
    Assert.assertNotNull(userACLInfo);
    for (QueueUserACLInfo queueUserACLInfo : userACLInfo) {
      Assert.assertEquals(1, getQueueCount(userACLInfo,
          queueUserACLInfo.getQueueName()));
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

    ApplicationAttemptId appAttemptId1 = appHelper(rm, cs, 100, 1, "default", "user");
    ApplicationAttemptId appAttemptId2 = appHelper(rm, cs, 100, 2, "default", "user");

    RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

    Priority priority = TestUtils.createMockPriority(1);
    ResourceRequest r1 = TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true, priority, recordFactory);

    //This will allocate for app1
    cs.allocate(appAttemptId1,
        Collections.<ResourceRequest>singletonList(r1), null, Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS);

    //And this will result in container assignment for app1
    CapacityScheduler.schedule(cs);

    //Verify that app1 is still first in assignment order
    //This happens because app2 has no demand/a magnitude of NaN, which
    //results in app1 and app2 being equal in the fairness comparison and
    //failling back to fifo (start) ordering
    assertEquals(q.getOrderingPolicy().getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR).next().getId(),
        appAttemptId1.getApplicationId().toString());

    //Now, allocate for app2 (this would be the first/AM allocation)
    ResourceRequest r2 = TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true, priority, recordFactory);
    cs.allocate(appAttemptId2,
        Collections.<ResourceRequest>singletonList(r2), null, Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS);

    //In this case we do not perform container assignment because we want to
    //verify re-ordering based on the allocation alone

    //Now, the first app for assignment is app2
    assertEquals(q.getOrderingPolicy().getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR).next().getId(),
        appAttemptId2.getApplicationId().toString());

    rm.stop();
  }

  @Test
  public void testResourceOverCommit() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    ResourceScheduler scheduler = rm.getResourceScheduler();

    MockNM nm = rm.registerNode("127.0.0.1:1234", 4 * GB);
    NodeId nmId = nm.getNodeId();
    RMApp app = MockRMAppSubmitter.submitWithMemory(2048, rm);
    // kick the scheduling, 2 GB given to AM1, remaining 2GB on nm
    nm.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am.registerAppAttempt();
    assertMemory(scheduler, nmId, 2 * GB, 2 * GB);

    // add request for 1 container of 2 GB
    am.addRequests(new String[] {"127.0.0.1", "127.0.0.2"}, 2 * GB, 1, 1);
    AllocateResponse alloc1Response = am.schedule(); // send the request

    // kick the scheduler, 2 GB given to AM1, resource remaining 0
    nm.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().isEmpty()) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      alloc1Response = am.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    assertEquals(1, allocated1.size());
    Container c1 = allocated1.get(0);
    assertEquals(2 * GB, c1.getResource().getMemorySize());
    assertEquals(nmId, c1.getNodeId());

    // check node report, 4 GB used and 0 GB available
    assertMemory(scheduler, nmId, 4 * GB, 0);
    nm.nodeHeartbeat(true);
    assertEquals(4 * GB, nm.getCapability().getMemorySize());

    // update node resource to 2 GB, so resource is over-consumed
    updateNodeResource(rm, nmId, 2 * GB, 2, -1);
    // the used resource should still 4 GB and negative available resource
    waitMemory(scheduler, nmId, 4 * GB, -2 * GB, 200, 5 * 1000);
    // check that we did not get a preemption requests
    assertNoPreemption(am.schedule().getPreemptionMessage());

    // check that the NM got the updated resources
    nm.nodeHeartbeat(true);
    assertEquals(2 * GB, nm.getCapability().getMemorySize());

    // check container can complete successfully with resource over-commitment
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        c1.getId(), ContainerState.COMPLETE, "", 0, c1.getResource());
    nm.containerStatus(containerStatus);

    LOG.info("Waiting for containers to be finished for app 1...");
    GenericTestUtils.waitFor(
        () -> attempt1.getJustFinishedContainers().size() == 1, 100, 2000);
    assertEquals(1, am.schedule().getCompletedContainersStatuses().size());
    assertMemory(scheduler, nmId, 2 * GB, 0);

    // verify no NPE is trigger in schedule after resource is updated
    am.addRequests(new String[] {"127.0.0.1", "127.0.0.2"}, 3 * GB, 1, 1);
    AllocateResponse allocResponse2 = am.schedule();
    assertTrue("Shouldn't have enough resource to allocate containers",
        allocResponse2.getAllocatedContainers().isEmpty());
    // try 10 times as scheduling is an async process
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      allocResponse2 = am.schedule();
      assertTrue("Shouldn't have enough resource to allocate containers",
          allocResponse2.getAllocatedContainers().isEmpty());
    }

    // increase the resources again to 5 GB to schedule the 3GB container
    updateNodeResource(rm, nmId, 5 * GB, 2, -1);
    waitMemory(scheduler, nmId, 2 * GB, 3 * GB, 100, 5 * 1000);

    // kick the scheduling and check it took effect
    nm.nodeHeartbeat(true);
    while (allocResponse2.getAllocatedContainers().isEmpty()) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      allocResponse2 = am.schedule();
    }
    assertEquals(1, allocResponse2.getAllocatedContainers().size());
    Container c2 = allocResponse2.getAllocatedContainers().get(0);
    assertEquals(3 * GB, c2.getResource().getMemorySize());
    assertEquals(nmId, c2.getNodeId());
    assertMemory(scheduler, nmId, 5 * GB, 0);

    // reduce the resources and trigger a preempt request to the AM for c2
    updateNodeResource(rm, nmId, 3 * GB, 2, 2 * 1000);
    waitMemory(scheduler, nmId, 5 * GB, -2 * GB, 200, 5 * 1000);

    PreemptionMessage preemptMsg = am.schedule().getPreemptionMessage();
    assertPreemption(c2.getId(), preemptMsg);

    // increasing the resources again, should stop killing the containers
    updateNodeResource(rm, nmId, 5 * GB, 2, -1);
    waitMemory(scheduler, nmId, 5 * GB, 0, 200, 5 * 1000);
    Thread.sleep(3 * 1000);
    assertMemory(scheduler, nmId, 5 * GB, 0);

    // reduce the resources again to trigger a preempt request to the AM for c2
    long t0 = Time.now();
    updateNodeResource(rm, nmId, 3 * GB, 2, 2 * 1000);
    waitMemory(scheduler, nmId, 5 * GB, -2 * GB, 200, 5 * 1000);

    preemptMsg = am.schedule().getPreemptionMessage();
    assertPreemption(c2.getId(), preemptMsg);

    // wait until the scheduler kills the container
    GenericTestUtils.waitFor(() -> {
      try {
        nm.nodeHeartbeat(true); // trigger preemption in the NM
      } catch (Exception e) {
        LOG.error("Cannot heartbeat", e);
      }
      SchedulerNodeReport report = scheduler.getNodeReport(nmId);
      return report.getAvailableResource().getMemorySize() > 0;
    }, 200, 5 * 1000);
    assertMemory(scheduler, nmId, 2 * GB, 1 * GB);

    List<ContainerStatus> completedContainers =
        am.schedule().getCompletedContainersStatuses();
    assertEquals(1, completedContainers.size());
    ContainerStatus c2status = completedContainers.get(0);
    assertContainerKilled(c2.getId(), c2status);

    assertTime(2000, Time.now() - t0);

    rm.stop();
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
    rm.stop();
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
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppName("appname")
            .withUser("appuser")
            .withAcls(acls)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);

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
    RMApp app0 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MEMORY, rm1);
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
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(1024, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId1 = ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId1, RMContainerState.ALLOCATED);

    RMContainer rmContainer = cs.getRMContainer(containerId1);
    List<ResourceRequest> requests =
        rmContainer.getContainerRequest().getResourceRequests();
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
    rm1.stop();
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
    cs.stop();
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

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
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
    rm.stop();
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

    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    waitContainerAllocated(am1, 1 * GB, 1, 2, rm1, nm1);

    // Maximum resoure of b1 is 100 * 0.895 * 0.792 = 71 GB
    // 2 GBs used by am, so it's 71 - 2 = 69G.
    Assert.assertEquals(69 * GB,
        am1.doHeartbeat().getAvailableResources().getMemorySize());

    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b2")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
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
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);
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
    rm1.stop();
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
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    waitContainerAllocated(am1, 4 * GB, 2, 2, rm1, nm1);

    // Try to launch app2 in a2, asked 2GB, should success 
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a2")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
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
    rm1.stop();
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
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    // Launch app2 in queue=b1  
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(8 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);
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
    rm.stop();
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
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(amMemory, rm)
            .withAppName("app-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue(queueName)
            .withUnmanagedAM(false)
            .build();
    RMApp rmApp = MockRMAppSubmitter.submit(rm, data);

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
            null, Collections.<ContainerId> emptyList(), null, null,
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
            null, Collections.<ContainerId> emptyList(), null, null,
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

    ApplicationAttemptId appAttemptId = appHelper(rm, cs, 100, 1, "default", "user1");
    ApplicationAttemptId appAttemptId2 = appHelper(rm, cs, 100, 2, "default", "user2");

    // add nodes  to cluster, so cluster have 20GB and 20 vcores
    Resource newResource = Resource.newInstance(10 * GB, 10);
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    cs.handle(new NodeAddedSchedulerEvent(node));

    Resource newResource2 = Resource.newInstance(10 * GB, 10);
    RMNode node2 = MockNodes.newNodeInfo(0, newResource2, 1, "127.0.0.2");
    cs.handle(new NodeAddedSchedulerEvent(node2));

    FiCaSchedulerApp fiCaApp1 =
            cs.getSchedulerApplications().get(appAttemptId.getApplicationId())
                .getCurrentAppAttempt();

    FiCaSchedulerApp fiCaApp2 =
            cs.getSchedulerApplications().get(appAttemptId2.getApplicationId())
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
    rm.stop();
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
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
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
    rm.stop();
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

    MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithResource(amResource1, rm)
        .withResource(amResource1)
        .withAppName("app-1")
        .withUser(userName)
        .withAcls(null)
        .withQueue(queueName)
        .build());

    MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithResource(amResource2, rm)
        .withResource(amResource2)
        .withAppName("app-2")
        .withUser(userName)
        .withAcls(null)
        .withQueue(queueName)
        .build());

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
          Collections.<ResourceRequest>singletonList(y1Req), null, Collections.<ContainerId>emptyList(),
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
          Collections.<ResourceRequest>singletonList(x1Req), null, Collections.<ContainerId>emptyList(),
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
        Collections.<ResourceRequest>singletonList(x2Req), null, Collections.<ContainerId>emptyList(),
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
        Collections.<ResourceRequest>singletonList(x1Req), null, Collections.<ContainerId>emptyList(),
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
          Collections.<ResourceRequest>singletonList(y1Req), null, Collections.<ContainerId>emptyList(),
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
        null, Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS).getContainers().size();
    CapacityScheduler.schedule(cs);
    ResourceRequest r2 = null;
    for (int i =0; i < 13; i++) {
      r2 = TestUtils.createResourceRequest(
          ResourceRequest.ANY, 1 * GB, 1, true, priority, recordFactory);
      cs.allocate(appAttemptId2,
          Collections.<ResourceRequest>singletonList(r2), null, Collections.<ContainerId>emptyList(),
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
        null, Collections.<ContainerId>emptyList(),
        null, null, NULL_UPDATE_REQUESTS).getContainers().size();
    CapacityScheduler.schedule(cs);

    cs.allocate(appAttemptId2, Collections.<ResourceRequest>singletonList(r2),
        null, Collections.<ContainerId>emptyList(), null, null, NULL_UPDATE_REQUESTS);
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
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);

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
    rm.stop();
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


  @Test
  public void testQueueMappingWithCurrentUserQueueMappingForaGroup() throws
      Exception {

    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();
    config.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    setupQueueConfiguration(config);

    config.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        TestGroupsCaching.FakeunPrivilegedGroupMapping.class, ShellBasedUnixGroupsMapping.class);
    config.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
        "a1" +"=" + "agroup" + "");
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(config);

    config.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "g:agroup:%user");

    MockRM rm = new MockRM(config);
    rm.start();
    CapacityScheduler cs = ((CapacityScheduler) rm.getResourceScheduler());
    cs.start();

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("appname")
            .withUser("a1")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    List<ApplicationAttemptId> appsInA1 = cs.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());
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
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", 0));
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
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", -1));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", 0));

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
    } catch (ServiceStateException sse) {
      Throwable rootCause = sse.getCause().getCause();
      Assert.assertTrue(
          rootCause.getMessage().contains("can't exceed maximum lifetime"));
    }

    maxLifetime = -1;
    defaultLifetime = 10;
    cs = setUpCSQueue(maxLifetime, defaultLifetime);
    Assert.assertEquals(100,
        cs.checkAndGetApplicationLifetime("default", 100));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", -1));
    Assert.assertEquals(defaultLifetime,
        cs.checkAndGetApplicationLifetime("default", 0));
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

  @Test (timeout = 60000)
  public void testClearRequestsBeforeApplyTheProposal()
      throws Exception {
    // init RM & NMs & Nodes
    final MockRM rm = new MockRM(new CapacitySchedulerConfiguration());
    rm.start();
    final MockNM nm = rm.registerNode("h1:1234", 200 * GB);

    // submit app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
            .withAppName("app")
            .withUser("user")
            .build();
    final RMApp app = MockRMAppSubmitter.submit(rm, data);
    MockRM.launchAndRegisterAM(app, rm, nm);

    // spy capacity scheduler to handle CapacityScheduler#apply
    final Priority priority = Priority.newInstance(1);
    final CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    final CapacityScheduler spyCs = Mockito.spy(cs);
    Mockito.doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) throws Exception {
        // clear resource request before applying the proposal for container_2
        spyCs.allocate(app.getCurrentAppAttempt().getAppAttemptId(),
            Arrays.asList(ResourceRequest.newInstance(priority, "*",
                Resources.createResource(1 * GB), 0)), null,
            Collections.<ContainerId>emptyList(), null, null,
            NULL_UPDATE_REQUESTS);
        // trigger real apply which can raise NPE before YARN-6629
        try {
          FiCaSchedulerApp schedulerApp = cs.getApplicationAttempt(
              app.getCurrentAppAttempt().getAppAttemptId());
          schedulerApp.apply((Resource) invocation.getArguments()[0],
              (ResourceCommitRequest) invocation.getArguments()[1],
              (Boolean) invocation.getArguments()[2]);
          // the proposal of removed request should be rejected
          Assert.assertEquals(1, schedulerApp.getLiveContainers().size());
        } catch (Throwable e) {
          Assert.fail();
        }
        return null;
      }
    }).when(spyCs).tryCommit(Mockito.any(Resource.class),
        Mockito.any(ResourceCommitRequest.class), Mockito.anyBoolean());

    // rm allocates container_2 to reproduce the process that can raise NPE
    spyCs.allocate(app.getCurrentAppAttempt().getAppAttemptId(),
        Arrays.asList(ResourceRequest.newInstance(priority, "*",
            Resources.createResource(1 * GB), 1)), null,
        Collections.<ContainerId>emptyList(), null, null, NULL_UPDATE_REQUESTS);
    spyCs.handle(new NodeUpdateSchedulerEvent(
        spyCs.getNode(nm.getNodeId()).getRMNode()));
    rm.stop();
  }

  // Testcase for YARN-8528
  // This is to test whether ContainerAllocation constants are holding correct
  // values during scheduling.
  @Test
  public void testContainerAllocationLocalitySkipped() throws Exception {
    Assert.assertEquals(AllocationState.APP_SKIPPED,
        ContainerAllocation.APP_SKIPPED.getAllocationState());
    Assert.assertEquals(AllocationState.LOCALITY_SKIPPED,
        ContainerAllocation.LOCALITY_SKIPPED.getAllocationState());
    Assert.assertEquals(AllocationState.PRIORITY_SKIPPED,
        ContainerAllocation.PRIORITY_SKIPPED.getAllocationState());
    Assert.assertEquals(AllocationState.QUEUE_SKIPPED,
        ContainerAllocation.QUEUE_SKIPPED.getAllocationState());

    // init RM & NMs & Nodes
    final MockRM rm = new MockRM(new CapacitySchedulerConfiguration());
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    rm.start();
    final MockNM nm1 = rm.registerNode("h1:1234", 4 * GB);
    final MockNM nm2 = rm.registerNode("h2:1234", 6 * GB); // maximum-allocation-mb = 6GB

    // submit app and request resource
    // container2 is larger than nm1 total resource, will trigger locality skip
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .build();
    final RMApp app = MockRMAppSubmitter.submit(rm, data);
    final MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);
    am.addRequests(new String[] {"*"}, 5 * GB, 1, 1, 2);
    am.schedule();

    // container1 (am) should be acquired, container2 should not
    RMNode node1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(node1));
    ContainerId cid = ContainerId.newContainerId(am.getApplicationAttemptId(), 1l);
    assertThat(cs.getRMContainer(cid).getState()).
        isEqualTo(RMContainerState.ACQUIRED);
    cid = ContainerId.newContainerId(am.getApplicationAttemptId(), 2l);
    Assert.assertNull(cs.getRMContainer(cid));

    Assert.assertEquals(AllocationState.APP_SKIPPED,
        ContainerAllocation.APP_SKIPPED.getAllocationState());
    Assert.assertEquals(AllocationState.LOCALITY_SKIPPED,
        ContainerAllocation.LOCALITY_SKIPPED.getAllocationState());
    Assert.assertEquals(AllocationState.PRIORITY_SKIPPED,
        ContainerAllocation.PRIORITY_SKIPPED.getAllocationState());
    Assert.assertEquals(AllocationState.QUEUE_SKIPPED,
        ContainerAllocation.QUEUE_SKIPPED.getAllocationState());
    rm.stop();
  }

  /**
   * Tests
   * @throws Exception
   */
  @Test
  public void testCSQueueMetricsDoesNotLeakOnReinit() throws Exception {
    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    ResourceInformation memory =
        ResourceInformation.newInstance(ResourceInformation.MEMORY_MB.getName(),
            ResourceInformation.MEMORY_MB.getUnits(),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores =
        ResourceInformation.newInstance(ResourceInformation.VCORES.getName(),
            ResourceInformation.VCORES.getUnits(),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setResourceComparator(DominantResourceCalculator.class);

    setupQueueConfiguration(csConf);

    YarnConfiguration conf = new YarnConfiguration(csConf);

    // Don't reset resource types since we have already configured resource
    // types
    conf.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES, false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    csConf = new CapacitySchedulerConfiguration();
    setupAdditionalQueues(csConf);
    cs.reinitialize(csConf, cs.getRMContext());
    QueueMetrics a3DefaultPartitionMetrics = QueueMetrics.getQueueMetrics().get(
        "default.root.a.a3");

    Assert.assertSame("Different ParentQueue of siblings is a sign of a memory leak",
        QueueMetrics.getQueueMetrics().get("root.a.a1").getParentQueue(),
        QueueMetrics.getQueueMetrics().get("root.a.a3").getParentQueue());

    Assert.assertSame("Different ParentQueue of partition metrics is a sign of a memory leak",
        QueueMetrics.getQueueMetrics().get("root.a.a1").getParentQueue(),
        a3DefaultPartitionMetrics.getParentQueue());
    rm.stop();
  }

  @Test
  public void testCSQueueMetrics() throws Exception {

    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    ResourceInformation memory =
        ResourceInformation.newInstance(ResourceInformation.MEMORY_MB.getName(),
            ResourceInformation.MEMORY_MB.getUnits(),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores =
        ResourceInformation.newInstance(ResourceInformation.VCORES.getName(),
            ResourceInformation.VCORES.getUnits(),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);
    riMap.put(TestQueueMetricsForCustomResources.CUSTOM_RES_1,
        ResourceInformation.newInstance(
            TestQueueMetricsForCustomResources.CUSTOM_RES_1, "", 1, 10));

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setResourceComparator(DominantResourceCalculator.class);

    csConf.set(YarnConfiguration.RESOURCE_TYPES,
        TestQueueMetricsForCustomResources.CUSTOM_RES_1);

    setupQueueConfiguration(csConf);

    YarnConfiguration conf = new YarnConfiguration(csConf);

    // Don't reset resource types since we have already configured resource
    // types
    conf.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES, false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    RMNode n1 = MockNodes.newNodeInfo(0,
        MockNodes.newResource(50 * GB, 50,
            ImmutableMap.<String, String> builder()
                .put(TestQueueMetricsForCustomResources.CUSTOM_RES_1,
                    String.valueOf(1000))
                .build()),
        1, "n1");
    RMNode n2 = MockNodes.newNodeInfo(0,
        MockNodes.newResource(50 * GB, 50,
            ImmutableMap.<String, String> builder()
                .put(TestQueueMetricsForCustomResources.CUSTOM_RES_1,
                    String.valueOf(2000))
                .build()),
        2, "n2");
    cs.handle(new NodeAddedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n2));

    Map<String, Long> guaranteedCapA11 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a1")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getGuaranteedCapacity();
    assertEquals(94, guaranteedCapA11
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> maxCapA11 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a1")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getMaxCapacity();
    assertEquals(3000, maxCapA11
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());

    assertEquals(10240, ((CSQueueMetrics)cs.getQueue("a").getMetrics()).getGuaranteedMB());
    assertEquals(71680, ((CSQueueMetrics)cs.getQueue("b1").getMetrics()).getGuaranteedMB());
    assertEquals(102400, ((CSQueueMetrics)cs.getQueue("a").getMetrics()).getMaxCapacityMB());
    assertEquals(102400, ((CSQueueMetrics)cs.getQueue("b1").getMetrics()).getMaxCapacityMB());
    Map<String, Long> guaranteedCapA =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getGuaranteedCapacity();
    assertEquals(314, guaranteedCapA
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> maxCapA =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getMaxCapacity();
    assertEquals(3000, maxCapA
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> guaranteedCapB1 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("b1")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getGuaranteedCapacity();
    assertEquals(2126, guaranteedCapB1
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> maxCapB1 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("b1")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getMaxCapacity();
    assertEquals(3000, maxCapB1
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());

    // Remove a node, metrics should be updated
    cs.handle(new NodeRemovedSchedulerEvent(n2));
    assertEquals(5120, ((CSQueueMetrics)cs.getQueue("a").getMetrics()).getGuaranteedMB());
    assertEquals(35840, ((CSQueueMetrics)cs.getQueue("b1").getMetrics()).getGuaranteedMB());
    assertEquals(51200, ((CSQueueMetrics)cs.getQueue("a").getMetrics()).getMaxCapacityMB());
    assertEquals(51200, ((CSQueueMetrics)cs.getQueue("b1").getMetrics()).getMaxCapacityMB());
    Map<String, Long> guaranteedCapA1 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getGuaranteedCapacity();

    assertEquals(104, guaranteedCapA1
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> maxCapA1 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getMaxCapacity();
    assertEquals(1000, maxCapA1
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> guaranteedCapB11 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("b1")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getGuaranteedCapacity();
    assertEquals(708, guaranteedCapB11
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> maxCapB11 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("b1")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getMaxCapacity();
    assertEquals(1000, maxCapB11
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    assertEquals(A_CAPACITY / 100, ((CSQueueMetrics)cs.getQueue("a")
        .getMetrics()).getGuaranteedCapacity(), DELTA);
    assertEquals(A_CAPACITY / 100, ((CSQueueMetrics)cs.getQueue("a")
        .getMetrics()).getGuaranteedAbsoluteCapacity(), DELTA);
    assertEquals(B1_CAPACITY / 100, ((CSQueueMetrics)cs.getQueue("b1")
        .getMetrics()).getGuaranteedCapacity(), DELTA);
    assertEquals((B_CAPACITY / 100) * (B1_CAPACITY / 100), ((CSQueueMetrics)cs
        .getQueue("b1").getMetrics()).getGuaranteedAbsoluteCapacity(), DELTA);
    assertEquals(1, ((CSQueueMetrics)cs.getQueue("a").getMetrics())
        .getMaxCapacity(), DELTA);
    assertEquals(1, ((CSQueueMetrics)cs.getQueue("a").getMetrics())
        .getMaxAbsoluteCapacity(), DELTA);
    assertEquals(1, ((CSQueueMetrics)cs.getQueue("b1").getMetrics())
        .getMaxCapacity(), DELTA);
    assertEquals(1, ((CSQueueMetrics)cs.getQueue("b1").getMetrics())
        .getMaxAbsoluteCapacity(), DELTA);

    // Add child queue to a, and reinitialize. Metrics should be updated
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT + ".a",
        new String[] {"a1", "a2", "a3"});
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".a.a2", 29.5f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".a.a3", 40.5f);
    csConf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT + ".a.a3",
        50.0f);

    cs.reinitialize(csConf, new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(csConf),
        new NMTokenSecretManagerInRM(csConf),
        new ClientToAMTokenSecretManagerInRM(), null));

    assertEquals(1024, ((CSQueueMetrics)cs.getQueue("a2").getMetrics()).getGuaranteedMB());
    assertEquals(2048, ((CSQueueMetrics)cs.getQueue("a3").getMetrics()).getGuaranteedMB());
    assertEquals(51200, ((CSQueueMetrics)cs.getQueue("a2").getMetrics()).getMaxCapacityMB());
    assertEquals(25600, ((CSQueueMetrics)cs.getQueue("a3").getMetrics()).getMaxCapacityMB());

    Map<String, Long> guaranteedCapA2 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a2")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getGuaranteedCapacity();
    assertEquals(30, guaranteedCapA2
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> maxCapA2 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a2")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getMaxCapacity();
    assertEquals(1000, maxCapA2
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());

    Map<String, Long> guaranteedCapA3 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a3")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getGuaranteedCapacity();
    assertEquals(42, guaranteedCapA3
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    Map<String, Long> maxCapA3 =
        ((CSQueueMetricsForCustomResources) ((CSQueueMetrics) cs.getQueue("a3")
            .getMetrics()).getQueueMetricsForCustomResources())
                .getMaxCapacity();
    assertEquals(500, maxCapA3
        .get(TestQueueMetricsForCustomResources.CUSTOM_RES_1).longValue());
    rm.stop();
  }

  @Test
  public void testReservedContainerLeakWhenMoveApplication() throws Exception {
    CapacitySchedulerConfiguration csConf
        = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b"});
    csConf.setCapacity("root.a", 50);
    csConf.setMaximumCapacity("root.a", 100);
    csConf.setUserLimitFactor("root.a", 100);
    csConf.setCapacity("root.b", 50);
    csConf.setMaximumCapacity("root.b", 100);
    csConf.setUserLimitFactor("root.b", 100);

    YarnConfiguration conf=new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    RMNodeLabelsManager mgr=new NullRMNodeLabelsManager();
    mgr.init(conf);
    MockRM rm1 = new MockRM(csConf);
    CapacityScheduler scheduler=(CapacityScheduler) rm1.getResourceScheduler();
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("127.0.0.2:1234", 8 * GB);
    /*
     * simulation
     * app1: (1 AM,1 running container)
     * app2: (1 AM,1 reserved container)
     */
    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData submissionData =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app_1")
            .withUser("user_1")
            .withAcls(null)
            .withQueue("a")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, submissionData);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm1
    submissionData =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app_2")
            .withUser("user_1")
            .withAcls(null)
            .withQueue("a")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, submissionData);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    am1.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    // this containerRequest should be reserved
    am2.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());

    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    // Do node heartbeats 2 times
    // First time will allocate container for app1, second time will reserve
    // container for app2
    scheduler.handle(new NodeUpdateSchedulerEvent(rmNode1));
    scheduler.handle(new NodeUpdateSchedulerEvent(rmNode1));

    FiCaSchedulerApp schedulerApp1 =
        scheduler.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        scheduler.getApplicationAttempt(am2.getApplicationAttemptId());
    // APP1:  1 AM, 1 allocatedContainer
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    // APP2:  1 AM,1 reservedContainer
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getReservedContainers().size());
    //before,move app2 which has one reservedContainer
    LeafQueue srcQueue = (LeafQueue) scheduler.getQueue("a");
    LeafQueue desQueue = (LeafQueue) scheduler.getQueue("b");
    Assert.assertEquals(4, srcQueue.getNumContainers());
    Assert.assertEquals(10*GB, srcQueue.getUsedResources().getMemorySize());
    Assert.assertEquals(0, desQueue.getNumContainers());
    Assert.assertEquals(0, desQueue.getUsedResources().getMemorySize());
    //app1 ResourceUsage (0 reserved)
    Assert.assertEquals(5*GB,
        schedulerApp1
            .getAppAttemptResourceUsage().getAllUsed().getMemorySize());
    Assert.assertEquals(0,
        schedulerApp1.getCurrentReservation().getMemorySize());
    //app2  ResourceUsage (4GB reserved)
    Assert.assertEquals(1*GB,
        schedulerApp2
            .getAppAttemptResourceUsage().getAllUsed().getMemorySize());
    Assert.assertEquals(4*GB,
        schedulerApp2.getCurrentReservation().getMemorySize());
    //move app2 which has one reservedContainer
    scheduler.moveApplication(app2.getApplicationId(), "b");
    // keep this order
    // if killing app1 first,the reservedContainer of app2 will be allocated
    rm1.killApp(app2.getApplicationId());
    rm1.killApp(app1.getApplicationId());
    //after,moved app2 which has one reservedContainer
    Assert.assertEquals(0, srcQueue.getNumContainers());
    Assert.assertEquals(0, desQueue.getNumContainers());
    Assert.assertEquals(0, srcQueue.getUsedResources().getMemorySize());
    Assert.assertEquals(0, desQueue.getUsedResources().getMemorySize());
    rm1.close();
  }
}
