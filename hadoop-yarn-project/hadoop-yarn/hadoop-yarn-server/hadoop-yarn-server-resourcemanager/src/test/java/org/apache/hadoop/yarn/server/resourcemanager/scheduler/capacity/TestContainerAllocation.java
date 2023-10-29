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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMSecretManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAX_ASSIGN_PER_HEARTBEAT;

public class TestContainerAllocation {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestContainerAllocation.class);

  private final int GB = 1024;

  private YarnConfiguration conf;
  
  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test(timeout = 60000)
  public void testExcessReservationThanNodeManagerCapacity() throws Exception {
    @SuppressWarnings("resource")
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 2 * GB, 4);
    MockNM nm2 = rm.registerNode("127.0.0.1:2234", 3 * GB, 4);

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(2, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(128, rm);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    LOG.info("sending container requests ");
    am1.addRequests(new String[] {"*"}, 2 * GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    int waitCounter = 20;
    LOG.info("heartbeating nm1");
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());

    // No container should be allocated.
    // Internally it should not been reserved.
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 0);

    LOG.info("heartbeating nm2");
    waitCounter = 20;
    nm2.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 1);

    rm.stop();
  }

  // This is to test container tokens are generated when the containers are
  // acquired by the AM, not when the containers are allocated
  @Test
  public void testContainerTokenGeneratedOnPullRequest() throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8000);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    RMContainer container =
        rm1.getResourceScheduler().getRMContainer(containerId2);
    // no container token is generated.
    Assert.assertEquals(containerId2, container.getContainerId());
    Assert.assertNull(container.getContainer().getContainerToken());

    // acquire the container.
    List<Container> containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    Assert.assertEquals(containerId2, containers.get(0).getId());
    // container token is generated.
    Assert.assertNotNull(containers.get(0).getContainerToken());
    rm1.stop();
  }

  @Test
  public void testNormalContainerAllocationWhenDNSUnavailable() throws Exception{
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED);

    // acquire the container.
    SecurityUtilTestHelper.setTokenServiceUseIp(true);
    List<Container> containers;
    try {
      containers =
          am1.allocate(new ArrayList<ResourceRequest>(),
              new ArrayList<ContainerId>()).getAllocatedContainers();
      // not able to fetch the container;
      Assert.assertEquals(0, containers.size());
    } finally {
      SecurityUtilTestHelper.setTokenServiceUseIp(false);
    }
    containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    // should be able to fetch the container;
    Assert.assertEquals(1, containers.size());
    rm1.stop();
  }

  // This is to test whether LogAggregationContext is passed into
  // container tokens correctly
  @Test
  public void testLogAggregationContextPassedIntoContainerToken()
      throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8000);
    MockNM nm2 = rm1.registerNode("127.0.0.1:2345", 8000);
    // LogAggregationContext is set as null
    Assert
      .assertNull(getLogAggregationContextFromContainerToken(rm1, nm1, null));

    // create a not-null LogAggregationContext
    LogAggregationContext logAggregationContext =
        LogAggregationContext.newInstance(
          "includePattern", "excludePattern",
          "rolledLogsIncludePattern",
          "rolledLogsExcludePattern",
          "policyClass",
          "policyParameters");
    LogAggregationContext returned =
        getLogAggregationContextFromContainerToken(rm1, nm2,
          logAggregationContext);
    Assert.assertEquals("includePattern", returned.getIncludePattern());
    Assert.assertEquals("excludePattern", returned.getExcludePattern());
    Assert.assertEquals("rolledLogsIncludePattern",
        returned.getRolledLogsIncludePattern());
    Assert.assertEquals("rolledLogsExcludePattern",
        returned.getRolledLogsExcludePattern());
    Assert.assertEquals("policyClass",
        returned.getLogAggregationPolicyClassName());
    Assert.assertEquals("policyParameters",
        returned.getLogAggregationPolicyParameters());
    rm1.stop();
  }

  private LogAggregationContext getLogAggregationContextFromContainerToken(
      MockRM rm1, MockNM nm1, LogAggregationContext logAggregationContext)
      throws Exception {
    RMApp app2 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withLogAggregationContext(logAggregationContext)
            .build());
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);
    nm1.nodeHeartbeat(true);
    // request a container.
    am2.allocate("127.0.0.1", 512, 1, new ArrayList<ContainerId>());
    ContainerId containerId =
        ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId, RMContainerState.ALLOCATED);

    // acquire the container.
    List<Container> containers =
        am2.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    Assert.assertEquals(containerId, containers.get(0).getId());
    // container token is generated.
    Assert.assertNotNull(containers.get(0).getContainerToken());
    ContainerTokenIdentifier token =
        BuilderUtils.newContainerTokenIdentifier(containers.get(0)
          .getContainerToken());
    return token.getLogAggregationContext();
  }

  private volatile int numRetries = 0;
  private class TestRMSecretManagerService extends RMSecretManagerService {

    public TestRMSecretManagerService(Configuration conf,
        RMContextImpl rmContext) {
      super(conf, rmContext);
    }
    @Override
    protected RMContainerTokenSecretManager createContainerTokenSecretManager(
        Configuration conf) {
      return new RMContainerTokenSecretManager(conf) {

        @Override
        public Token createContainerToken(ContainerId containerId,
            int containerVersion, NodeId nodeId, String appSubmitter,
            Resource capability, Priority priority, long createTime,
            LogAggregationContext logAggregationContext, String nodeLabelExp,
            ContainerType containerType, ExecutionType executionType,
            long allocationRequestId, Set<String> allocationTags) {
          numRetries++;
          return super.createContainerToken(containerId, containerVersion,
              nodeId, appSubmitter, capability, priority, createTime,
              logAggregationContext, nodeLabelExp, containerType,
              executionType, allocationRequestId, allocationTags);
        }
      };
    }
  }

  // This is to test fetching AM container will be retried, if AM container is
  // not fetchable since DNS is unavailable causing container token/NMtoken
  // creation failure.
  @Test(timeout = 30000)
  public void testAMContainerAllocationWhenDNSUnavailable() throws Exception {
    MockRM rm1 = new MockRM(conf) {
      @Override
      protected RMSecretManagerService createRMSecretManagerService() {
        return new TestRMSecretManagerService(conf, rmContext);
      }
    };
    rm1.start();

    MockNM nm1 = rm1.registerNode("unknownhost:1234", 8000);
    RMApp app1;
    try {
      SecurityUtilTestHelper.setTokenServiceUseIp(true);
      app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
      RMAppAttempt attempt = app1.getCurrentAppAttempt();
      nm1.nodeHeartbeat(true);

      // fetching am container will fail, keep retrying 5 times.
      while (numRetries <= 5) {
        nm1.nodeHeartbeat(true);
        Thread.sleep(1000);
        Assert.assertEquals(RMAppAttemptState.SCHEDULED,
            attempt.getAppAttemptState());
        System.out.println("Waiting for am container to be allocated.");
      }
    } finally {
      SecurityUtilTestHelper.setTokenServiceUseIp(false);
    }
    MockRM.launchAndRegisterAM(app1, rm1, nm1);
    rm1.stop();
  }
  
  @Test(timeout = 60000)
  public void testExcessReservationWillBeUnreserved() throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to a queue. And there's one
     * node with 8G resource in the cluster. App1 allocates a 6G container, Then
     * app2 asks for a 4G container. App2's request will be reserved on the
     * node.
     * 
     * Before next node heartbeat, app2 cancels the reservation, we should found
     * the reserved resource is cancelled as well.
     */
    // inject node label manager
    MockRM rm1 = new MockRM();

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    
    // launch another app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);
  
    am1.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    am2.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("default");

    // Do node heartbeats 2 times
    // First time will allocate container for app1, second time will reserve
    // container for app2
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    
    // App2 will get preference to be allocated on node1, and node1 will be all
    // used by App2.
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    // Check if a 4G contaienr allocated for app1, and nothing allocated for app2
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertTrue(schedulerApp2.getReservedContainers().size() > 0);
    
    // NM1 has available resource = 2G (8G - 2 * 1G - 4G)
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Usage of queue = 4G + 2 * 1G + 4G (reserved)
    Assert.assertEquals(10 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemorySize());
    Assert.assertEquals(4 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getReserved().getMemorySize());
    Assert.assertEquals(4 * GB, leafQueue.getQueueResourceUsage().getReserved()
        .getMemorySize());

    // Cancel asks of app2 and re-kick RM
    am2.allocate("*", 4 * GB, 0, new ArrayList<ContainerId>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    
    // App2's reservation will be cancelled
    Assert.assertTrue(schedulerApp2.getReservedContainers().size() == 0);
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(6 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemorySize());
    Assert.assertEquals(0, cs.getRootQueue().getQueueResourceUsage()
        .getReserved().getMemorySize());
    Assert.assertEquals(0, leafQueue.getQueueResourceUsage().getReserved()
        .getMemorySize());

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testAllocationForReservedContainer() throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to a queue. And there's one
     * node with 8G resource in the cluster. App1 allocates a 6G container, Then
     * app2 asks for a 4G container. App2's request will be reserved on the
     * node.
     *
     * Before next node heartbeat, app1 container is completed/killed. So app1
     * container which was reserved will be allocated.
     */
    // inject node label manager
    MockRM rm1 = new MockRM();

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    am1.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    am2.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("default");

    // Do node heartbeats 2 times
    // First time will allocate container for app1, second time will reserve
    // container for app2
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // App2 will get preference to be allocated on node1, and node1 will be all
    // used by App2.
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    // Check if a 4G container allocated for app1, and nothing allocated for app2
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertTrue(schedulerApp2.getReservedContainers().size() > 0);

    // NM1 has available resource = 2G (8G - 2 * 1G - 4G)
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Usage of queue = 4G + 2 * 1G + 4G (reserved)
    Assert.assertEquals(10 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemorySize());
    Assert.assertEquals(4 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getReserved().getMemorySize());
    Assert.assertEquals(4 * GB, leafQueue.getQueueResourceUsage().getReserved()
        .getMemorySize());

    // Mark one app1 container as killed/completed and re-kick RM
    for (RMContainer container : schedulerApp1.getLiveContainers()) {
      if (container.isAMContainer()) {
        continue;
      }
      cs.markContainerForKillable(container);
    }
    // Cancel asks of app1 and re-kick RM
    am1.allocate("*", 4 * GB, 0, new ArrayList<ContainerId>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // Check 4G container cancelled for app1, and one container allocated for
    // app2
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());
    Assert.assertFalse(schedulerApp2.getReservedContainers().size() > 0);

    // NM1 has available resource = 2G (8G - 2 * 1G - 4G)
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Usage of queue = 4G + 2 * 1G
    Assert.assertEquals(6 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemorySize());
    Assert.assertEquals(0 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getReserved().getMemorySize());
    Assert.assertEquals(0 * GB, leafQueue.getQueueResourceUsage().getReserved()
        .getMemorySize());

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testReservedContainerMetricsOnDecommisionedNode() throws Exception {
    /**
     * Test case: Submit two application (app1/app2) to a queue. And there's one
     * node with 8G resource in the cluster. App1 allocates a 6G container, Then
     * app2 asks for a 4G container. App2's request will be reserved on the
     * node.
     *
     * Before next node heartbeat, app1 container is completed/killed. So app1
     * container which was reserved will be allocated.
     */
    // inject node label manager
    MockRM rm1 = new MockRM();

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    am1.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());
    am2.allocate("*", 4 * GB, 1, new ArrayList<ContainerId>());

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("default");

    // Do node heartbeats 2 times
    // First time will allocate container for app1, second time will reserve
    // container for app2
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // App2 will get preference to be allocated on node1, and node1 will be all
    // used by App2.
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    // Check if a 4G container allocated for app1, and nothing allocated for app2
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertTrue(schedulerApp2.getReservedContainers().size() > 0);

    // NM1 has available resource = 2G (8G - 2 * 1G - 4G)
    Assert.assertEquals(2 * GB, cs.getNode(nm1.getNodeId())
        .getUnallocatedResource().getMemorySize());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Usage of queue = 4G + 2 * 1G + 4G (reserved)
    Assert.assertEquals(10 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemorySize());
    Assert.assertEquals(4 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getReserved().getMemorySize());
    Assert.assertEquals(4 * GB, leafQueue.getQueueResourceUsage().getReserved()
        .getMemorySize());

    // Remove the node
    cs.handle(new NodeRemovedSchedulerEvent(rmNode1));

    // Check all container cancelled for app1 and app2
    Assert.assertEquals(0, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(0, schedulerApp2.getLiveContainers().size());
    Assert.assertFalse(schedulerApp2.getReservedContainers().size() > 0);

    // Usage and Reserved capacity of queue is 0
    Assert.assertEquals(0 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed().getMemorySize());
    Assert.assertEquals(0 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getReserved().getMemorySize());
    Assert.assertEquals(0 * GB, leafQueue.getQueueResourceUsage().getReserved()
        .getMemorySize());

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testAssignMultipleOffswitchContainers() throws Exception {
    MockRM rm1 = new MockRM();

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 80 * GB);

    // launch an app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    am1.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    // Do node heartbeats once
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());

    // App1 will get one container allocated (plus AM container
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());

    // Set assign multiple off-switch containers to 3
    CapacitySchedulerConfiguration newCSConf = new CapacitySchedulerConfiguration();
    newCSConf.setInt(
        CapacitySchedulerConfiguration.OFFSWITCH_PER_HEARTBEAT_LIMIT, 3);

    cs.reinitialize(newCSConf, rm1.getRMContext());

    // Do node heartbeats once
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // App1 will get 3 new container allocated (plus 2 previously allocated
    // container)
    Assert.assertEquals(5, schedulerApp1.getLiveContainers().size());

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testContinuousReservationLookingWhenUsedEqualsMax() throws Exception {
    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    // Set maximum capacity of A to 10
    newConf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT + ".a", 10);
    MockRM rm1 = new MockRM(newConf);

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 90 * GB);

    // launch an app to queue A, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch 2nd app to queue B, AM container should be launched in nm1
    // Now usage of nm1 is 3G (2G + 1G)
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    am1.allocate("*", 4 * GB, 2, null);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // Do node heartbeats twice, we expect one container allocated on nm1 and
    // one container reserved on nm1.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());

    // App1 will get 2 container allocated (plus AM container)
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp1.getReservedContainers().size());

    // Do node heartbeats on nm2, we expect one container allocated on nm2 and
    // one unreserved on nm1
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    Assert.assertEquals(3, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(0, schedulerApp1.getReservedContainers().size());

    rm1.close();
  }

  @Test
  public void testPendingResourcesConsideringUserLimit() throws Exception {
    // Set maximum capacity of A to 10
    CapacitySchedulerConfiguration newConf = new CapacitySchedulerConfiguration(
        conf);
    newConf.setUserLimitFactor(CapacitySchedulerConfiguration.ROOT + ".default",
        0.5f);
    newConf.setMaximumAMResourcePercentPerPartition(
        CapacitySchedulerConfiguration.ROOT + ".default", "", 1.0f);
    MockRM rm1 = new MockRM(newConf);

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue default, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("u1")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch 2nd app to queue default, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(4 * GB, rm1)
            .withAppName("app")
            .withUser("u2")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    // am1 asks 1 * 3G container
    am1.allocate("*", 3 * GB, 1, null);

    // am2 asks 4 * 5G container
    am2.allocate("*", 5 * GB, 4, null);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // Do node heartbeats one, we expect one container allocated reserved on nm1
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());

    // App1 will get 1 container reserved
    Assert.assertEquals(1, schedulerApp1.getReservedContainers().size());

    /*
     * Note that the behavior of appAttemptResourceUsage is different from queue's
     * For queue, used = actual-used + reserved
     * For app, used = actual-used.
     *
     * TODO (wangda): Need to make behaviors of queue/app's resource usage
     * consistent
     */
    Assert.assertEquals(2 * GB,
        schedulerApp1.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(3 * GB,
        schedulerApp1.getAppAttemptResourceUsage().getReserved()
            .getMemorySize());
    Assert.assertEquals(3 * GB,
        schedulerApp1.getAppAttemptResourceUsage().getPending()
            .getMemorySize());

    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());
    Assert.assertEquals(4 * GB,
        schedulerApp2.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        schedulerApp2.getAppAttemptResourceUsage().getReserved()
            .getMemorySize());
    Assert.assertEquals(5 * 4 * GB,
        schedulerApp2.getAppAttemptResourceUsage().getPending()
            .getMemorySize());

    LeafQueue lq = (LeafQueue) cs.getQueue("default");

    // UL = 8GB, so head room of u1 = 8GB - 2GB (AM) - 3GB (Reserved) = 3GB
    //                           u2 = 8GB - 4GB = 4GB
    // When not deduct reserved, total-pending = 3G (u1) + 4G (u2) = 7G
    //          deduct reserved, total-pending = 0G (u1) + 4G = 4G
    Assert.assertEquals(7 * GB, lq.getTotalPendingResourcesConsideringUserLimit(
        Resources.createResource(20 * GB), "", false).getMemorySize());
    Assert.assertEquals(4 * GB, lq.getTotalPendingResourcesConsideringUserLimit(
        Resources.createResource(20 * GB), "", true).getMemorySize());
    rm1.close();
  }


  @Test(timeout = 60000)
  public void testQueuePriorityOrdering() throws Exception {
    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);

    // Set ordering policy
    newConf.setQueueOrderingPolicy(CapacitySchedulerConfiguration.ROOT,
        CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);

    // Set maximum capacity of A to 20
    newConf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT + ".a", 20);
    newConf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".c", 1);
    newConf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".b", 2);
    newConf.setQueuePriority(CapacitySchedulerConfiguration.ROOT + ".a", 3);

    MockRM rm1 = new MockRM(newConf);

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 100 * GB);

    // launch an app to queue A, AM container should be launched in nm1
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch an app to queue B, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    // launch an app to queue C, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm1);

    // Each application asks 10 * 5GB containers
    am1.allocate("*", 5 * GB, 10, null);
    am2.allocate("*", 5 * GB, 10, null);
    am3.allocate("*", 5 * GB, 10, null);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp3 =
        cs.getApplicationAttempt(am3.getApplicationAttemptId());

    // container will be allocated to am1
    // App1 will get 2 container allocated (plus AM container)
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp3.getLiveContainers().size());

    // container will be allocated to am1 again,
    // App1 will get 3 container allocated (plus AM container)
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertEquals(3, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp3.getLiveContainers().size());

    // (Now usages of queues: a=12G (satisfied), b=2G, c=2G)

    // container will be allocated to am2 (since app1 reaches its guaranteed
    // capacity)
    // App2 will get 2 container allocated (plus AM container)
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertEquals(3, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp3.getLiveContainers().size());

    // Do this 3 times
    // container will be allocated to am2 (since app1 reaches its guaranteed
    // capacity)
    // App2 will get 2 container allocated (plus AM container)
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    Assert.assertEquals(3, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(5, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp3.getLiveContainers().size());

    // (Now usages of queues: a=12G (satisfied), b=22G (satisfied), c=2G))

    // Do this 10 times
    for (int i = 0; i < 10; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    Assert.assertEquals(3, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(5, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(11, schedulerApp3.getLiveContainers().size());

    // (Now usages of queues: a=12G (satisfied), b=22G (satisfied),
    // c=52G (satisfied and no pending))

    // Do this 20 times, we can only allocate 3 containers, 1 to A and 3 to B
    for (int i = 0; i < 20; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    Assert.assertEquals(4, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(6, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(11, schedulerApp3.getLiveContainers().size());

    // (Now usages of queues: a=17G (satisfied), b=27G (satisfied), c=52G))

    rm1.close();
  }



  @Test(timeout = 60000)
  public void testUserLimitAllocationMultipleContainers() throws Exception {
    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    // make sure an unlimited number of containers can be assigned,
    // overriding the default of 100 after YARN-8896
    newConf.set(MAX_ASSIGN_PER_HEARTBEAT, "-1");
    newConf.setUserLimit("root.c", 50);
    MockRM rm1 = new MockRM(newConf);

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 1000 * GB);

    // launch app from 1st user to queue C, AM container should be launched in nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user1")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch app from 2nd user to queue C, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user2")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    // Each application asks 1000 * 5GB containers
    am1.allocate("*", 5 * GB, 1000, null);
    am1.allocate("h1", 5 * GB, 1000, null);
    am1.allocate(NetworkTopology.DEFAULT_RACK, 5 * GB, 1000, null);

    // Each application asks 1000 * 5GB containers
    am2.allocate("*", 5 * GB, 1000, null);
    am2.allocate("h1", 5 * GB, 1000, null);
    am2.allocate(NetworkTopology.DEFAULT_RACK, 5 * GB, 1000, null);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    // container will be allocated to am1
    // App1 will get 2 container allocated (plus AM container)
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertEquals(101, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(100, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  @Test
  public void testActiveUsersWithOnlyPendingApps() throws Exception {

    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    newConf.setMaximumAMResourcePercentPerPartition(
        CapacitySchedulerConfiguration.ROOT + ".default", "", 0.2f);
    MockRM rm1 = new MockRM(newConf);

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);

    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("u1")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data3);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("u2")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("u3")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data1);

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("u4")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app4 = MockRMAppSubmitter.submit(rm1, data);

    // Each application asks 50 * 1GB containers
    am1.allocate("*", 1 * GB, 50, null);
    am2.allocate("*", 1 * GB, 50, null);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    for (int i = 0; i < 10; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      Thread.sleep(1000);
    }
    LeafQueue lq = (LeafQueue) cs.getQueue("default");
    UsersManager um = (UsersManager) lq.getAbstractUsersManager();

    Assert.assertEquals(4, um.getNumActiveUsers());
    Assert.assertEquals(2, um.getNumActiveUsersWithOnlyPendingApps());
    Assert.assertEquals(2, lq.getMetrics().getAppsPending());
    rm1.close();
  }

  @Test(timeout = 60000)
  public void testUnreserveWhenClusterResourceHasEmptyResourceType()
      throws Exception {
    /**
     * Test case:
     * Create a cluster with two nodes whose node resource both are
     * <8GB, 8core, 0>, create queue "a" whose max-resource is <8GB, 8 core, 0>,
     * submit app1 to queue "a" whose am use <1GB, 1 core, 0> and launch on nm1,
     * submit app2 to queue "b" whose am use <1GB, 1 core, 0> and launch on nm1,
     * app1 asks two <7GB, 1core> containers and nm1 do 1 heartbeat,
     * then scheduler reserves one container on nm1.
     *
     * After nm2 do next node heartbeat, scheduler should unreserve the reserved
     * container on nm1 then allocate a container on nm2.
     */
    CustomResourceTypesConfigurationProvider.
        initResourceTypes("resource1");
    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    newConf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);
    newConf
        .setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES, false);
    // Set maximum capacity of queue "a" to 50
    newConf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT + ".a", 50);
    MockRM rm1 = new MockRM(newConf);

    RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
    nodeLabelsManager.init(newConf);
    rm1.getRMContext().setNodeLabelManager(nodeLabelsManager);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue "a", AM container should be launched on nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue "b", AM container should be launched on nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockRM.launchAndRegisterAM(app2, rm1, nm1);

    am1.allocate("*", 7 * GB, 2, new ArrayList<ContainerId>());

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());

    // Do nm1 heartbeats 1 times, will reserve a container on nm1 for app1
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp1.getReservedContainers().size());

    // Do nm2 heartbeats 1 times, will unreserve a container on nm1
    // and allocate a container on nm2 for app1
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(0, schedulerApp1.getReservedContainers().size());

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testAllocationCannotBeBlockedWhenFormerQueueReachedItsLimit()
      throws Exception {
    /**
     * Queue structure:
     * <pre>
     *             Root
     *            /  |  \
     *           a   b   c
     *          10   20  70
     *                   |  \
     *                  c1   c2
     *           10(max=10)  90
     * </pre>
     * Test case:
     * Create a cluster with two nodes whose node resource both are
     * <10GB, 10core>, create queues as above, among them max-capacity of "c1"
     * is 10 and others are all 100, so that max-capacity of queue "c1" is
     * <2GB, 2core>,
     * submit app1 to queue "c1" and launch am1(resource=<1GB, 1 core>) on nm1,
     * submit app2 to queue "b" and launch am2(resource=<1GB, 1 core>) on nm1,
     * app1 and app2 both ask one <2GB, 1core> containers
     *
     * Now queue "c" has lower capacity percentage than queue "b", the
     * allocation sequence will be "a" -> "c" -> "b", queue "c1" has reached
     * queue limit so that requests of app1 should be pending
     *
     * After nm1 do 1 heartbeat, scheduler should allocate one container for
     * app2 on nm1.
     */
    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    newConf.setQueues(CapacitySchedulerConfiguration.ROOT + ".c",
        new String[] { "c1", "c2" });
    newConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".c.c1", 10);
    newConf
        .setMaximumCapacity(CapacitySchedulerConfiguration.ROOT + ".c.c1", 10);
    newConf.setCapacity(CapacitySchedulerConfiguration.ROOT + ".c.c2", 90);
    newConf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);

    MockRM rm1 = new MockRM(newConf);

    RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
    nodeLabelsManager.init(newConf);
    rm1.getRMContext().setNodeLabelManager(nodeLabelsManager);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB);

    // launch an app to queue "c1", AM container should be launched on nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue "b", AM container should be launched on nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm1);

    am1.allocate("*", 2 * GB, 1, new ArrayList<ContainerId>());
    am2.allocate("*", 2 * GB, 1, new ArrayList<ContainerId>());

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    // Do nm1 heartbeats 1 times, will allocate a container on nm1 for app2
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    rm1.drainEvents();
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  @Test(timeout = 60000)
  public void testContainerRejectionWhenAskBeyondDynamicMax()
      throws Exception {
    CapacitySchedulerConfiguration newConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getConfigurationWithMultipleQueues(conf);
    newConf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);
    newConf.set(CapacitySchedulerConfiguration.getQueuePrefix("root.a")
        + MAXIMUM_ALLOCATION_MB, "4096");

    MockRM rm1 = new MockRM(newConf);
    rm1.start();

    // before any node registered or before registration timeout,
    // submit an app beyond queue max leads to failure.
    boolean submitFailed = false;
    MockNM nm1 = rm1.registerNode("h1:1234", 2 * GB, 1);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    try {
      am1.allocate("*", 5 * GB, 1, null);
    } catch (InvalidResourceRequestException e) {
      submitFailed = true;
    }
    Assert.assertTrue(submitFailed);

    // Ask 4GB succeeded.
    am1.allocate("*", 4 * GB, 1, null);

    // Add a new node, now the cluster maximum should be refreshed to 3GB.
    CapacityScheduler cs = (CapacityScheduler)rm1.getResourceScheduler();
    cs.getNodeTracker().setForceConfiguredMaxAllocation(false);
    rm1.registerNode("h2:1234", 3 * GB, 1);

    // Now ask 4 GB will fail
    submitFailed = false;
    try {
      am1.allocate("*", 4 * GB, 1, null);
    } catch (InvalidResourceRequestException e) {
      submitFailed = true;
    }
    Assert.assertTrue(submitFailed);

    // But ask 3 GB succeeded.
    am1.allocate("*", 3 * GB, 1, null);

    rm1.close();
  }
}
