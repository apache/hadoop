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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMSecretManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestContainerAllocation {

  private static final Log LOG = LogFactory
      .getLog(TestContainerAllocation.class);

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

  @Test(timeout = 3000000)
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
    RMApp app1 = rm.submitApp(128);

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
    RMApp app1 = rm1.submitApp(200);
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
    RMApp app1 = rm1.submitApp(200);
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
    RMApp app2 = rm1.submitApp(200, logAggregationContext);
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
            NodeId nodeId, String appSubmitter, Resource capability,
            Priority priority, long createTime,
            LogAggregationContext logAggregationContext, String nodeLabelExp, ContainerType containerType) {
          numRetries++;
          return super.createContainerToken(containerId, nodeId, appSubmitter,
              capability, priority, createTime, logAggregationContext,
              nodeLabelExp, containerType);
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
      app1 = rm1.submitApp(200);
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
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    
    // launch another app to queue, AM container should be launched in nm1
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "default");
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
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm1
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "default");
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
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm1
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "default");
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
}
