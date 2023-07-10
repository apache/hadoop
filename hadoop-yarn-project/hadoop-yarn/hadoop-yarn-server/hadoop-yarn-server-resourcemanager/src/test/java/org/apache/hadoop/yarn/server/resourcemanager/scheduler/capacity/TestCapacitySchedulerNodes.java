/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.appHelper;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.nodeUpdate;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.registerNode;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.stopResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.NULL_UPDATE_REQUESTS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerNodes {

  private ResourceManager resourceManager = null;

  @Before
  public void setUp() throws Exception {
    resourceManager = createResourceManager();
  }

  @After
  public void tearDown() throws Exception {
    stopResourceManager(resourceManager);
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

    ApplicationAttemptId appAttemptId = appHelper(rm, cs, 100, 1, "default", "user");

    // Verify the blacklist can be updated independent of requesting containers
    cs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(), null,
        Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null, NULL_UPDATE_REQUESTS);
    Assert.assertTrue(cs.getApplicationAttempt(appAttemptId)
        .isPlaceBlacklisted(host));
    cs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(), null,
        Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host), NULL_UPDATE_REQUESTS);
    Assert.assertFalse(cs.getApplicationAttempt(appAttemptId)
        .isPlaceBlacklisted(host));
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
    Assert.assertEquals("Queue Name should be a", "a",
        queueInfoA.getQueueName());
    Assert.assertEquals("Queue Path should be root.a", "root.a",
        queueInfoA.getQueuePath());
    Assert.assertEquals("Default Node Label Expression should be x", "x",
        queueInfoA.getDefaultNodeLabelExpression());

    QueueInfo queueInfoB = cs.getQueueInfo("b", true, false);
    Assert.assertEquals("Queue Name should be b", "b",
        queueInfoB.getQueueName());
    Assert.assertEquals("Queue Path should be root.b", "root.b",
        queueInfoB.getQueuePath());
    Assert.assertEquals("Default Node Label Expression should be y", "y",
        queueInfoB.getDefaultNodeLabelExpression());
    cs.stop();
  }

  @Test
  public void testRemovedNodeDecommissioningNode() throws Exception {
    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register nodemanager
    NodeManager nm = registerNode(resourceManager, "host_decom", 1234, 2345,
        NetworkTopology.DEFAULT_RACK, Resources.createResource(8 * GB, 4),
        mockNodeStatus);

    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);

    // force remove the node to simulate race condition
    ((CapacityScheduler) resourceManager.getResourceScheduler()).getNodeTracker().
        removeNode(nm.getNodeId());
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

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node
    String host0 = "host_0";
    NodeManager nm0 = registerNode(resourceManager, host0, 1234, 2345,
        NetworkTopology.DEFAULT_RACK, Resources.createResource(8 * GB, 4),
        mockNodeStatus);
    // ResourceRequest priorities
    Priority priority0 = Priority.newInstance(0);

    // Submit an application
    Application application0 =
        new Application("user_0", "a1", resourceManager);
    application0.submit();

    application0.addNodeManager(host0, 1234, nm0);

    Resource capability00 = Resources.createResource(1 * GB, 1);
    application0.addResourceRequestSpec(priority0, capability00);

    Task task00 =
        new Task(application0, priority0, new String[]{host0});
    application0.addTask(task00);

    // Send resource requests to the scheduler
    application0.schedule();

    nodeUpdate(resourceManager, nm0);
    // Kick off another heartbeat with the node state mocked to decommissioning
    // This should update the schedulernodes to have 0 available resource
    RMNode spyNode =
        Mockito.spy(resourceManager.getRMContext().getRMNodes()
            .get(nm0.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));

    // Get allocations from the scheduler
    application0.schedule();

    // Check the used resource is 1 GB 1 core
    Assert.assertEquals(1 * GB, nm0.getUsed().getMemorySize());
    Resource usedResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm0.getNodeId()).getAllocatedResource();
    Assert.assertEquals("Used Resource Memory Size should be 1GB", 1 * GB,
        usedResource.getMemorySize());
    Assert.assertEquals("Used Resource Virtual Cores should be 1", 1,
        usedResource.getVirtualCores());
    // Check total resource of scheduler node is also changed to 1 GB 1 core
    Resource totalResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm0.getNodeId()).getTotalResource();
    Assert.assertEquals("Total Resource Memory Size should be 1GB", 1 * GB,
        totalResource.getMemorySize());
    Assert.assertEquals("Total Resource Virtual Cores should be 1", 1,
        totalResource.getVirtualCores());
    // Check the available resource is 0/0
    Resource availableResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm0.getNodeId()).getUnallocatedResource();
    Assert.assertEquals("Available Resource Memory Size should be 0", 0,
        availableResource.getMemorySize());
    Assert.assertEquals("Available Resource Memory Size should be 0", 0,
        availableResource.getVirtualCores());
    // Kick off another heartbeat where the RMNodeResourceUpdateEvent would
    // be skipped for DECOMMISSIONING state since the total resource is
    // already equal to used resource from the previous heartbeat.
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));
    verify(mockDispatcher, times(4)).getEventHandler();
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
    RMApp app = MockRMAppSubmitter.submitWithMemory(100, rm);
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

}
