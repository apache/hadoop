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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.appHelper;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.createResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.nodeUpdate;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.registerNode;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.stopResourceManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.NULL_UPDATE_REQUESTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerNodes {

  private ResourceManager resourceManager = null;

  @BeforeEach
  public void setUp() throws Exception {
    resourceManager = createResourceManager();
  }

  @AfterEach
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

    assertEquals(6 * GB, cs.getClusterResource().getMemorySize());

    // reconnect n1 with downgraded memory
    n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 1);
    cs.handle(new NodeRemovedSchedulerEvent(n1));
    cs.handle(new NodeAddedSchedulerEvent(n1));

    assertEquals(4 * GB, cs.getClusterResource().getMemorySize());
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
    assertTrue(cs.getApplicationAttempt(appAttemptId)
        .isPlaceBlacklisted(host));
    cs.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(), null,
        Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host), NULL_UPDATE_REQUESTS);
    assertFalse(cs.getApplicationAttempt(appAttemptId)
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
    conf.setDefaultNodeLabelExpression(new QueuePath("root.a"), " x");
    conf.setDefaultNodeLabelExpression(new QueuePath("root.b"), " y ");
    cs.setConf(new YarnConfiguration());
    cs.setRMContext(resourceManager.getRMContext());
    cs.init(conf);
    cs.start();

    QueueInfo queueInfoA = cs.getQueueInfo("a", true, false);
    assertEquals("a", queueInfoA.getQueueName(),
        "Queue Name should be a");
    assertEquals("root.a", queueInfoA.getQueuePath(),
        "Queue Path should be root.a");
    assertEquals("x", queueInfoA.getDefaultNodeLabelExpression(),
        "Default Node Label Expression should be x");

    QueueInfo queueInfoB = cs.getQueueInfo("b", true, false);
    assertEquals("b", queueInfoB.getQueueName(),
        "Queue Name should be b");
    assertEquals("root.b", queueInfoB.getQueuePath(),
        "Queue Path should be root.b");
    assertEquals("y", queueInfoB.getDefaultNodeLabelExpression(),
        "Default Node Label Expression should be y");
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
        spy(resourceManager.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));
  }

  @Test
  public void testResourceUpdateDecommissioningNode() throws Exception {
    // Mock the RMNodeResourceUpdate event handler to update SchedulerNode
    // to have 0 available resource
    RMContext spyContext = spy(resourceManager.getRMContext());
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
    doReturn(mockDispatcher).when(spyContext).getDispatcher();
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
        spy(resourceManager.getRMContext().getRMNodes()
            .get(nm0.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));

    // Get allocations from the scheduler
    application0.schedule();

    // Check the used resource is 1 GB 1 core
    assertEquals(1 * GB, nm0.getUsed().getMemorySize());
    Resource usedResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm0.getNodeId()).getAllocatedResource();
    assertEquals(1 * GB, usedResource.getMemorySize(),
        "Used Resource Memory Size should be 1GB");
    assertEquals(1, usedResource.getVirtualCores(),
        "Used Resource Virtual Cores should be 1");
    // Check total resource of scheduler node is also changed to 1 GB 1 core
    Resource totalResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm0.getNodeId()).getTotalResource();
    assertEquals(1 * GB, totalResource.getMemorySize(),
        "Total Resource Memory Size should be 1GB");
    assertEquals(1, totalResource.getVirtualCores(),
        "Total Resource Virtual Cores should be 1");
    // Check the available resource is 0/0
    Resource availableResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm0.getNodeId()).getUnallocatedResource();
    assertEquals(0, availableResource.getMemorySize(),
        "Available Resource Memory Size should be 0");
    assertEquals(0, availableResource.getVirtualCores(),
        "Available Resource Memory Size should be 0");
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
