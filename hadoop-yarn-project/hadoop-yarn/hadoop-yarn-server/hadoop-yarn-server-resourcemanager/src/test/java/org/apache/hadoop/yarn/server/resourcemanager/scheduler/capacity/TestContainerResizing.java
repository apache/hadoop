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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica
    .FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimplePlacementSet;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestContainerResizing {
  private static final Log LOG = LogFactory.getLog(TestContainerResizing.class);
  private final int GB = 1024;

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  class MyScheduler extends CapacityScheduler {
    /*
     * A Mock Scheduler to simulate the potential effect of deadlock between:
     * 1. The AbstractYarnScheduler.decreaseContainers() call (from
     *    ApplicationMasterService thread)
     * 2. The CapacityScheduler.allocateContainersToNode() call (from the
     *    scheduler thread)
     */
    MyScheduler() {
      super();
    }

    @Override
    protected void decreaseContainers(
        List<UpdateContainerRequest> decreaseRequests,
        SchedulerApplicationAttempt attempt) {
      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        LOG.debug("Thread interrupted.");
      }
      super.decreaseContainers(decreaseRequests, attempt);
    }

    @Override
    public CSAssignment allocateContainersToNode(
        PlacementSet<FiCaSchedulerNode> ps, boolean withNodeHeartbeat) {
      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        LOG.debug("Thread interrupted.");
      }
      return super.allocateContainersToNode(ps, withNodeHeartbeat);
    }
  }

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test
  public void testSimpleIncreaseContainer() throws Exception {
    /**
     * Application has a container running, and the node has enough available
     * resource. Add a increase request to see if container will be increased
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    ContainerId containerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    sentRMContainerLaunched(rm1, containerId1);
    // am1 asks to change its AM container from 1GB to 3GB
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId1,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(3 * GB), null)));

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    checkPendingResource(rm1, "default", 2 * GB, null);
    Assert.assertEquals(2 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // NM1 do 1 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // Pending resource should be deducted
    checkPendingResource(rm1, "default", 0 * GB, null);
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    verifyContainerIncreased(am1.allocate(null, null), containerId1, 3 * GB);
    verifyAvailableResourceOfSchedulerNode(rm1, nm1.getNodeId(), 17 * GB);

    rm1.close();
  }

  @Test
  public void testSimpleDecreaseContainer() throws Exception {
    /**
     * Application has a container running, try to decrease the container and
     * check queue's usage and container resource will be updated.
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(3 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    checkUsedResource(rm1, "default", 3 * GB, null);
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    ContainerId containerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    sentRMContainerLaunched(rm1, containerId1);

    // am1 asks to change its AM container from 1GB to 3GB
    AllocateResponse response = am1.sendContainerResizingRequest(Arrays
        .asList(UpdateContainerRequest
            .newInstance(0, containerId1,
                ContainerUpdateType.DECREASE_RESOURCE,
                Resources.createResource(1 * GB), null)));

    verifyContainerDecreased(response, containerId1, 1 * GB);
    checkUsedResource(rm1, "default", 1 * GB, null);
    Assert.assertEquals(1 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    // Check if decreased containers added to RMNode
    RMNodeImpl rmNode =
        (RMNodeImpl) rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    Collection<Container> decreasedContainers =
        rmNode.getToBeDecreasedContainers();
    boolean rmNodeReceivedDecreaseContainer = false;
    for (Container c : decreasedContainers) {
      if (c.getId().equals(containerId1)
          && c.getResource().equals(Resources.createResource(1 * GB))) {
        rmNodeReceivedDecreaseContainer = true;
      }
    }
    Assert.assertTrue(rmNodeReceivedDecreaseContainer);

    rm1.close();
  }

  @Test
  public void testSimpleIncreaseRequestReservation() throws Exception {
    /**
     * Application has two containers running, try to increase one of then, node
     * doesn't have enough resource, so the increase request will be reserved.
     * Check resource usage after container reserved, finish a container, the
     * reserved container should be allocated.
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    // Allocate two more containers
    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(2 * GB), 1)),
        null);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm1, containerId2,
        RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am1.allocate(null, null);
    sentRMContainerLaunched(rm1, containerId2);

    ContainerId containerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    sentRMContainerLaunched(rm1, containerId1);


    // am1 asks to change its AM container from 1GB to 3GB
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId1,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(7 * GB), null)));

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // NM1 do 1 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    RMContainer rmContainer1 = app.getLiveContainersMap().get(containerId1);
    
    /* Check reservation statuses */
    // Increase request should be reserved
    Assert.assertTrue(rmContainer1.hasIncreaseReservation());
    Assert.assertEquals(6 * GB, rmContainer1.getReservedResource().getMemorySize());
    Assert.assertFalse(app.getReservedContainers().isEmpty());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Pending resource will not be changed since it's not satisfied
    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 9 * GB, null);
    Assert.assertEquals(9 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());

    // Complete one container and do another allocation
    am1.allocate(null, Arrays.asList(containerId2));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // Now container should be increased
    verifyContainerIncreased(am1.allocate(null, null), containerId1, 7 * GB);
    
    /* Check statuses after reservation satisfied */
    // Increase request should be unreserved
    Assert.assertFalse(rmContainer1.hasIncreaseReservation());
    Assert.assertTrue(app.getReservedContainers().isEmpty());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Pending resource will be changed since it's satisfied
    checkPendingResource(rm1, "default", 0 * GB, null);
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 7 * GB, null);
    Assert.assertEquals(7 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    Assert.assertEquals(7 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    verifyAvailableResourceOfSchedulerNode(rm1, nm1.getNodeId(), 1 * GB);

    rm1.close();
  }

  @Test
  public void testIncreaseRequestWithNoHeadroomLeft() throws Exception {
    /**
     * Application has two containers running, try to increase one of them, the
     * requested amount exceeds user's headroom for the queue.
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    // Allocate 1 container
    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1), "*",
                Resources.createResource(2 * GB), 1)),
        null);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm1, containerId2,
            RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am1.allocate(null, null);
    sentRMContainerLaunched(rm1, containerId2);

    // am1 asks to change container2 from 2GB to 8GB, which will exceed user
    // limit
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId2,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(8 * GB), null)));

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // NM1 do 1 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    RMContainer rmContainer1 = app.getLiveContainersMap().get(containerId2);

    /* Check reservation statuses */
    // Increase request should *NOT* be reserved as it exceeds user limit
    Assert.assertFalse(rmContainer1.hasIncreaseReservation());
    Assert.assertTrue(app.getReservedContainers().isEmpty());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Pending resource will not be changed since it's not satisfied
    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will *NOT* be updated
    checkUsedResource(rm1, "default", 3 * GB, null);
    Assert.assertEquals(3 * GB, ((LeafQueue) cs.getQueue("default"))
            .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    rm1.close();
  }

  @Test
  public void testExcessiveReservationWhenCancelIncreaseRequest()
      throws Exception {
    /**
     * Application has two containers running, try to increase one of then, node
     * doesn't have enough resource, so the increase request will be reserved.
     * Check resource usage after container reserved, finish a container &
     * cancel the increase request, reservation should be cancelled
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    // Allocate two more containers
    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(2 * GB), 1)),
        null);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm1, containerId2,
        RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am1.allocate(null, null);
    sentRMContainerLaunched(rm1, containerId2);

    ContainerId containerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    sentRMContainerLaunched(rm1, containerId1);

    // am1 asks to change its AM container from 1GB to 7GB
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId1,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(7 * GB), null)));

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // NM1 do 1 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    RMContainer rmContainer1 = app.getLiveContainersMap().get(containerId1);
    
    /* Check reservation statuses */
    // Increase request should be reserved
    Assert.assertTrue(rmContainer1.hasIncreaseReservation());
    Assert.assertEquals(6 * GB, rmContainer1.getReservedResource().getMemorySize());
    Assert.assertFalse(app.getReservedContainers().isEmpty());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Pending resource will not be changed since it's not satisfied
    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 9 * GB, null);
    Assert.assertEquals(9 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());

    // Complete one container and cancel increase request (via send a increase
    // request, make target_capacity=existing_capacity)
    am1.allocate(null, Arrays.asList(containerId2));
    // am1 asks to change its AM container from 1G to 1G (cancel the increase
    // request actually)
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId1,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(1 * GB), null)));
    // Trigger a node heartbeat..
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    
    /* Check statuses after reservation satisfied */
    // Increase request should be unreserved
    Assert.assertTrue(app.getReservedContainers().isEmpty());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertFalse(rmContainer1.hasIncreaseReservation());
    // Pending resource will be changed since it's satisfied
    checkPendingResource(rm1, "default", 0 * GB, null);
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 1 * GB, null);
    Assert.assertEquals(1 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    Assert.assertEquals(1 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    rm1.close();
  }

  @Test
  public void testExcessiveReservationWhenDecreaseSameContainer()
      throws Exception {
    /**
     * Very similar to testExcessiveReservationWhenCancelIncreaseRequest, after
     * the increase request reserved, it decreases the reserved container,
     * container should be decreased and reservation will be cancelled
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(2 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    // Allocate two more containers
    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(2 * GB), 1)),
        null);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm1, containerId2,
        RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am1.allocate(null, null);
    sentRMContainerLaunched(rm1, containerId2);

    ContainerId containerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    sentRMContainerLaunched(rm1, containerId1);


    // am1 asks to change its AM container from 2GB to 8GB
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId1,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(8 * GB), null)));

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // NM1 do 1 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    RMContainer rmContainer1 = app.getLiveContainersMap().get(containerId1);
    
    /* Check reservation statuses */
    // Increase request should be reserved
    Assert.assertTrue(rmContainer1.hasIncreaseReservation());
    Assert.assertEquals(6 * GB, rmContainer1.getReservedResource().getMemorySize());
    Assert.assertFalse(app.getReservedContainers().isEmpty());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Pending resource will not be changed since it's not satisfied
    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 10 * GB, null);
    Assert.assertEquals(10 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(4 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());

    // Complete one container and cancel increase request (via send a increase
    // request, make target_capacity=existing_capacity)
    am1.allocate(null, Arrays.asList(containerId2));
    // am1 asks to change its AM container from 2G to 1G (decrease)
    am1.sendContainerResizingRequest(Arrays.asList(
        UpdateContainerRequest
            .newInstance(0, containerId1,
                ContainerUpdateType.INCREASE_RESOURCE,
                Resources.createResource(1 * GB), null)));
    // Trigger a node heartbeat..
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    
    /* Check statuses after reservation satisfied */
    // Increase request should be unreserved
    Assert.assertTrue(app.getReservedContainers().isEmpty());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertFalse(rmContainer1.hasIncreaseReservation());
    // Pending resource will be changed since it's satisfied
    checkPendingResource(rm1, "default", 0 * GB, null);
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 1 * GB, null);
    Assert.assertEquals(1 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    Assert.assertEquals(1 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    rm1.close();
  }

  @Test
  public void testIncreaseContainerUnreservedWhenContainerCompleted()
      throws Exception {
    /**
     * App has two containers on the same node (node.resource = 8G), container1
     * = 2G, container2 = 2G. App asks to increase container2 to 8G.
     *
     * So increase container request will be reserved. When app releases
     * container2, reserved part should be released as well.
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    // Allocate two more containers
    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(2 * GB), 1)),
        null);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm1, containerId2,
        RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am1.allocate(null, null);
    sentRMContainerLaunched(rm1, containerId2);
    rm1.waitForState(Arrays.asList(nm1, nm2), containerId2,
         RMContainerState.RUNNING);

    // am1 asks to change its AM container from 2GB to 8GB
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId2,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(8 * GB), null)));

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // NM1 do 1 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    RMContainer rmContainer2 = app.getLiveContainersMap().get(containerId2);
    
    /* Check reservation statuses */
    // Increase request should be reserved
    Assert.assertTrue(rmContainer2.hasIncreaseReservation());
    Assert.assertEquals(6 * GB, rmContainer2.getReservedResource().getMemorySize());
    Assert.assertFalse(app.getReservedContainers().isEmpty());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Pending resource will not be changed since it's not satisfied
    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 9 * GB, null);
    Assert.assertEquals(9 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());

    // Complete container2, container will be unreserved and completed
    am1.allocate(null, Arrays.asList(containerId2));
    
    /* Check statuses after reservation satisfied */
    // Increase request should be unreserved
    Assert.assertTrue(app.getReservedContainers().isEmpty());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertFalse(rmContainer2.hasIncreaseReservation());
    // Pending resource will be changed since it's satisfied
    checkPendingResource(rm1, "default", 0 * GB, null);
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 1 * GB, null);
    Assert.assertEquals(1 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    Assert.assertEquals(1 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    rm1.close();
  }

  @Test
  public void testIncreaseContainerUnreservedWhenApplicationCompleted()
      throws Exception {
    /**
     * Similar to testIncreaseContainerUnreservedWhenContainerCompleted, when
     * application finishes, reserved increase container should be cancelled
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());

    // Allocate two more containers
    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(2 * GB), 1)),
        null);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(
        rm1.waitForState(nm1, containerId2, RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am1.allocate(null, null);
    sentRMContainerLaunched(rm1, containerId2);

    // am1 asks to change its AM container from 2GB to 8GB
    am1.sendContainerResizingRequest(Arrays.asList(
            UpdateContainerRequest
                .newInstance(0, containerId2,
                    ContainerUpdateType.INCREASE_RESOURCE,
                    Resources.createResource(8 * GB), null)));

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // NM1 do 1 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    RMContainer rmContainer2 = app.getLiveContainersMap().get(containerId2);
    
    /* Check reservation statuses */
    // Increase request should be reserved
    Assert.assertTrue(rmContainer2.hasIncreaseReservation());
    Assert.assertEquals(6 * GB, rmContainer2.getReservedResource().getMemorySize());
    Assert.assertFalse(app.getReservedContainers().isEmpty());
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    // Pending resource will not be changed since it's not satisfied
    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 9 * GB, null);
    Assert.assertEquals(9 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());

    // Kill the application
    cs.handle(new AppAttemptRemovedSchedulerEvent(am1.getApplicationAttemptId(),
        RMAppAttemptState.KILLED, false));

    /* Check statuses after reservation satisfied */
    // Increase request should be unreserved
    Assert.assertTrue(app.getReservedContainers().isEmpty());
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertFalse(rmContainer2.hasIncreaseReservation());
    // Pending resource will be changed since it's satisfied
    checkPendingResource(rm1, "default", 0 * GB, null);
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 0 * GB, null);
    // User will be removed
    Assert.assertNull(((LeafQueue) cs.getQueue("default")).getUser("user"));
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    rm1.close();
  }

  private void allocateAndLaunchContainers(MockAM am, MockNM nm, MockRM rm,
      int nContainer, int mem, int priority, int startContainerId)
          throws Exception {
    am.allocate(Arrays
        .asList(ResourceRequest.newInstance(Priority.newInstance(priority), "*",
            Resources.createResource(mem), nContainer)),
        null);
    ContainerId lastContainerId = ContainerId.newContainerId(
        am.getApplicationAttemptId(), startContainerId + nContainer - 1);
    Assert.assertTrue(rm.waitForState(nm, lastContainerId,
        RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am.allocate(null, null);

    for (int cId = startContainerId; cId < startContainerId
        + nContainer; cId++) {
      sentRMContainerLaunched(rm,
          ContainerId.newContainerId(am.getApplicationAttemptId(), cId));
      rm.waitForState(nm,
          ContainerId.newContainerId(am.getApplicationAttemptId(), cId),
          RMContainerState.RUNNING);
    }
  }

  @Test
  public void testOrderOfIncreaseContainerRequestAllocation()
      throws Exception {
    /**
     * There're multiple containers need to be increased, check container will
     * be increase sorted by priority, if priority is same, smaller containerId
     * container will get preferred
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());
    ApplicationAttemptId attemptId = am1.getApplicationAttemptId();

    // Container 2, 3 (priority=3)
    allocateAndLaunchContainers(am1, nm1, rm1, 2, 1 * GB, 3, 2);

    // Container 4, 5 (priority=2)
    allocateAndLaunchContainers(am1, nm1, rm1, 2, 1 * GB, 2, 4);

    // Container 6, 7 (priority=4)
    allocateAndLaunchContainers(am1, nm1, rm1, 2, 1 * GB, 4, 6);

    // am1 asks to change its container[2-7] from 1G to 2G
    List<UpdateContainerRequest> increaseRequests = new ArrayList<>();
    for (int cId = 2; cId <= 7; cId++) {
      ContainerId containerId =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), cId);
      increaseRequests.add(UpdateContainerRequest
          .newInstance(0, containerId,
              ContainerUpdateType.INCREASE_RESOURCE,
              Resources.createResource(2 * GB), null));
    }
    am1.sendContainerResizingRequest(increaseRequests);

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // Get rmNode1
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    // assignContainer, container-4/5/2 increased (which has highest priority OR
    // earlier allocated)
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    AllocateResponse allocateResponse = am1.allocate(null, null);
    Assert.assertEquals(3, allocateResponse.getUpdatedContainers().size());
    verifyContainerIncreased(allocateResponse,
        ContainerId.newContainerId(attemptId, 4), 2 * GB);
    verifyContainerIncreased(allocateResponse,
        ContainerId.newContainerId(attemptId, 5), 2 * GB);
    verifyContainerIncreased(allocateResponse,
        ContainerId.newContainerId(attemptId, 2), 2 * GB);

    /* Check statuses after allocation */
    // There're still 3 pending increase requests
    checkPendingResource(rm1, "default", 3 * GB, null);
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 10 * GB, null);
    Assert.assertEquals(10 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    Assert.assertEquals(10 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    rm1.close();
  }

  @Test
  public void testIncreaseContainerRequestGetPreferrence()
      throws Exception {
    /**
     * There're multiple containers need to be increased, and there're several
     * container allocation request, scheduler will try to increase container
     * before allocate new containers
     */
    MockRM rm1 = new MockRM() {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB);

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm1, app1.getApplicationId());
    ApplicationAttemptId attemptId = am1.getApplicationAttemptId();

    // Container 2, 3 (priority=3)
    allocateAndLaunchContainers(am1, nm1, rm1, 2, 1 * GB, 3, 2);

    // Container 4, 5 (priority=2)
    allocateAndLaunchContainers(am1, nm1, rm1, 2, 1 * GB, 2, 4);

    // Container 6, 7 (priority=4)
    allocateAndLaunchContainers(am1, nm1, rm1, 2, 1 * GB, 4, 6);

    // am1 asks to change its container[2-7] from 1G to 2G
    List<UpdateContainerRequest> increaseRequests = new ArrayList<>();
    for (int cId = 2; cId <= 7; cId++) {
      ContainerId containerId =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), cId);
      increaseRequests.add(UpdateContainerRequest
          .newInstance(0, containerId,
              ContainerUpdateType.INCREASE_RESOURCE,
              Resources.createResource(2 * GB), null));
    }
    am1.sendContainerResizingRequest(increaseRequests);

    checkPendingResource(rm1, "default", 6 * GB, null);
    Assert.assertEquals(6 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());

    // Get rmNode1
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    // assignContainer, container-4/5/2 increased (which has highest priority OR
    // earlier allocated)
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    AllocateResponse allocateResponse = am1.allocate(null, null);
    Assert.assertEquals(3, allocateResponse.getUpdatedContainers().size());
    verifyContainerIncreased(allocateResponse,
        ContainerId.newContainerId(attemptId, 4), 2 * GB);
    verifyContainerIncreased(allocateResponse,
        ContainerId.newContainerId(attemptId, 5), 2 * GB);
    verifyContainerIncreased(allocateResponse,
        ContainerId.newContainerId(attemptId, 2), 2 * GB);

    /* Check statuses after allocation */
    // There're still 3 pending increase requests
    checkPendingResource(rm1, "default", 3 * GB, null);
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getPending().getMemorySize());
    // Queue/user/application's usage will be updated
    checkUsedResource(rm1, "default", 10 * GB, null);
    Assert.assertEquals(10 * GB, ((LeafQueue) cs.getQueue("default"))
        .getUser("user").getUsed().getMemorySize());
    Assert.assertEquals(0 * GB,
        app.getAppAttemptResourceUsage().getReserved().getMemorySize());
    Assert.assertEquals(10 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());

    rm1.close();
  }

  @Test (timeout = 60000)
  public void testDecreaseContainerWillNotDeadlockContainerAllocation()
      throws Exception {
    // create and start MockRM with our MyScheduler
    MockRM rm = new MockRM() {
      @Override
      public ResourceScheduler createScheduler() {
        CapacityScheduler cs = new MyScheduler();
        cs.setConf(conf);
        return cs;
      }
    };
    rm.start();
    // register a node
    MockNM nm = rm.registerNode("h1:1234", 20 * GB);
    // submit an application -> app1
    RMApp app1 = rm.submitApp(3 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm);
    // making sure resource is allocated
    checkUsedResource(rm, "default", 3 * GB, null);
    FiCaSchedulerApp app = TestUtils.getFiCaSchedulerApp(
        rm, app1.getApplicationId());
    Assert.assertEquals(3 * GB,
        app.getAppAttemptResourceUsage().getUsed().getMemorySize());
    // making sure container is launched
    ContainerId containerId1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    sentRMContainerLaunched(rm, containerId1);
    // submit allocation request for a new container
    am1.allocate(Collections.singletonList(ResourceRequest.newInstance(
        Priority.newInstance(1), "*", Resources.createResource(2 * GB), 1)),
        null);
    // nm reports status update and triggers container allocation
    nm.nodeHeartbeat(true);
    // *In the mean time*, am1 asks to decrease its AM container resource from
    // 3GB to 1GB
    AllocateResponse response = am1.sendContainerResizingRequest(
        Collections.singletonList(UpdateContainerRequest
            .newInstance(0, containerId1,
                ContainerUpdateType.DECREASE_RESOURCE,
                Resources.createResource(GB), null)));
    // verify that the containe resource is decreased
    verifyContainerDecreased(response, containerId1, GB);

    rm.close();
  }

  private void checkPendingResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(memory,
        queue.getQueueResourceUsage()
            .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
            .getMemorySize());
  }

  private void checkUsedResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(memory,
        queue.getQueueResourceUsage()
            .getUsed(label == null ? RMNodeLabelsManager.NO_LABEL : label)
            .getMemorySize());
  }

  private void verifyContainerIncreased(AllocateResponse response,
      ContainerId containerId, int mem) {
    List<UpdatedContainer> increasedContainers =
        response.getUpdatedContainers();
    boolean found = false;
    for (UpdatedContainer c : increasedContainers) {
      if (c.getContainer().getId().equals(containerId)) {
        found = true;
        Assert.assertEquals(ContainerUpdateType.INCREASE_RESOURCE,
            c.getUpdateType());
        Assert.assertEquals(mem,
            c.getContainer().getResource().getMemorySize());
      }
    }
    if (!found) {
      Assert.fail("Container not increased: containerId=" + containerId);
    }
  }

  private void verifyContainerDecreased(AllocateResponse response,
      ContainerId containerId, int mem) {
    List<UpdatedContainer> decreasedContainers =
        response.getUpdatedContainers();
    boolean found = false;
    for (UpdatedContainer c : decreasedContainers) {
      if (c.getContainer().getId().equals(containerId)) {
        found = true;
        Assert.assertEquals(ContainerUpdateType.DECREASE_RESOURCE,
            c.getUpdateType());
        Assert.assertEquals(mem,
            c.getContainer().getResource().getMemorySize());
      }
    }
    if (!found) {
      Assert.fail("Container not decreased: containerId=" + containerId);
    }
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

  private void verifyAvailableResourceOfSchedulerNode(MockRM rm, NodeId nodeId,
      int expectedMemory) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    SchedulerNode node = cs.getNode(nodeId);
    Assert
        .assertEquals(expectedMemory, node.getUnallocatedResource().getMemorySize());
  }
}
