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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;

public class TestSchedulerHealth {

  private ResourceManager resourceManager;

  public void setup() {
    DefaultMetricsSystem.setMiniClusterMode(true);
    resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };

    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager()
      .rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
  }

  @Test
  public void testCounts() {

    SchedulerHealth sh = new SchedulerHealth();
    int value = 1;
    for (int i = 0; i < 2; ++i) {
      sh.updateSchedulerPreemptionCounts(value);
      sh.updateSchedulerAllocationCounts(value);
      sh.updateSchedulerReservationCounts(value);
      sh.updateSchedulerReleaseCounts(value);

      assertEquals(value, sh.getAllocationCount().longValue());
      assertEquals(value, sh.getReleaseCount().longValue());
      assertEquals(value, sh.getReservationCount().longValue());
      assertEquals(value, sh.getPreemptionCount().longValue());

      assertEquals(value * (i + 1), sh.getAggregateAllocationCount()
        .longValue());
      assertEquals(value * (i + 1), sh.getAggregateReleaseCount()
        .longValue());
      assertEquals(value * (i + 1), sh.getAggregateReservationCount()
        .longValue());
      assertEquals(value * (i + 1), sh.getAggregatePreemptionCount()
        .longValue());

    }
  }

  @Test
  public void testOperationDetails() {

    SchedulerHealth sh = new SchedulerHealth();
    long now = Time.now();
    sh.updateRelease(now, NodeId.newInstance("testhost", 1234),
      ContainerId.fromString("container_1427562107907_0002_01_000001"),
      "testqueue");
    assertEquals("container_1427562107907_0002_01_000001", sh
      .getLastReleaseDetails().getContainerId().toString());
    assertEquals("testhost:1234", sh.getLastReleaseDetails().getNodeId()
      .toString());
    assertEquals("testqueue", sh.getLastReleaseDetails().getQueue());
    assertEquals(now, sh.getLastReleaseDetails().getTimestamp());
    assertEquals(0, sh.getLastSchedulerRunTime());

    now = Time.now();
    sh.updateReservation(now, NodeId.newInstance("testhost1", 1234),
      ContainerId.fromString("container_1427562107907_0003_01_000001"),
      "testqueue1");
    assertEquals("container_1427562107907_0003_01_000001", sh
      .getLastReservationDetails().getContainerId().toString());
    assertEquals("testhost1:1234", sh.getLastReservationDetails()
      .getNodeId().toString());
    assertEquals("testqueue1", sh.getLastReservationDetails().getQueue());
    assertEquals(now, sh.getLastReservationDetails().getTimestamp());
    assertEquals(0, sh.getLastSchedulerRunTime());

    now = Time.now();
    sh.updateAllocation(now, NodeId.newInstance("testhost2", 1234),
      ContainerId.fromString("container_1427562107907_0004_01_000001"),
      "testqueue2");
    assertEquals("container_1427562107907_0004_01_000001", sh
      .getLastAllocationDetails().getContainerId().toString());
    assertEquals("testhost2:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    assertEquals("testqueue2", sh.getLastAllocationDetails().getQueue());
    assertEquals(now, sh.getLastAllocationDetails().getTimestamp());
    assertEquals(0, sh.getLastSchedulerRunTime());

    now = Time.now();
    sh.updatePreemption(now, NodeId.newInstance("testhost3", 1234),
      ContainerId.fromString("container_1427562107907_0005_01_000001"),
      "testqueue3");
    assertEquals("container_1427562107907_0005_01_000001", sh
      .getLastPreemptionDetails().getContainerId().toString());
    assertEquals("testhost3:1234", sh.getLastPreemptionDetails()
      .getNodeId().toString());
    assertEquals("testqueue3", sh.getLastPreemptionDetails().getQueue());
    assertEquals(now, sh.getLastPreemptionDetails().getTimestamp());
    assertEquals(0, sh.getLastSchedulerRunTime());
  }

  @Test
  public void testResourceUpdate() {
    SchedulerHealth sh = new SchedulerHealth();
    long now = Time.now();
    sh.updateSchedulerRunDetails(now, Resource.newInstance(1024, 1),
      Resource.newInstance(2048, 1));
    assertEquals(now, sh.getLastSchedulerRunTime());
    assertEquals(Resource.newInstance(1024, 1),
      sh.getResourcesAllocated());
    assertEquals(Resource.newInstance(2048, 1),
      sh.getResourcesReserved());
    now = Time.now();
    sh.updateSchedulerReleaseDetails(now, Resource.newInstance(3072, 1));
    assertEquals(now, sh.getLastSchedulerRunTime());
    assertEquals(Resource.newInstance(3072, 1),
      sh.getResourcesReleased());
  }

  private NodeManager registerNode(String hostName, int containerManagerPort,
      int httpPort, String rackName, Resource capability, NodeStatus nodeStatus)
      throws IOException, YarnException {
    NodeManager nm =
        new NodeManager(hostName, containerManagerPort, httpPort, rackName,
          capability, resourceManager, nodeStatus);
    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(resourceManager.getRMContext().getRMNodes()
          .get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  private void nodeUpdate(NodeManager nm) {
    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);
  }

  @Test
  public void testCapacitySchedulerAllocation() throws Exception {

    setup();

    boolean isCapacityScheduler =
        resourceManager.getResourceScheduler() instanceof CapacityScheduler;
    assumeTrue(isCapacityScheduler,
        "This test is only supported on Capacity Scheduler");

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
          Resources.createResource(5 * 1024, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

    // Submit an application
    Application application_0 =
        new Application("user_0", "default", resourceManager);
    application_0.submit();

    application_0.addNodeManager(host_0, 1234, nm_0);

    Resource capability_0_0 = Resources.createResource(1024, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * 1024, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0 });
    application_0.addTask(task_0_0);
    Task task_0_1 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_1);

    // Send resource requests to the scheduler
    application_0.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    nodeUpdate(nm_0);
    SchedulerHealth sh =
        ((CapacityScheduler) resourceManager.getResourceScheduler())
          .getSchedulerHealth();
    // Now SchedulerHealth records last container allocated, aggregated
    // allocation account will not be changed
    assertEquals(1, sh.getAllocationCount().longValue());
    assertEquals(Resource.newInstance(1 * 1024, 1),
      sh.getResourcesAllocated());
    assertEquals(2, sh.getAggregateAllocationCount().longValue());
    assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());

    Task task_0_2 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_2);
    application_0.schedule();

    nodeUpdate(nm_0);
    assertEquals(1, sh.getAllocationCount().longValue());
    assertEquals(Resource.newInstance(2 * 1024, 1),
      sh.getResourcesAllocated());
    assertEquals(3, sh.getAggregateAllocationCount().longValue());
    assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());
  }

  @Test
  public void testCapacitySchedulerReservation() throws Exception {

    setup();

    boolean isCapacityScheduler =
        resourceManager.getResourceScheduler() instanceof CapacityScheduler;
    assumeTrue(isCapacityScheduler,
        "This test is only supported on Capacity Scheduler");

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register nodes
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
          Resources.createResource(2 * 1024, 1), mockNodeStatus);
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
          Resources.createResource(5 * 1024, 1), mockNodeStatus);
    nodeUpdate(nm_0);
    nodeUpdate(nm_1);

    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

    // Submit an application
    Application application_0 =
        new Application("user_0", "default", resourceManager);
    application_0.submit();

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(1024, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * 1024, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[] { host_0 });
    application_0.addTask(task_0_0);

    // Send resource requests to the scheduler
    application_0.schedule();

    // Send a heartbeat to kick the tires on the Scheduler
    nodeUpdate(nm_0);
    SchedulerHealth sh =
        ((CapacityScheduler) resourceManager.getResourceScheduler())
          .getSchedulerHealth();
    assertEquals(1, sh.getAllocationCount().longValue());
    assertEquals(Resource.newInstance(1024, 1),
      sh.getResourcesAllocated());
    assertEquals(1, sh.getAggregateAllocationCount().longValue());
    assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());

    Task task_0_1 =
        new Task(application_0, priority_0, new String[] { host_0 });
    application_0.addTask(task_0_1);
    application_0.schedule();

    nodeUpdate(nm_0);
    assertEquals(0, sh.getAllocationCount().longValue());
    assertEquals(1, sh.getReservationCount().longValue());
    assertEquals(Resource.newInstance(2 * 1024, 1),
      sh.getResourcesReserved());
    assertEquals(1, sh.getAggregateAllocationCount().longValue());
    assertEquals("host_0:1234", sh.getLastAllocationDetails()
      .getNodeId().toString());
    assertEquals("root.default", sh.getLastAllocationDetails()
      .getQueue());
  }
}
