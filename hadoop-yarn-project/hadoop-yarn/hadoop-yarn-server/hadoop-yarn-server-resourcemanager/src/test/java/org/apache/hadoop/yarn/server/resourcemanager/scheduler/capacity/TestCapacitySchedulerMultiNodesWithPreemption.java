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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TestCapacitySchedulerMultiNodesWithPreemption {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCapacitySchedulerMultiNodesWithPreemption.class);
  private CapacitySchedulerConfiguration conf;
  private static final String POLICY_CLASS_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement."
      + "ResourceUsageMultiNodeLookupPolicy";

  @Before
  public void setUp() {
    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();
    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    conf = new CapacitySchedulerConfiguration(config);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
        "resource-based");
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
        "resource-based");
    String policyName =
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
        + ".resource-based" + ".class";
    conf.set(policyName, POLICY_CLASS_NAME);
    conf.setBoolean(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
        true);
    // Set this to avoid the AM pending issue
    conf.set(CapacitySchedulerConfiguration
        .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, "1");
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
    conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
    conf.setInt("yarn.scheduler.maximum-allocation-mb", 102400);

    // Configure two queues to test Preemption
    conf.set("yarn.scheduler.capacity.root.queues", "A, default");
    conf.set("yarn.scheduler.capacity.root.A.capacity", "50");
    conf.set("yarn.scheduler.capacity.root.default.capacity", "50");
    conf.set("yarn.scheduler.capacity.root.A.maximum-capacity", "100");
    conf.set("yarn.scheduler.capacity.root.default.maximum-capacity", "100");
    conf.set("yarn.scheduler.capacity.root.A.user-limit-factor", "10");
    conf.set("yarn.scheduler.capacity.root.default.user-limit-factor", "10");

    // Configure Preemption
    conf.setLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL, 10000);
    conf.setLong(CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        1500);
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        1.0f);
    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        1.0f);

    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        ProportionalCapacityPreemptionPolicy.class.getCanonicalName());
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.setLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 60000);
  }

  @Test(timeout=60000)
  public void testAllocateReservationFromOtherNode() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM[] nms = new MockNM[3];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 1 * GB, 2);
    nms[0] = nm1;
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 2 * GB, 2);
    nms[1] = nm2;
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 3 * GB, 2);
    nms[2] = nm3;

    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();

    // Step 1: Launch an App in Default Queue which utilizes the entire cluster
    RMApp app1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(3 * GB, rm)
            .withAppName("app-1")
            .withUser("user1")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    am1.allocateAndWaitForContainers("*", 1, 2 * GB, nm2);
    am1.allocateAndWaitForContainers("*", 1, 1 * GB, nm3);

    // Step 2: Wait till the nodes utilization are full
    GenericTestUtils.waitFor(() -> {
      SchedulerNodeReport reportNM1 =
          rm.getResourceScheduler().getNodeReport(nms[0].getNodeId());
      SchedulerNodeReport reportNM2 =
          rm.getResourceScheduler().getNodeReport(nms[1].getNodeId());
      return (reportNM1.getAvailableResource().getMemorySize() == 0 * GB)
          && (reportNM2.getAvailableResource().getMemorySize() == 0 * GB);
    }, 10, 10000);


    // Step 3: Launch another App in Queue A which will be Reserved
    // after Preemption
    final AtomicBoolean result = new AtomicBoolean(false);
    RMApp app2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app-2")
            .withUser("user2")
            .withAcls(null)
            .withQueue("A")
            .build());

    // Launch AM in a thread and in parallel free the preempted node's
    // unallocated resources in main thread
    Thread t1 = new Thread() {
      public void run() {
        try {
          MockAM am2 = MockRM.launchAM(app2, rm, nm1);
          result.set(true);
        } catch (Exception e) {
          Assert.fail("Failed to launch app-2");
        }
      }
    };
    t1.start();

    // Step 4: Wait for Preemption to happen. It will preempt Node1 (1GB)
    // Get the node where preemption happened which has the available space
    final AtomicReference<MockNM> preemptedNode = new AtomicReference<>();
    GenericTestUtils.waitFor(() -> {
      for (int i = 0; i < nms.length; i++) {
        SchedulerNodeReport reportNM =
            rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
        if (reportNM.getAvailableResource().getMemorySize() == 1 * GB) {
          preemptedNode.set(nms[i]);
          return true;
        }
      }
      return false;
    }, 10, 30000);
    LOG.info("Preempted node is: " + preemptedNode.get().getNodeId());


    // Step 5: Don't release the container from NodeManager so that Reservation
    // happens. Used Capacity will be < 1.0f but nodes won't have available
    // containers so Reservation will happen.
    FiCaSchedulerNode schedulerNode =
        ((CapacityScheduler) rm.getResourceScheduler())
        .getNodeTracker().getNode(preemptedNode.get().getNodeId());
    Resource curResource = schedulerNode.getUnallocatedResource();
    schedulerNode.deductUnallocatedResource(Resource.newInstance(curResource));

    ((CapacityScheduler) rm.getResourceScheduler()).getNodeTracker()
        .removeNode(preemptedNode.get().getNodeId());
    ((CapacityScheduler) rm.getResourceScheduler()).getNodeTracker()
        .addNode(schedulerNode);

    // Send a heartbeat to kick the tires on the Scheduler
    // The container will be reserved for app-2
    RMNode preemptedRMNode = rm.getRMContext().getRMNodes().get(
        preemptedNode.get().getNodeId());
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(
        preemptedRMNode);
    rm.getResourceScheduler().handle(nodeUpdate);

    // Validate if Reservation happened
    // Reservation will happen on last node in the iterator - Node3
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    ApplicationAttemptId app2AttemptId = app2.getCurrentAppAttempt()
        .getAppAttemptId();
    FiCaSchedulerApp schedulerApp = cs.getApplicationAttempt(app2AttemptId);

    assertEquals("App2 failed to get reserved container", 1,
        schedulerApp.getReservedContainers().size());
    LOG.info("Reserved node is: " +
        schedulerApp.getReservedContainers().get(0).getReservedNode());
    assertNotEquals("Failed to reserve as per the Multi Node Itearor",
        schedulerApp.getReservedContainers().get(0).getReservedNode(),
        preemptedNode.get().getNodeId());


    // Step 6: Okay, now preempted node is Node1 and reserved node is Node3
    // Validate if the Reserved Container gets allocated
    // after updating release container.
    schedulerNode = ((CapacityScheduler) rm.getResourceScheduler())
        .getNodeTracker().getNode(preemptedNode.get().getNodeId());
    curResource = schedulerNode.getAllocatedResource();
    schedulerNode.updateTotalResource(
        Resources.add(schedulerNode.getTotalResource(), curResource));

    ((CapacityScheduler) rm.getResourceScheduler()).getNodeTracker()
        .removeNode(preemptedNode.get().getNodeId());
    ((CapacityScheduler) rm.getResourceScheduler()).getNodeTracker()
        .addNode(schedulerNode);

    preemptedRMNode = rm.getRMContext().getRMNodes().get(
        preemptedNode.get().getNodeId());
    nodeUpdate = new NodeUpdateSchedulerEvent(preemptedRMNode);
    rm.getResourceScheduler().handle(nodeUpdate);

    // Step 7: Wait for app-2 to get ALLOCATED
    GenericTestUtils.waitFor(() -> {
      return result.get();
    }, 10, 20000);

    // Step 8: Validate if app-2 has got 1 live container and
    // released the reserved container
    schedulerApp = cs.getApplicationAttempt(app2AttemptId);
    assertEquals("App2 failed to get Allocated", 1,
        schedulerApp.getLiveContainers().size());
    assertEquals("App2 failed to Unreserve", 0,
        schedulerApp.getReservedContainers().size());

    rm.stop();
  }
}