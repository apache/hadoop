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
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.waitforNMRegistered;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;

import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterNodeTracker;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test class for Multi Node scheduling related tests.
 */
public class TestCapacitySchedulerMultiNodes {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCapacitySchedulerMultiNodes.class);
  private static final QueuePath DEFAULT = new QueuePath("root.default");
  private CapacitySchedulerConfiguration conf;
  private static final String POLICY_CLASS_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy";

  @BeforeEach
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
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
    conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
  }

  @Test
  public void testMultiNodeSorterForScheduling() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("127.0.0.1:1234", 10 * GB);
    rm.registerNode("127.0.0.1:1235", 10 * GB);
    rm.registerNode("127.0.0.1:1236", 10 * GB);
    rm.registerNode("127.0.0.1:1237", 10 * GB);
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);
    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();
    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    assertEquals(4, nodes.size());
    rm.stop();
  }

  @Test
  public void testMultiNodeSorterForSchedulingWithOrdering() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB, 10);
    MockNM nm2 = rm.registerNode("127.0.0.2:1235", 10 * GB, 10);
    MockNM nm3 = rm.registerNode("127.0.0.3:1236", 10 * GB, 10);
    MockNM nm4 = rm.registerNode("127.0.0.4:1237", 10 * GB, 10);
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);

    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();

    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    assertEquals(4, nodes.size());

    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2048, rm)
            .withAppName("app-1")
            .withUser("user1")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    SchedulerNodeReport reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    // check node report
    assertEquals(2 * GB, reportNm1.getUsedResource().getMemorySize());
    assertEquals(8 * GB,
        reportNm1.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppName("app-2")
            .withUser("user2")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);
    SchedulerNodeReport reportNm2 =
        rm.getResourceScheduler().getNodeReport(nm2.getNodeId());

    // check node report
    assertEquals(1 * GB, reportNm2.getUsedResource().getMemorySize());
    assertEquals(9 * GB,
        reportNm2.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    // Node1 and Node2 are now having used resources. Hence ensure these 2 comes
    // latter in the list.
    nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    List<NodeId> currentNodes = new ArrayList<>();
    currentNodes.add(nm3.getNodeId());
    currentNodes.add(nm4.getNodeId());
    currentNodes.add(nm2.getNodeId());
    currentNodes.add(nm1.getNodeId());
    Iterator<SchedulerNode> it = nodes.iterator();
    SchedulerNode current;
    int i = 0;
    while (it.hasNext()) {
      current = it.next();
      assertEquals(current.getNodeID(), currentNodes.get(i++));
    }
    rm.stop();
  }

  @Test
  @Timeout(value = 30)
  public void testExcessReservationWillBeUnreserved() throws Exception {
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    newConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    newConf.setInt(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
        + ".resource-based.sorting-interval.ms", 0);
    newConf.setMaximumApplicationMasterResourcePerQueuePercent(DEFAULT,
        1.0f);
    MockRM rm1 = new MockRM(newConf);

    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    RMApp app1 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(5 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm2
    RMApp app2 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(5 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("default");
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    /*
     * Verify that reserved container will be unreserved
     * after its ask has been cancelled when used capacity of root queue is 1.
     */
    // Ask a container with 6GB memory size for app1,
    // nm2 will reserve a container for app1
    // Last Node from Node Iterator will be RESERVED
    am1.allocate("*", 6 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    // Check containers of app1 and app2.
    assertNotNull(cs.getNode(nm2.getNodeId()).getReservedContainer());
    assertEquals(1, schedulerApp1.getLiveContainers().size());
    assertEquals(1, schedulerApp1.getReservedContainers().size());
    assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Cancel ask of the reserved container.
    am1.allocate("*", 6 * GB, 0, new ArrayList<>());
    // Ask another container with 2GB memory size for app2.
    am2.allocate("*", 2 * GB, 1, new ArrayList<>());

    // Trigger scheduling to release reserved container
    // whose ask has been cancelled.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    assertEquals(1, schedulerApp1.getLiveContainers().size());
    assertEquals(0, schedulerApp1.getReservedContainers().size());
    assertEquals(2, schedulerApp2.getLiveContainers().size());
    assertEquals(7 * GB,
        cs.getNode(nm1.getNodeId()).getAllocatedResource().getMemorySize());
    assertEquals(12 * GB,
        cs.getRootQueue().getQueueResourceUsage().getUsed().getMemorySize());
    assertEquals(0,
        cs.getRootQueue().getQueueResourceUsage().getReserved()
            .getMemorySize());
    assertEquals(0,
        leafQueue.getQueueResourceUsage().getReserved().getMemorySize());

    rm1.close();
  }

  @Test
  @Timeout(value = 30)
  public void testAllocateForReservedContainer() throws Exception {
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    newConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    newConf.setInt(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
        + ".resource-based.sorting-interval.ms", 0);
    newConf.setMaximumApplicationMasterResourcePerQueuePercent(DEFAULT,
        1.0f);
    MockRM rm1 = new MockRM(newConf);

    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    RMApp app1 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(5 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm2
    RMApp app2 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(5 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    /*
     * Verify that reserved container will be allocated
     * after node has sufficient resource.
     */
    // Ask a container with 6GB memory size for app2,
    // nm2 will reserve a container for app2
    // Last Node from Node Iterator will be RESERVED
    am2.allocate("*", 6 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // Check containers of app1 and app2.
    assertNotNull(cs.getNode(nm2.getNodeId()).getReservedContainer());
    assertEquals(1, schedulerApp1.getLiveContainers().size());
    assertEquals(1, schedulerApp2.getLiveContainers().size());
    assertEquals(1, schedulerApp2.getReservedContainers().size());

    // Kill app1 to release resource on nm1.
    rm1.killApp(app1.getApplicationId());

    // Trigger scheduling to allocate for reserved container on nm1.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    assertEquals(2, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  @Test
  @Timeout(value = 30)
  public void testAllocateOfReservedContainerFromAnotherNode()
      throws Exception {
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    newConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    newConf.setInt(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
        + ".resource-based.sorting-interval.ms", 0);
    newConf.setMaximumApplicationMasterResourcePerQueuePercent(DEFAULT,
        1.0f);
    MockRM rm1 = new MockRM(newConf);

    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 12 * GB, 2);
    MockNM nm2 = rm1.registerNode("h2:1234", 12 * GB, 2);

    // launch an app1 to queue, AM container will be launched in nm1
    RMApp app1 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(8 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app2 to queue, AM container will be launched in nm2
    RMApp app2 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(8 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    // Reserve a Container for app3
    RMApp app3 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(8 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());

    final AtomicBoolean result = new AtomicBoolean(false);
    Thread t = new Thread() {
      public void run() {
        try {
          MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm1);
          result.set(true);
        } catch (Exception e) {
          fail("Failed to allocate the reserved container");
        }
      }
    };
    t.start();
    Thread.sleep(1000);

    // Validate if app3 has got RESERVED container
    FiCaSchedulerApp schedulerApp =
        cs.getApplicationAttempt(app3.getCurrentAppAttempt().getAppAttemptId());
    assertEquals(1, schedulerApp.getReservedContainers().size(),
        "App3 failed to get reserved container");

    // Free the Space on other node where Reservation has not happened
    if (cs.getNode(rmNode1.getNodeID()).getReservedContainer() != null) {
      rm1.killApp(app2.getApplicationId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    } else {
      rm1.killApp(app1.getApplicationId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }

    // Check if Reserved AM of app3 gets allocated in
    // node where space available
    while (!result.get()) {
      Thread.sleep(100);
    }

    // Validate release of reserved containers
    schedulerApp =
        cs.getApplicationAttempt(app3.getCurrentAppAttempt().getAppAttemptId());
    assertEquals(0, schedulerApp.getReservedContainers().size(),
        "App3 failed to release Reserved container");
    assertNull(cs.getNode(rmNode1.getNodeID()).getReservedContainer());
    assertNull(cs.getNode(rmNode2.getNodeID()).getReservedContainer());

    rm1.close();
  }

  @Test
  public void testMultiNodeSorterAfterHeartbeatInterval() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("127.0.0.1:1234", 10 * GB);
    rm.registerNode("127.0.0.2:1234", 10 * GB);
    rm.registerNode("127.0.0.3:1234", 10 * GB);
    rm.registerNode("127.0.0.4:1234", 10 * GB);

    Set<SchedulerNode> nodes = new HashSet<>();
    String partition = "";

    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);
    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();

    Iterator<SchedulerNode> nodeIterator = mns.getMultiNodeSortIterator(
        nodes, partition, POLICY_CLASS_NAME);
    assertEquals(4, Iterators.size(nodeIterator));

    // Validate the count after missing 3 node heartbeats
    Thread.sleep(YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS * 3);

    nodeIterator = mns.getMultiNodeSortIterator(
        nodes, partition, POLICY_CLASS_NAME);
    assertEquals(0, Iterators.size(nodeIterator));

    rm.stop();
  }

  @Test
  @Timeout(value = 30)
  public void testSkipAllocationOnNodeReservedByAnotherApp() throws Exception {
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    newConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    newConf.setInt(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
        + ".resource-based.sorting-interval.ms", 0);
    newConf.setMaximumApplicationMasterResourcePerQueuePercent(DEFAULT, 1.0f);
    newConf.set(CapacitySchedulerConfiguration.SKIP_ALLOCATE_ON_NODES_WITH_RESERVED_CONTAINERS,
        "true");
    MockRM rm1 = new MockRM(newConf);

    rm1.start();
    MockNM nm1 = rm1.registerNode("127.0.0.1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("127.0.0.2:1235", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    RMApp app1 = MockRMAppSubmitter.submit(rm1, MockRMAppSubmissionData.Builder
        .createWithMemory(5 * GB, rm1)
        .withAppName("app")
        .withUser("user")
        .withQueue("default")
        .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm2
    RMApp app2 = MockRMAppSubmitter.submit(rm1, MockRMAppSubmissionData.Builder
        .createWithMemory(5 * GB, rm1)
        .withAppName("app")
        .withUser("user")
        .withQueue("default")
        .build());
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    // Ask a container with 4 GB memory size for app1,
    am1.allocate("*", 4 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));


    // Check containers of app1 and app2.
    Set<RMNode> reservedContainers = checkReservedContainers(cs,
        rm1.getRMContext().getRMNodes(), 1);
    assertEquals(1, reservedContainers.size());
    RMNode nodeWithReservedContainer = reservedContainers.iterator().next();
    LOG.debug("Reserved container on: {}", nodeWithReservedContainer);

    //Move reservation to nm1 for easier testing
    if (nodeWithReservedContainer.getNodeID().getHost().startsWith("127.0.0.2")) {
      moveReservation(cs, rm1, nm1, nm2, am1);
    }
    assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    assertNull(cs.getNode(nm2.getNodeId()).getReservedContainer());

    assertEquals(1, schedulerApp1.getLiveContainers().size());
    assertEquals(1, schedulerApp2.getLiveContainers().size());
    assertEquals(1, schedulerApp1.getReservedContainers().size());

    //Make sure to have available headroom on the child queue,
    // see: RegularContainerAllocator#checkHeadroom,
    //that can make RegularContainerAllocator.preCheckForNodeCandidateSet to return
    // ContainerAllocation.QUEUE_SKIPPED
    MockNM nm3 = rm1.registerNode("127.0.0.3:1235", 3 * GB);

    //Allocate a container for app2, we expect this to be allocated on nm2 as
    // nm1 has a reservation for another app
    am2.allocate("*", 4 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    assertNotNull(cs.getNode(nm2.getNodeId()).getReservedContainer());

    rm1.close();
  }

  // In a scheduling scenario with 2000 candidate nodes and an unsatisfied
  // request that reaches the headroom,
  // currently the global scheduler checks the request for each node repeatedly,
  // causing a scheduling cycle to exceed 200ms.
  // With the improvement of YARN-11798, the global scheduler checks the request
  // only once before evaluating all nodes, reducing the scheduling cycle to
  // less than 2ms.
  @Test
  @Timeout(value = 30)
  public void testCheckRequestOnceForUnsatisfiedRequest() throws Exception {
    QueuePath defaultQueuePath =
        new QueuePath(CapacitySchedulerConfiguration.ROOT, "default");
    String resourceLimit = "[vcores=3,memory=3096]";
    conf.setCapacity(defaultQueuePath, resourceLimit);
    conf.setMaximumCapacityByLabel(defaultQueuePath, "", resourceLimit);
    conf.setInt(YarnConfiguration.SCHEDULER_SKIP_NODE_MULTIPLIER, 1000);
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("test:1234", 10 * GB);
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 1, 5);

    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2048, rm)
            .withAppName("app-1")
            .withUser("user1")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    SchedulerNodeReport reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    // check node report
    assertEquals(2 * GB, reportNm1.getUsedResource().getMemorySize());
    assertEquals(8 * GB,
        reportNm1.getAvailableResource().getMemorySize());

    // mock node tracker with 2000 nodes
    // to simulate the scenario where there are many nodes in the cluster
    List<FiCaSchedulerNode> mockNodes = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      FiCaSchedulerNode node =
          TestUtils.getMockNode("host" + i + ":1234", "", 0, 10 * GB, 10);
      mockNodes.add(node);
    }
    ClusterNodeTracker<FiCaSchedulerNode> mockNodeTracker =
        new ClusterNodeTracker<FiCaSchedulerNode>() {
      @Override
      public List<FiCaSchedulerNode> getNodesPerPartition(String partition) {
        return mockNodes;
      }
    };

    // replace the scheduler with a spy scheduler with mocked node-tracker
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    final CapacityScheduler spyCs = Mockito.spy(cs);
    when(spyCs.getNodeTracker()).thenReturn(mockNodeTracker);
    RMContext rmContext = rm.getRMContext();
    ((RMContextImpl) rmContext).setScheduler(spyCs);

    MultiNodeSortingManager<SchedulerNode> mns =
        rmContext.getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter =
        mns.getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();

    // verify that the number of nodes is correct
    Set<SchedulerNode> nodes =
        sorter.getMultiNodeLookupPolicy().getNodesPerPartition("");
    assertEquals(mockNodes.size(), nodes.size());

    // create an unsatisfied request which will reach the headroom
    am1.allocate("*", 2 * GB, 10, new ArrayList<>());

    List<Long> elapsedMsLst = new ArrayList<>();
    try {
      GenericTestUtils.waitFor(() -> {
        // verify that when headroom is reached for an unsatisfied request,
        // scheduler should only check the request once before checking all nodes.
        CandidateNodeSet<FiCaSchedulerNode> candidates =
            new SimpleCandidateNodeSet<>(Collections.emptyMap(), "");
        int numSchedulingCycles = 10;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numSchedulingCycles; i++) {
          spyCs.allocateContainersToNode(candidates, false);
        }
        long avgElapsedMs =
            (System.currentTimeMillis() - startTime) / numSchedulingCycles;
        LOG.info("Average elapsed time for a scheduling cycle: {} ms",
            avgElapsedMs);

        elapsedMsLst.add(avgElapsedMs);
        // verify that the scheduling cycle is less than 10ms,
        // ideally the latency should be less than 2ms.
        return avgElapsedMs < 10;
      }, 500, 3000);
    } catch (TimeoutException e) {
      fail("Scheduling cycle expected to be less than 10ms, " +
          "but took too long, elapsedMs:" + elapsedMsLst);
    } finally {
      rm.stop();
    }
  }

  private static void moveReservation(CapacityScheduler cs,
      MockRM rm1, MockNM nm1, MockNM nm2, MockAM am1) {
    RMNode sourceNode = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    RMNode targetNode = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    SchedulerApplicationAttempt firstSchedulerAppAttempt =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp app = (FiCaSchedulerApp)firstSchedulerAppAttempt;
    RMContainer reservedContainer = cs.getNode(sourceNode.getNodeID()).getReservedContainer();
    LOG.debug("Moving reservation");
    app.moveReservation(reservedContainer,
        cs.getNode(sourceNode.getNodeID()), cs.getNode(targetNode.getNodeID()));
  }

  private static Set<RMNode> checkReservedContainers(CapacityScheduler cs,
      ConcurrentMap<NodeId, RMNode> rmNodes, int expectedNumberOfContainers) {
    Set<RMNode> result = new HashSet<>();
    for (Map.Entry<NodeId, RMNode> entry : rmNodes.entrySet()) {
      if (cs.getNode(entry.getKey()).getReservedContainer() != null) {
        result.add(entry.getValue());
      }
    }

    assertEquals(expectedNumberOfContainers, result.size());
    return result;
  }
}
