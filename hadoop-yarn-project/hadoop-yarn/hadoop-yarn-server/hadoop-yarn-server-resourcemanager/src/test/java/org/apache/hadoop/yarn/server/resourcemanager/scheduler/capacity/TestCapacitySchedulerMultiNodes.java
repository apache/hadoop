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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.ROOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.waitforNMRegistered;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.policy.MultiComparatorPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for Multi Node scheduling related tests.
 */
public class TestCapacitySchedulerMultiNodes {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCapacitySchedulerMultiNodes.class);
  private static final QueuePath DEFAULT = new QueuePath("root.default");
  private CapacitySchedulerConfiguration conf;
  private static final String POLICY_NAME = "resource-based";
  private static final String POLICY_CLASS_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy";

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
        POLICY_NAME);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
        POLICY_NAME);
    String policyName =
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
            + DOT + POLICY_NAME + ".class";
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
        .getMultiNodePolicy(POLICY_NAME);
    sorter.reSortClusterNodes();
    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(4, nodes.size());
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
        .getMultiNodePolicy(POLICY_NAME);
    sorter.reSortClusterNodes();

    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(4, nodes.size());

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
    Assert.assertEquals(2 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals(8 * GB,
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
    Assert.assertEquals(1 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals(9 * GB,
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
      Assert.assertEquals(current.getNodeID(), currentNodes.get(i++));
    }
    rm.stop();
  }

  @Test (timeout=30000)
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
    Assert.assertNotNull(cs.getNode(nm2.getNodeId()).getReservedContainer());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp1.getReservedContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Cancel ask of the reserved container.
    am1.allocate("*", 6 * GB, 0, new ArrayList<>());
    // Ask another container with 2GB memory size for app2.
    am2.allocate("*", 2 * GB, 1, new ArrayList<>());

    // Trigger scheduling to release reserved container
    // whose ask has been cancelled.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(0, schedulerApp1.getReservedContainers().size());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(7 * GB,
        cs.getNode(nm1.getNodeId()).getAllocatedResource().getMemorySize());
    Assert.assertEquals(12 * GB,
        cs.getRootQueue().getQueueResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(0,
        cs.getRootQueue().getQueueResourceUsage().getReserved()
            .getMemorySize());
    Assert.assertEquals(0,
        leafQueue.getQueueResourceUsage().getReserved().getMemorySize());

    rm1.close();
  }

  @Test(timeout=30000)
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
    Assert.assertNotNull(cs.getNode(nm2.getNodeId()).getReservedContainer());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getReservedContainers().size());

    // Kill app1 to release resource on nm1.
    rm1.killApp(app1.getApplicationId());

    // Trigger scheduling to allocate for reserved container on nm1.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }

  @Test(timeout=30000)
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
          Assert.fail("Failed to allocate the reserved container");
        }
      }
    };
    t.start();
    Thread.sleep(1000);

    // Validate if app3 has got RESERVED container
    FiCaSchedulerApp schedulerApp =
        cs.getApplicationAttempt(app3.getCurrentAppAttempt().getAppAttemptId());
    Assert.assertEquals("App3 failed to get reserved container", 1,
        schedulerApp.getReservedContainers().size());

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
    Assert.assertEquals("App3 failed to release Reserved container", 0,
        schedulerApp.getReservedContainers().size());
    Assert.assertNull(cs.getNode(rmNode1.getNodeID()).getReservedContainer());
    Assert.assertNull(cs.getNode(rmNode2.getNodeID()).getReservedContainer());

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
        .getMultiNodePolicy(POLICY_NAME);
    sorter.reSortClusterNodes();

    Iterator<SchedulerNode> nodeIterator = mns.getMultiNodeSortIterator(
        nodes, partition, POLICY_NAME);
    Assert.assertEquals(4, Iterators.size(nodeIterator));

    // Validate the count after missing 3 node heartbeats
    Thread.sleep(YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS * 3);

    nodeIterator = mns.getMultiNodeSortIterator(
        nodes, partition, POLICY_NAME);
    Assert.assertEquals(0, Iterators.size(nodeIterator));

    rm.stop();
  }

  @Test(timeout=30000)
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
    Assert.assertEquals(1, reservedContainers.size());
    RMNode nodeWithReservedContainer = reservedContainers.iterator().next();
    LOG.debug("Reserved container on: {}", nodeWithReservedContainer);

    //Move reservation to nm1 for easier testing
    if (nodeWithReservedContainer.getNodeID().getHost().startsWith("127.0.0.2")) {
      moveReservation(cs, rm1, nm1, nm2, am1);
    }
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertNull(cs.getNode(nm2.getNodeId()).getReservedContainer());

    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp1.getReservedContainers().size());

    //Make sure to have available headroom on the child queue,
    // see: RegularContainerAllocator#checkHeadroom,
    //that can make RegularContainerAllocator.preCheckForNodeCandidateSet to return
    // ContainerAllocation.QUEUE_SKIPPED
    MockNM nm3 = rm1.registerNode("127.0.0.3:1235", 3 * GB);

    //Allocate a container for app2, we expect this to be allocated on nm2 as
    // nm1 has a reservation for another app
    am2.allocate("*", 4 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertNotNull(cs.getNode(nm2.getNodeId()).getReservedContainer());

    rm1.close();
  }

  @Test
  public void testMultiComparatorPolicy() throws Exception {
    /*
     * init conf
     * - configure 2 policies with MultiComparatorPolicy class
     *      default: use default comparator
     *               (DOMINANT_RESOURCE_RATIO:ASC,NODE_ID:ASC)
     *      test: use custom comparator (ALLOCATED_RESOURCE:ASC,NODE_ID:ASC)
     * - enable synchronous refresh (set sorting-interval-ms to be 0)
     * - configure queue "test" to use test policy.
     */
    String defaultQueueName = "default", defaultPolicyName = "default",
        testQueueName = "test", testPolicyName = "test",
        enhancedPolicyClass = MultiComparatorPolicy.class.getName();
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    // init queues
    conf.setQueues(ROOT, new String[]{defaultQueueName, testQueueName});
    QueuePath defaultQueuePath =
        QueuePath.createFromQueues(ROOT.getFullPath(), defaultQueueName);
    QueuePath testQueuePath =
        QueuePath.createFromQueues(ROOT.getFullPath(), testQueueName);
    conf.setCapacity(defaultQueuePath, 50.0f);
    conf.setCapacity(testQueuePath, 50.0f);
    conf.setMaximumApplicationMasterResourcePercent(1.0f);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
        defaultPolicyName + "," + testPolicyName);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
        + defaultPolicyName + ".class", enhancedPolicyClass);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
        + defaultPolicyName + DOT
        + CapacitySchedulerConfiguration.SORTING_INTERVAL_MS_SUFFIX, "0");
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
        + testPolicyName + DOT
        + CapacitySchedulerConfiguration.SORTING_INTERVAL_MS_SUFFIX, "0");
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
        + testPolicyName + ".class", enhancedPolicyClass);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + DOT
            + testPolicyName + DOT
            + MultiComparatorPolicy.COMPARATORS_CONF_KEY,
        "ALLOCATED_RESOURCE:ASC,NODE_ID");
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
        defaultPolicyName);
    conf.set(getQueuePrefix(testQueuePath)
            + CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_SUFFIX,
        testPolicyName);
    conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "600000");
    // mock RM and 4 NMs
    // nm1, nm2, nm3 have 10 GB memory and 10 vcores each
    // nm4 has 100 GB memory and 100 vcores.
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 10 * GB, 10);
    MockNM nm2 = rm.registerNode("host2:1234", 10 * GB, 10);
    MockNM nm3 = rm.registerNode("host3:1234", 10 * GB, 10);
    MockNM nm4 = rm.registerNode("host4:1234", 100 * GB, 100);
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);
    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();

    // allocate for nodes
    BiFunction<String, Resource, Void> launchAndRegisterAM = (queue, resource) -> {
      try {
        MockRMAppSubmissionData data1 =
            MockRMAppSubmissionData.Builder.createWithResource(
                resource, rm)
            .withAppName("app-1")
            .withAcls(null)
            .withQueue(queue)
            .withUnmanagedAM(false)
            .build();
        RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
        MockRM.launchAndRegisterAM(app1, rm, nm1);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    };
    // try to assign four containers.
    Resource nm1AllocatedResource = Resource.newInstance(GB, 1),
      nm2AllocatedResource = Resource.newInstance(2*GB, 2),
      nm3AllocatedResource = Resource.newInstance(3*GB, 3),
      nm4AllocatedResource = Resource.newInstance(4*GB, 4);
    launchAndRegisterAM.apply(defaultQueueName, nm1AllocatedResource);
    launchAndRegisterAM.apply(defaultQueueName, nm2AllocatedResource);
    launchAndRegisterAM.apply(defaultQueueName, nm3AllocatedResource);
    launchAndRegisterAM.apply(defaultQueueName, nm4AllocatedResource);
    // verify that four containers will be allocated sequentially to
    // nm1, nm2, nm3, nm4 according to the default policy.
    Assert.assertEquals(
        rm.getResourceScheduler().getSchedulerNode(nm1.getNodeId())
            .getAllocatedResource(), Resource.newInstance(GB, 1));
    Assert.assertEquals(
        rm.getResourceScheduler().getSchedulerNode(nm2.getNodeId())
            .getAllocatedResource(), Resource.newInstance(2 * GB, 2));
    Assert.assertEquals(
        rm.getResourceScheduler().getSchedulerNode(nm3.getNodeId())
            .getAllocatedResource(), Resource.newInstance(3 * GB, 3));
    Assert.assertEquals(
        rm.getResourceScheduler().getSchedulerNode(nm4.getNodeId())
            .getAllocatedResource(), Resource.newInstance(4 * GB, 4));

    // for default policy, nm4 with least dominant-resource-ratio
    // should be chosen at first.
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(defaultPolicyName);
    sorter.reSortClusterNodes();
    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(4, nodes.size());
    Assert.assertEquals(nm4.getNodeId(), nodes.iterator().next().getNodeID());

    // for test policy, nm1 with least allocated-resource
    // should be chosen at first
    sorter = mns
        .getMultiNodePolicy(testPolicyName);
    sorter.reSortClusterNodes();
    nodes = sorter.getMultiNodeLookupPolicy().getNodesPerPartition("");
    Assert.assertEquals(4, nodes.size());
    Assert.assertEquals(nm1.getNodeId(), nodes.iterator().next().getNodeID());

    // schedule for app in test queue with policy=test,
    // verify that nm1 will be chosen
    Resource nm1AddResource = Resource.newInstance(6 * GB, 4);
    launchAndRegisterAM.apply(testQueuePath.getLeafName(), nm1AddResource);
    Resource expectedAllocatedResourceForNM1 =
        Resources.add(nm1AllocatedResource, nm1AddResource);
    Assert.assertEquals(
        rm.getResourceScheduler().getSchedulerNode(nm1.getNodeId())
            .getAllocatedResource(), expectedAllocatedResourceForNM1);

    rm.stop();
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

    Assert.assertEquals(expectedNumberOfContainers, result.size());
    return result;
  }
}
