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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
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
public class TestCapacitySchedulerMultiNodes extends CapacitySchedulerTestBase {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCapacitySchedulerMultiNodes.class);
  private CapacitySchedulerConfiguration conf;
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
        .getMultiNodePolicy(POLICY_CLASS_NAME);
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
    newConf.setMaximumApplicationMasterResourcePerQueuePercent("root.default",
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
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Trigger scheduling to allocate a container on nm1 for app2.
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
    newConf.setMaximumApplicationMasterResourcePerQueuePercent("root.default",
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
    newConf.setMaximumApplicationMasterResourcePerQueuePercent("root.default",
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
}
