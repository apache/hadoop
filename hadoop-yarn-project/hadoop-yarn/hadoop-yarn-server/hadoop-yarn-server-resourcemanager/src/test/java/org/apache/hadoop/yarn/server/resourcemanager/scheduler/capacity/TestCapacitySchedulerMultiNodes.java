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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
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

  private static final Log LOG = LogFactory
      .getLog(TestCapacitySchedulerMultiNodes.class);
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

    RMApp app1 = rm.submitApp(2048, "app-1", "user1", null, "default");
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

    RMApp app2 = rm.submitApp(1024, "app-2", "user2", null, "default");
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
}
