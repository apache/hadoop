/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.LayeredNodeUsageBinPackingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.waitforNMRegistered;

public class TestCapacitySchedulerLayeredBinPackingPolicy {

  private CapacitySchedulerConfiguration conf;
  private static final String POLICY_CLASS_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement."
          + "LayeredNodeUsageBinPackingPolicy";

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
  }

  /**
   * Test the scorer output given a usage number.
   * */
  @Test
  public void testLayeredResourceUsageScorer() {
    LayeredNodeUsageBinPackingPolicy policy = new LayeredNodeUsageBinPackingPolicy(true);
    // threshold 0.6
    float threshold = (float)0.6;
    float currentUsage = (float)0.2;
    float normalNodeScore = policy.scorer(currentUsage, threshold);
    // node with resource usage 0.2
    Assert.assertEquals(2000 + 3 * currentUsage * 100,
        normalNodeScore, 0);
    // node with resource usage 0.5
    currentUsage = (float)0.5;
    Assert.assertEquals(2000 + 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0.6
    currentUsage = (float)0.6;
    Assert.assertEquals(1000 - 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0.7
    currentUsage = (float)0.7;
    float busyNodeScore = policy.scorer(currentUsage, threshold);
    Assert.assertEquals(1000 - 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0.9
    currentUsage = (float)0.9;
    float busyNodeScore2 = policy.scorer(currentUsage, threshold);
    Assert.assertEquals(1000 - 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0, the score shouldn't exceed 100
    currentUsage = (float)0.0;
    float freeNodeScore = policy.scorer(currentUsage, threshold);
    if ( freeNodeScore >= 100.0) {
      Assert.assertFalse(
          "Node usage 0 shoudn't have a score higher than 100. Actual:"
              + freeNodeScore,
          true);
    } else {
      Assert.assertTrue(true);
    }

      assertTrue("Node usage 0 shoudn't have a score higher than 100. Actual:"
          + freeNodeScore, freeNodeScore < 100.0);

      //node with resource usage 1, the score should be 0
      currentUsage = (float)1.0;
      float fullNodeScore = policy.scorer(currentUsage, threshold);
      assertEquals(0, fullNodeScore, 0);

      assertTrue(
          "High usage busy node score should be smaller to avoid hot spot",
          busyNodeScore2 < busyNodeScore);
      assertTrue("Busy node score should be smaller than normal node",
          busyNodeScore < normalNodeScore);
      assertTrue("Busy node score should be larger than free node",
          busyNodeScore > freeNodeScore);
      assertTrue("Full usage node score should be smaller than free node",
          fullNodeScore < freeNodeScore);
    }

    @Test
    public void testLayeredFullResourceUsageOrdering() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[4];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB, 1);
      nms[0] = nm1;
      MockNM nm2 = rm.registerNode("127.0.0.2:1235", 10 * GB, 1);
      nms[1] = nm2;
      MockNM nm3 = rm.registerNode("127.0.0.3:1236", 10 * GB, 1);
      nms[2] = nm3;
      MockNM nm4 = rm.registerNode("127.0.0.4:1237", 10 * GB, 1);
      nms[3] = nm4;
      MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
            .getMultiNodeSortingManager();
      MultiNodeSorter<SchedulerNode> sorter = mns
            .getMultiNodePolicy(POLICY_CLASS_NAME);
      sorter.reSortClusterNodes();
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(10 * GB, rm)
              .withAppName("app-1")
              .withUser("user1")
              .withAcls(null)
              .withQueue("default")
              .build();
      RMApp app1 = MockRMAppSubmitter.submit(rm, data);
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);
      heartbeat(rm, nm1);
      heartbeat(rm, nm2);
      heartbeat(rm, nm3);
      heartbeat(rm, nm4);
      MockNM chosenNode = null;
      for(int i=0; i<nms.length; i++) {
        SchedulerNodeReport reportNM =
              rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
        if(reportNM.getUsedResource().getMemorySize() == 10 * GB) {
          chosenNode = nms[i];
          break;
        }
      }
      sorter.reSortClusterNodes();
      Set<SchedulerNode> nodeList = sorter.getMultiNodeLookupPolicy()
           .getNodesPerPartition("");
      Iterator<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
            .getPreferredNodeIterator(nodeList, "", null);
      Assert.assertEquals("Nodes size does not match", 4, nodeList.size());
      SchedulerNode lastNM = nodes.next();
      while (nodes.hasNext()) {
        lastNM = nodes.next();
      }
      Assert.assertEquals(chosenNode.getNodeId(), lastNM.getNodeID());
      rm.stop();
    }

  /**
   * The container requests will generate nodes with resource usage
   * 0.6, 0.5, 0.7 gradually. Then it will verify the nodes order.
   * */
  @Test
  public void testMultiNodeSorterForLayeredBinPacking() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM[] nms = new MockNM[4];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 100 * GB, 100);
    MockNM nm2 = rm.registerNode("127.0.0.2:1235", 100 * GB, 100);
    MockNM nm3 = rm.registerNode("127.0.0.3:1236", 100 * GB, 100);
    MockNM nm4 = rm.registerNode("127.0.0.4:1237", 100 * GB, 100);

    nms[0] = nm4;
    nms[1] = nm3;
    nms[2] = nm2;
    nms[3] = nm1;

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

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm)
            .withAppName("app-1")
            .withUser("user1")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);

    // Get the first chosen node
    MockNM firstChosenNode = null;
    int firstChosenNodeIndex = 0;
    for (int i = 0; i < nms.length; i++) {
      SchedulerNodeReport reportNM =
          rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
      if (reportNM.getUsedResource().getMemorySize() == 2 * GB) {
        firstChosenNode = nms[i];
        firstChosenNodeIndex = i;
        break;
      }
    }

    // check node report
    SchedulerNodeReport reportNM = rm.getResourceScheduler()
        .getNodeReport(firstChosenNode.getNodeId());
    Assert.assertEquals(2 * GB, reportNM.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 2) * GB,
        reportNM.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    // Another app will be scheduled in the first chosen node
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app-2")
            .withUser("user2")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data2);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
    reportNM =
        rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
    Resource cResource = Resources.createResource(5 * GB, 1);
    am2.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    heartbeat(rm, firstChosenNode);

    // check node report
    Assert.assertEquals(8 * GB, reportNM.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 8) * GB,
        reportNM.getAvailableResource().getMemorySize());

    // am request containers. Now totally 60GB, the first chosen node will fall
    // into busy layer
    cResource = Resources.createResource(52 * GB, 1);
    am1.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);

    heartbeat(rm, firstChosenNode);
    reportNM =
        rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
    // the above resource request should be allocated to first chosen node
    Assert.assertEquals(60 * GB, reportNM.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 60) * GB,
        reportNM.getAvailableResource().getMemorySize());

    // This container request cannot be served by the first chosen node.
    // It should pick a random node as second chosen node
    cResource = Resources.createResource(50 * GB, 1);
    am1.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    // first chosen node doesn't change usage
    heartbeat(rm, firstChosenNode);
    reportNM =
        rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
    Assert.assertEquals(60 * GB, reportNM.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 60) * GB,
        reportNM.getAvailableResource().getMemorySize());
    // heartbeat nodes
    heartbeat(rm, nm1);
    heartbeat(rm, nm2);
    heartbeat(rm, nm3);
    heartbeat(rm, nm4);
    // This container request cannot be served by first and second. Pick another
    // random one
    cResource = Resources.createResource(70 * GB, 1);
    am1.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    // heartbeat nodes
    heartbeat(rm, nm1);
    heartbeat(rm, nm2);
    heartbeat(rm, nm3);
    heartbeat(rm, nm4);
    // Get the second chosen node
    MockNM secondChosenNode = null;
    int secondChosenNodeIndex = 0;
    MockNM thirdChosenNode = null;
    int thirdChosenNodeIndex = 0;
    for (int i = 0; i < nms.length; i++) {
      reportNM =
          rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
      if (reportNM.getUsedResource().getMemorySize() == 50 * GB) {
        secondChosenNode = nms[i];
        secondChosenNodeIndex = i;
      }
      if (reportNM.getUsedResource().getMemorySize() == 70 * GB) {
        thirdChosenNode = nms[i];
        thirdChosenNodeIndex = i;
      }
    }

    SchedulerNodeReport reportSecondNM =
        rm.getResourceScheduler().getNodeReport(secondChosenNode.getNodeId());
    Assert.assertEquals(50 * GB, reportSecondNM.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 50) * GB,
        reportSecondNM.getAvailableResource().getMemorySize());

    SchedulerNodeReport reportThirdNM =
        rm.getResourceScheduler().getNodeReport(thirdChosenNode.getNodeId());
    Assert.assertEquals(70 * GB, reportThirdNM.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 70) * GB,
        reportThirdNM.getAvailableResource().getMemorySize());

    for (int i = 0; i < nms.length; i++) {
      if (i == firstChosenNodeIndex) {
        continue;
      }
      if (i == secondChosenNodeIndex) {
        continue;
      }
      if (i == thirdChosenNodeIndex) {
        continue;
      }
      reportNM =
          rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
      Assert.assertEquals(0 * GB, reportNM.getUsedResource().getMemorySize());
      Assert.assertEquals((100 - 0) * GB,
          reportNM.getAvailableResource().getMemorySize());
    }

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    List<NodeId> currentNodes = new ArrayList<>();
    // second one usage is 0.5, first one usage is 0.6, third usage is 0.7
    currentNodes.add(nms[secondChosenNodeIndex].getNodeId());
    currentNodes.add(nms[firstChosenNodeIndex].getNodeId());
    currentNodes.add(nms[thirdChosenNodeIndex].getNodeId());
    Iterator<SchedulerNode> it = nodes.iterator();
    SchedulerNode current;
    int i = 0;
    while (it.hasNext()) {
      current = it.next();
      Assert.assertEquals(current.getNodeID(), currentNodes.get(i++));
      if (i == 2) {
        break;
      }
    }
    rm.stop();
  }

  /**
   * This is to test reservation won't block allocating pending container when
   * there's available resource in the other nodes
   * In this case, when node1 has no enought resource to use,
   * but node2 has available resource,
   * There should be no reservation created which make containers pending there.
   * */
  @Test
  public void testSkipReservationWhenPossible() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM[] nms = new MockNM[2];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 13 * GB, 100);
    MockNM nm2 = rm.registerNode("127.0.0.2:1235", 13 * GB, 100);

    nms[0] = nm2;
    nms[1] = nm1;

    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 2, 5);

    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();

    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(2, nodes.size());

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm)
            .withAppName("app-1")
            .withUser("user1")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);

    // Get the first chosen node
    MockNM firstChosenNode = null;
    int firstChosenNodeIndex = 0;
    MockNM secondChosenNode = null;
    int secondChosenNodeIndex = 0;
    for (int i = 0; i < nms.length; i++) {
      SchedulerNodeReport reportNM =
          rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
      if (reportNM.getUsedResource().getMemorySize() == 2 * GB) {
        firstChosenNode = nms[i];
        firstChosenNodeIndex = i;
      } else {
        secondChosenNode = nms[i];
        secondChosenNodeIndex = i;
      }
    }
    // check node report
    SchedulerNodeReport reportNM = rm.getResourceScheduler().
        getNodeReport(firstChosenNode.getNodeId());
    Assert.assertEquals(2 * GB, reportNM.getUsedResource().getMemorySize());
    Assert.assertEquals((13 - 2) * GB,
        reportNM.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app-2")
            .withUser("user2")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data2);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
    SchedulerNodeReport reportNm2 =
        rm.getResourceScheduler().getNodeReport(secondChosenNode.getNodeId());
    Resource cResource = Resources.createResource(5 * GB, 1);
    am2.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    heartbeat(rm, nm2);
    // check node report
    Assert.assertEquals(0 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals(13 * GB,
        reportNm2.getAvailableResource().getMemorySize());

    // check node report
    reportNM = rm.getResourceScheduler().
        getNodeReport(firstChosenNode.getNodeId());
    Assert.assertEquals(8 * GB, reportNM.getUsedResource().getMemorySize());
    Assert.assertEquals((13 - 8) * GB,
        reportNM.getAvailableResource().getMemorySize());

    // am request containers, this request should be served by second node
    // due to first one has no headroom
    cResource = Resources.createResource(6 * GB, 1);
    am1.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);

    heartbeat(rm, nm1);
    heartbeat(rm, nm2);
    reportNM =
        rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
    Assert.assertEquals(8 * GB, reportNM.getUsedResource().getMemorySize());
    Assert.assertEquals((13 - 8) * GB,
        reportNM.getAvailableResource().getMemorySize());

    // check node report
    reportNm2 =
        rm.getResourceScheduler().getNodeReport(secondChosenNode.getNodeId());
    Assert.assertEquals(6 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals((13 - 6) * GB,
        reportNm2.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    List<NodeId> currentNodes = new ArrayList<>();
    // first chosen node usage is 8/13 (busy layer)
    // second chosen node usage is 6/13 (normal layer)
    currentNodes.add(secondChosenNode.getNodeId());
    currentNodes.add(firstChosenNode.getNodeId());
    Iterator<SchedulerNode> it = nodes.iterator();
    SchedulerNode current;
    int i = 0;
    while (it.hasNext()) {
      current = it.next();
      Assert.assertEquals(current.getNodeID(), currentNodes.get(i++));
    }
    rm.stop();
  }

  /**
   * This is to test that big containers should be reserved when there's
   * no headroom in the cluster. When there's headroom,
   * it will allocate this container from reservation
   * */
  @Test
  public void testReserveContainerAndThenAllocated() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM[] nms = new MockNM[2];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 100);
    MockNM nm2 = rm.registerNode("127.0.0.2:1235", 1 * GB, 100);

    nms[0] = nm2;
    nms[1] = nm1;

    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 2, 5);

    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();

    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(2, nodes.size());
    
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm)
            .withAppName("app-1")
            .withUser("user1")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);
    SchedulerNodeReport reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    // check node report
    Assert.assertEquals(2 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals((5 - 2) * GB,
        reportNm1.getAvailableResource().getMemorySize());
    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app-2")
            .withUser("user2")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data2);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
    // This 4GB request should be reserved
    Resource cResource = Resources.createResource(4 * GB, 1);
    am2.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    heartbeat(rm, nm2);

    // finish app1
    finishApp(rm, nm1, app1, am1, 1);

    heartbeat(rm, nm1);
    // It should success due to previous reservation
    am2.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(5 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        reportNm1.getAvailableResource().getMemorySize());
    SchedulerNodeReport reportNm2 =
        rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(0 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals(1 * GB,
        reportNm2.getAvailableResource().getMemorySize());
    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();
    rm.stop();
  }

  private void heartbeat(MockRM rm, MockNM nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  private void finishApp(MockRM rm, MockNM nm,
      RMApp app, MockAM am, int cout) throws Exception {
    am.unregisterAppAttempt();
    //complete the AM container to finish the app normally
    nm.nodeHeartbeat(app.getCurrentAppAttempt().getAppAttemptId(),
        cout, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);
  }

}