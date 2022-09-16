/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Test functionality of AllocationTagsManager.
 */
public class TestAllocationTagsManager {
  private RMContext rmContext;

  @BeforeEach
  public void setup() {
    MockRM rm = new MockRM();
    rm.start();
    MockNodes.resetHostIds();
    List<RMNode> rmNodes =
        MockNodes.newNodes(2, 4, Resource.newInstance(4096, 4));
    for (RMNode rmNode : rmNodes) {
      rm.getRMContext().getRMNodes().putIfAbsent(rmNode.getNodeID(), rmNode);
    }
    rmContext = rm.getRMContext();
  }

  @Test
  void testMultipleAddRemoveContainer() {
    AllocationTagsManager atm = new AllocationTagsManager(rmContext);

    NodeId nodeId = NodeId.fromString("host1:123");
    ContainerId cid1 = TestUtils.getMockContainerId(1, 1);
    ContainerId cid2 = TestUtils.getMockContainerId(1, 2);
    ContainerId cid3 = TestUtils.getMockContainerId(1, 3);
    Set<String> tags1 = ImmutableSet.of("mapper", "reducer");
    Set<String> tags2 = ImmutableSet.of("mapper");
    Set<String> tags3 = ImmutableSet.of("zk");

    // node - mapper : 2
    //      - reduce : 1
    atm.addContainer(nodeId, cid1, tags1);
    atm.addContainer(nodeId, cid2, tags2);
    atm.addContainer(nodeId, cid3, tags3);
    Assertions.assertEquals(2L,
        (long) atm.getAllocationTagsWithCount(nodeId).get("mapper"));
    Assertions.assertEquals(1L,
        (long) atm.getAllocationTagsWithCount(nodeId).get("reducer"));

    // remove container1
    atm.removeContainer(nodeId, cid1, tags1);
    Assertions.assertEquals(1L,
        (long) atm.getAllocationTagsWithCount(nodeId).get("mapper"));
    Assertions.assertNull(atm.getAllocationTagsWithCount(nodeId).get("reducer"));

    // remove the same container again, the reducer no longer exists,
    // make sure there is no NPE here
    atm.removeContainer(nodeId, cid1, tags1);
    Assertions.assertNull(atm.getAllocationTagsWithCount(nodeId).get("mapper"));
    Assertions.assertNull(atm.getAllocationTagsWithCount(nodeId).get("reducer"));
  }

  @Test
  void testAllocationTagsManagerSimpleCases()
      throws InvalidAllocationTagsQueryException {

    AllocationTagsManager atm = new AllocationTagsManager(rmContext);

    /**
     * Construct test case:
     * Node1 (rack0):
     *    container_1_1 (mapper/reducer/app_1)
     *    container_1_3 (service/app_1)
     *
     * Node2 (rack0):
     *    container_1_2 (mapper/reducer/app_1)
     *    container_1_4 (reducer/app_1)
     *    container_2_1 (service/app_2)
     */

    // 3 Containers from app1
    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 3),
        ImmutableSet.of("service"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 4),
        ImmutableSet.of("reducer"));

    // 1 Container from app2
    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(2, 3),
        ImmutableSet.of("service"));

    // Get Node Cardinality of app1 on node1, with tag "mapper"
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper")),
            Long::max),
            1);

    // Get Rack Cardinality of app1 on rack0, with tag "mapper"
    Assertions.assertEquals(atm.getRackCardinality("rack0", TestUtils.getMockApplicationId(1), "mapper"),
        2);

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=min
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper", "reducer")),
            Long::min),
            1);

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=max
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper", "reducer")),
            Long::max),
            2);

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper", "reducer")),
            Long::sum),
            3);

    // Get Node Cardinality by passing single tag.
    Assertions.assertEquals(atm.getNodeCardinality(NodeId.fromString("host2:123"),
        TestUtils.getMockApplicationId(1), "mapper"),
            1);

    Assertions.assertEquals(atm.getNodeCardinality(NodeId.fromString("host2:123"),
        TestUtils.getMockApplicationId(1), "reducer"),
            2);

    // Get Node Cardinality of app1 on node2, with tag "no_existed/reducer",
    // op=min
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("no_existed", "reducer")),
            Long::min),
            0);

    // Get Node Cardinality of app1 on node2, with tag "<applicationId>", op=max
    // (Expect this returns #containers from app1 on node2)
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1), null),
            Long::max),
            2);

    // Get Node Cardinality of app1 on node2, with empty tag set, op=max
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1), null),
            Long::max),
            2);

    // Get Cardinality of app1 on node2, with empty tag set, op=max
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1), ImmutableSet.of()),
            Long::max),
            2);

    // Get Node Cardinality of all apps on node2, with empty tag set, op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(
        NodeId.fromString("host2:123"), AllocationTags.createGlobalAllocationTags(ImmutableSet.of()),
        Long::sum),
        4);

    // Get Node Cardinality of app_1 on node2, with empty tag set, op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1), ImmutableSet.of()),
            Long::sum),
            3);

    // Get Node Cardinality of app_1 on node2, with empty tag set, op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(2), ImmutableSet.of()),
            Long::sum),
            1);

    // Finish all containers:
    atm.removeContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 3), ImmutableSet.of("service"));

    atm.removeContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 4), ImmutableSet.of("reducer"));

    atm.removeContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(2, 3), ImmutableSet.of("service"));

    // Expect all cardinality to be 0
    // Get Cardinality of app1 on node1, with tag "mapper"
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper")),
            Long::max),
            0);

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=min
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper", "reducer")),
            Long::min),
            0);

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=max
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper", "reducer")),
            Long::max),
            0);

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper", "reducer")),
            Long::sum),
            0);

    // Get Node Cardinality of app1 on node2, with tag "<applicationId>", op=max
    // (Expect this returns #containers from app1 on node2)
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of(TestUtils.getMockApplicationId(1).toString())),
            Long::max),
            0);

    Assertions.assertEquals(atm.getNodeCardinality(NodeId.fromString("host2:123"),
        TestUtils.getMockApplicationId(1),
            TestUtils.getMockApplicationId(1).toString()),
            0);

    // Get Node Cardinality of app1 on node2, with empty tag set, op=max
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of()),
            Long::max),
            0);

    // Get Node Cardinality of all apps on node2, with empty tag set, op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(
        NodeId.fromString("host2:123"), AllocationTags.createGlobalAllocationTags(ImmutableSet.of()),
        Long::sum),
        0);

    // Get Node Cardinality of app_1 on node2, with empty tag set, op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of()),
            Long::sum),
            0);

    // Get Node Cardinality of app_2 on node2, with empty tag set, op=sum
    Assertions.assertEquals(atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
        AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of()),
            Long::sum),
            0);
  }


  @Test
  void testAllocationTagsManagerRackMapping()
      throws InvalidAllocationTagsQueryException {

    AllocationTagsManager atm = new AllocationTagsManager(rmContext);

    /**
     * Construct Rack test case:
     * Node1 (rack0):
     *    container_1_1 (mapper/reducer/app_1)
     *    container_1_4 (reducer/app_2)
     *
     * Node2 (rack0):
     *    container_1_2 (mapper/reducer/app_2)
     *    container_1_3 (service/app_1)
     *
     * Node5 (rack1):
     *    container_2_1 (service/app_2)
     */

    // 3 Containers from app1
    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(2, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(2, 4), ImmutableSet.of("reducer"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 3), ImmutableSet.of("service"));

    // 1 Container from app2
    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(2, 3), ImmutableSet.of("service"));

    // Get Rack Cardinality of app1 on rack0, with tag "mapper"
    Assertions.assertEquals(atm.getRackCardinality("rack0", TestUtils.getMockApplicationId(1), "mapper"),
        1);

    // Get Rack Cardinality of app2 on rack0, with tag "reducer"
    Assertions.assertEquals(atm.getRackCardinality("rack0", TestUtils.getMockApplicationId(2), "reducer"),
        2);

    // Get Rack Cardinality of all apps on rack0, with tag "reducer"
    Assertions.assertEquals(atm.getRackCardinality("rack0", null, "reducer"), 3);

    // Get Rack Cardinality of app_1 on rack0, with empty tag set, op=max
    Assertions.assertEquals(atm.getRackCardinalityByOp("rack0", AllocationTags.createSingleAppAllocationTags(
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of()),
        Long::max),
        1);

    // Get Rack Cardinality of app_1 on rack0, with empty tag set, op=min
    Assertions.assertEquals(atm.getRackCardinalityByOp("rack0", AllocationTags.createSingleAppAllocationTags(
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of()),
        Long::min),
        1);

    // Get Rack Cardinality of all apps on rack0, with empty tag set, op=min
    Assertions.assertEquals(atm.getRackCardinalityByOp("rack0", AllocationTags.createGlobalAllocationTags(ImmutableSet.of()),
        Long::max),
        3);
  }

  @Test
  void testAllocationTagsManagerMemoryAfterCleanup() {
    /**
     * Make sure YARN cleans up all memory once container/app finishes.
     */

    AllocationTagsManager atm = new AllocationTagsManager(rmContext);

    // Add a bunch of containers
    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 3), ImmutableSet.of("service"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 4), ImmutableSet.of("reducer"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(2, 3), ImmutableSet.of("service"));

    // Remove all these containers
    atm.removeContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 3), ImmutableSet.of("service"));

    atm.removeContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 4), ImmutableSet.of("reducer"));

    atm.removeContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(2, 3), ImmutableSet.of("service"));

    // Check internal data structure
    Assertions.assertEquals(0,
        atm.getGlobalNodeMapping().getTypeToTagsWithCount().size());
    Assertions.assertEquals(0, atm.getPerAppNodeMappings().size());
    Assertions.assertEquals(0,
        atm.getGlobalRackMapping().getTypeToTagsWithCount().size());
    Assertions.assertEquals(0, atm.getPerAppRackMappings().size());
  }

  @Test
  void testQueryCardinalityWithIllegalParameters() {
    /**
     * Make sure YARN cleans up all memory once container/app finishes.
     */

    AllocationTagsManager atm = new AllocationTagsManager(rmContext);

    // Add a bunch of containers
    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 3), ImmutableSet.of("service"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 4), ImmutableSet.of("reducer"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(2, 3), ImmutableSet.of("service"));

    // No node-id
    boolean caughtException = false;
    try {
      atm.getNodeCardinalityByOp(null,
          AllocationTags.createSingleAppAllocationTags(
              TestUtils.getMockApplicationId(2),
              ImmutableSet.of("mapper")),
          Long::min);
    } catch (InvalidAllocationTagsQueryException e1) {
      caughtException = true;
    }
    Assertions.assertTrue(caughtException,
        "should fail because of nodeId specified");

    // No op
    caughtException = false;
    try {
      atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
          AllocationTags.createSingleAppAllocationTags(
              TestUtils.getMockApplicationId(2),
              ImmutableSet.of("mapper")),
          null);
    } catch (InvalidAllocationTagsQueryException e1) {
      caughtException = true;
    }
    Assertions.assertTrue(caughtException,
        "should fail because of nodeId specified");
  }

  @Test
  void testNodeAllocationTagsAggregation()
      throws InvalidAllocationTagsQueryException {
    RMContext mockContext = Mockito.spy(rmContext);

    ApplicationId app1 = TestUtils.getMockApplicationId(1);
    ApplicationId app2 = TestUtils.getMockApplicationId(2);
    ApplicationId app3 = TestUtils.getMockApplicationId(3);

    NodeId host1 = NodeId.fromString("host1:123");
    NodeId host2 = NodeId.fromString("host2:123");
    NodeId host3 = NodeId.fromString("host3:123");

    ConcurrentMap<ApplicationId, RMApp> allApps = new ConcurrentHashMap<>();
    allApps.put(app1, new MockRMApp(123, 1000,
        RMAppState.NEW, "userA", ImmutableSet.of("")));
    allApps.put(app2, new MockRMApp(124, 1001,
        RMAppState.NEW, "userA", ImmutableSet.of("")));
    allApps.put(app3, new MockRMApp(125, 1002,
        RMAppState.NEW, "userA", ImmutableSet.of("")));
    Mockito.when(mockContext.getRMApps()).thenReturn(allApps);

    AllocationTagsManager atm = new AllocationTagsManager(mockContext);

    /**
     * Node1 (rack0)
     *   app1/A(2)
     *   app1/B(1)
     *   app2/A(3)
     *   app3/A(1)
     *
     * Node2 (rack0)
     *   app2/A(1)
     *   app2/B(2)
     *   app1/C(1)
     *   app3/B(1)
     *
     * Node3 (rack1):
     *   app2/D(1)
     *   app3/D(1)
     */
    atm.addContainer(host1, TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("A", "B"));
    atm.addContainer(host1, TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("A"));
    atm.addContainer(host1, TestUtils.getMockContainerId(2, 1),
        ImmutableSet.of("A"));
    atm.addContainer(host1, TestUtils.getMockContainerId(2, 2),
        ImmutableSet.of("A"));
    atm.addContainer(host1, TestUtils.getMockContainerId(2, 3),
        ImmutableSet.of("A"));
    atm.addContainer(host1, TestUtils.getMockContainerId(3, 1),
        ImmutableSet.of("A"));

    atm.addContainer(host2, TestUtils.getMockContainerId(1, 3),
        ImmutableSet.of("C"));
    atm.addContainer(host2, TestUtils.getMockContainerId(2, 4),
        ImmutableSet.of("A"));
    atm.addContainer(host2, TestUtils.getMockContainerId(2, 5),
        ImmutableSet.of("B"));
    atm.addContainer(host2, TestUtils.getMockContainerId(2, 6),
        ImmutableSet.of("B"));
    atm.addContainer(host2, TestUtils.getMockContainerId(3, 2),
        ImmutableSet.of("B"));

    atm.addContainer(host3, TestUtils.getMockContainerId(2, 7),
        ImmutableSet.of("D"));
    atm.addContainer(host3, TestUtils.getMockContainerId(3, 3),
        ImmutableSet.of("D"));

    // Target applications, current app: app1
    // all apps: app1, app2, app3
    TargetApplications ta = new TargetApplications(app1,
        ImmutableSet.of(app1, app2, app3));

    //********************************
    // 1) self (app1)
    //********************************
    AllocationTags tags = AllocationTags
        .createSingleAppAllocationTags(app1, ImmutableSet.of("A", "C"));
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::max), 2);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::min), 0);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::max), 1);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::min), 0);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host3, tags, Long::max), 0);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host3, tags, Long::min), 0);

    //********************************
    // 2) not-self (app2, app3)
    //********************************
    /**
     * Verify max/min cardinality of tag "A" on host1 from all applications
     * other than app1. This returns the max/min cardinality of tag "A" of
     * app2 or app3 on this node.
     *
     * Node1 (rack0)
     *   app1/A(1)
     *   app1/B(1)
     *   app2/A(3)
     *   app3/A(1)
     *
     *   app2_app3/A(4)
     *   app2_app3/B(0)
     *
     * expecting to return max=3, min=1
     *
     */
    tags = AllocationTags.createOtherAppAllocationTags(app1,
        ImmutableSet.of("A", "B"));

    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::max), 4);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::min), 0);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::sum), 4);

    //********************************
    // 3) app-id/app2 (app2)
    //********************************
    tags = AllocationTags
        .createSingleAppAllocationTags(app2, ImmutableSet.of("A", "B"));
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::max), 3);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::min), 0);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::max), 2);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::min), 1);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::sum), 3);


    //********************************
    // 4) all (app1, app2, app3)
    //********************************
    tags = AllocationTags
        .createGlobalAllocationTags(ImmutableSet.of("A"));
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::sum), 6);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::sum), 1);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host3, tags, Long::sum), 0);

    tags = AllocationTags
        .createGlobalAllocationTags(ImmutableSet.of("A", "B"));
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::sum), 7);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::sum), 4);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host3, tags, Long::sum), 0);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::max), 6);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::max), 3);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host3, tags, Long::max), 0);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host1, tags, Long::min), 1);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host2, tags, Long::min), 1);
    Assertions.assertEquals(atm.getNodeCardinalityByOp(host3, tags, Long::min), 0);
  }
}
