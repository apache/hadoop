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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Test functionality of AllocationTagsManager.
 */
public class TestAllocationTagsManager {
  private RMContext rmContext;

  @Before
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
  public void testAllocationTagsManagerSimpleCases()
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
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of("mapper"),
            Long::max));

    // Get Rack Cardinality of app1 on rack0, with tag "mapper"
    Assert.assertEquals(2, atm.getRackCardinality("rack0",
        TestUtils.getMockApplicationId(1), "mapper"));

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=min
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::min));

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=max
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::max));

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=sum
    Assert.assertEquals(3,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::sum));

    // Get Node Cardinality by passing single tag.
    Assert.assertEquals(1,
        atm.getNodeCardinality(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), "mapper"));

    Assert.assertEquals(2,
        atm.getNodeCardinality(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), "reducer"));

    // Get Node Cardinality of app1 on node2, with tag "no_existed/reducer",
    // op=min
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("no_existed", "reducer"), Long::min));

    // Get Node Cardinality of app1 on node2, with tag "<applicationId>", op=max
    // (Expect this returns #containers from app1 on node2)
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), null, Long::max));

    // Get Node Cardinality of app1 on node2, with empty tag set, op=max
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), null, Long::max));

    // Get Cardinality of app1 on node2, with empty tag set, op=max
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::max));

    // Get Node Cardinality of all apps on node2, with empty tag set, op=sum
    Assert.assertEquals(4, atm.getNodeCardinalityByOp(
        NodeId.fromString("host2:123"), null, ImmutableSet.of(), Long::sum));

    // Get Node Cardinality of app_1 on node2, with empty tag set, op=sum
    Assert.assertEquals(3,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::sum));

    // Get Node Cardinality of app_1 on node2, with empty tag set, op=sum
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(2), ImmutableSet.of(), Long::sum));

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
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of("mapper"),
            Long::max));

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=min
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::min));

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=max
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::max));

    // Get Node Cardinality of app1 on node2, with tag "mapper/reducer", op=sum
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::sum));

    // Get Node Cardinality of app1 on node2, with tag "<applicationId>", op=max
    // (Expect this returns #containers from app1 on node2)
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of(TestUtils.getMockApplicationId(1).toString()),
            Long::max));

    Assert.assertEquals(0,
        atm.getNodeCardinality(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1),
            TestUtils.getMockApplicationId(1).toString()));

    // Get Node Cardinality of app1 on node2, with empty tag set, op=max
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::max));

    // Get Node Cardinality of all apps on node2, with empty tag set, op=sum
    Assert.assertEquals(0, atm.getNodeCardinalityByOp(
        NodeId.fromString("host2:123"), null, ImmutableSet.of(), Long::sum));

    // Get Node Cardinality of app_1 on node2, with empty tag set, op=sum
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::sum));

    // Get Node Cardinality of app_2 on node2, with empty tag set, op=sum
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            TestUtils.getMockApplicationId(2), ImmutableSet.of(), Long::sum));
  }


  @Test
  public void testAllocationTagsManagerRackMapping()
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
    Assert.assertEquals(1, atm.getRackCardinality("rack0",
        TestUtils.getMockApplicationId(1), "mapper"));

    // Get Rack Cardinality of app2 on rack0, with tag "reducer"
    Assert.assertEquals(2, atm.getRackCardinality("rack0",
        TestUtils.getMockApplicationId(2), "reducer"));

    // Get Rack Cardinality of all apps on rack0, with tag "reducer"
    Assert.assertEquals(3, atm.getRackCardinality("rack0", null, "reducer"));

    // Get Rack Cardinality of app_1 on rack0, with empty tag set, op=max
    Assert.assertEquals(1, atm.getRackCardinalityByOp("rack0",
        TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::max));

    // Get Rack Cardinality of app_1 on rack0, with empty tag set, op=min
    Assert.assertEquals(1, atm.getRackCardinalityByOp("rack0",
        TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::min));

    // Get Rack Cardinality of all apps on rack0, with empty tag set, op=min
    Assert.assertEquals(3, atm.getRackCardinalityByOp("rack0", null,
        ImmutableSet.of(), Long::max));
  }

  @Test
  public void testAllocationTagsManagerMemoryAfterCleanup()
      throws InvalidAllocationTagsQueryException {
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
    Assert.assertEquals(0,
        atm.getGlobalNodeMapping().getTypeToTagsWithCount().size());
    Assert.assertEquals(0, atm.getPerAppNodeMappings().size());
    Assert.assertEquals(0,
        atm.getGlobalRackMapping().getTypeToTagsWithCount().size());
    Assert.assertEquals(0, atm.getPerAppRackMappings().size());
  }

  @Test
  public void testQueryCardinalityWithIllegalParameters()
      throws InvalidAllocationTagsQueryException {
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
      atm.getNodeCardinalityByOp(null, TestUtils.getMockApplicationId(2),
          ImmutableSet.of("mapper"), Long::min);
    } catch (InvalidAllocationTagsQueryException e) {
      caughtException = true;
    }
    Assert.assertTrue("should fail because of nodeId specified",
        caughtException);

    // No op
    caughtException = false;
    try {
      atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
          TestUtils.getMockApplicationId(2), ImmutableSet.of("mapper"), null);
    } catch (InvalidAllocationTagsQueryException e) {
      caughtException = true;
    }
    Assert.assertTrue("should fail because of nodeId specified",
        caughtException);
  }
}
