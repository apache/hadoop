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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test functionality of AllocationTagsManager.
 */
public class TestAllocationTagsManager {
  @Test
  public void testAllocationTagsManagerSimpleCases()
      throws InvalidAllocationTagsQueryException {
    AllocationTagsManager atm = new AllocationTagsManager();

    /**
     * Construct test case:
     * Node1:
     *    container_1_1 (mapper/reducer/app_1)
     *    container_1_3 (service/app_1)
     *
     * Node2:
     *    container_1_2 (mapper/reducer/app_1)
     *    container_1_4 (reducer/app_1)
     *    container_2_1 (service/app_2)
     */

    // 3 Containers from app1
    atm.addContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 3),
        ImmutableSet.of("service"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 4),
        ImmutableSet.of("reducer"));

    // 1 Container from app2
    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(2), TestUtils.getMockContainerId(2, 3),
        ImmutableSet.of("service"));

    // Get Cardinality of app1 on node1, with tag "mapper"
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("node1:1234"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of("mapper"),
            Long::max));

    // Get Cardinality of app1 on node2, with tag "mapper/reducer", op=min
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::min));

    // Get Cardinality of app1 on node2, with tag "mapper/reducer", op=max
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::max));

    // Get Cardinality of app1 on node2, with tag "mapper/reducer", op=sum
    Assert.assertEquals(3,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::sum));

    // Get Cardinality by passing single tag.
    Assert.assertEquals(1,
        atm.getNodeCardinality(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1), "mapper"));

    Assert.assertEquals(2,
        atm.getNodeCardinality(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1), "reducer"));

    // Get Cardinality of app1 on node2, with tag "no_existed/reducer", op=min
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("no_existed", "reducer"), Long::min));

    // Get Cardinality of app1 on node2, with tag "<applicationId>", op=max
    // (Expect this returns #containers from app1 on node2)
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1), ImmutableSet
                .of(AllocationTagsNamespaces.APP_ID + TestUtils
                    .getMockApplicationId(1).toString()), Long::max));

    // Get Cardinality of app1 on node2, with empty tag set, op=max
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::max));

    // Get Cardinality of all apps on node2, with empty tag set, op=sum
    Assert.assertEquals(7,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"), null,
            ImmutableSet.of(), Long::sum));

    // Get Cardinality of app_1 on node2, with empty tag set, op=sum
    Assert.assertEquals(5,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::sum));

    // Get Cardinality of app_1 on node2, with empty tag set, op=sum
    Assert.assertEquals(2,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(2), ImmutableSet.of(), Long::sum));

    // Finish all containers:
    atm.removeContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 3),
        ImmutableSet.of("service"));

    atm.removeContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 4),
        ImmutableSet.of("reducer"));

    atm.removeContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(2), TestUtils.getMockContainerId(2, 3),
        ImmutableSet.of("service"));

    // Expect all cardinality to be 0
    // Get Cardinality of app1 on node1, with tag "mapper"
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node1:1234"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of("mapper"),
            Long::max));

    // Get Cardinality of app1 on node2, with tag "mapper/reducer", op=min
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::min));

    // Get Cardinality of app1 on node2, with tag "mapper/reducer", op=max
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::max));

    // Get Cardinality of app1 on node2, with tag "mapper/reducer", op=sum
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of("mapper", "reducer"), Long::sum));

    // Get Cardinality of app1 on node2, with tag "<applicationId>", op=max
    // (Expect this returns #containers from app1 on node2)
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            ImmutableSet.of(TestUtils.getMockApplicationId(1).toString()),
            Long::max));

    Assert.assertEquals(0,
        atm.getNodeCardinality(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1),
            TestUtils.getMockApplicationId(1).toString()));

    // Get Cardinality of app1 on node2, with empty tag set, op=max
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::max));

    // Get Cardinality of all apps on node2, with empty tag set, op=sum
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"), null,
            ImmutableSet.of(), Long::sum));

    // Get Cardinality of app_1 on node2, with empty tag set, op=sum
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(1), ImmutableSet.of(), Long::sum));

    // Get Cardinality of app_1 on node2, with empty tag set, op=sum
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
            TestUtils.getMockApplicationId(2), ImmutableSet.of(), Long::sum));
  }

  @Test
  public void testAllocationTagsManagerMemoryAfterCleanup()
      throws InvalidAllocationTagsQueryException {
    /**
     * Make sure YARN cleans up all memory once container/app finishes.
     */

    AllocationTagsManager atm = new AllocationTagsManager();

    // Add a bunch of containers
    atm.addContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 3),
        ImmutableSet.of("service"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 4),
        ImmutableSet.of("reducer"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(2), TestUtils.getMockContainerId(2, 3),
        ImmutableSet.of("service"));

    // Remove all these containers
    atm.removeContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.removeContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 3),
        ImmutableSet.of("service"));

    atm.removeContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 4),
        ImmutableSet.of("reducer"));

    atm.removeContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(2), TestUtils.getMockContainerId(2, 3),
        ImmutableSet.of("service"));

    // Check internal data structure
    Assert.assertEquals(0,
        atm.getGlobalMapping().getNodeToTagsWithCount().size());
    Assert.assertEquals(0, atm.getPerAppMappings().size());
  }

  @Test
  public void testQueryCardinalityWithIllegalParameters()
      throws InvalidAllocationTagsQueryException {
    /**
     * Make sure YARN cleans up all memory once container/app finishes.
     */

    AllocationTagsManager atm = new AllocationTagsManager();

    // Add a bunch of containers
    atm.addContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 2),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("node1:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 3),
        ImmutableSet.of("service"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(1), TestUtils.getMockContainerId(1, 4),
        ImmutableSet.of("reducer"));

    atm.addContainer(NodeId.fromString("node2:1234"),
        TestUtils.getMockApplicationId(2), TestUtils.getMockContainerId(2, 3),
        ImmutableSet.of("service"));

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
      atm.getNodeCardinalityByOp(NodeId.fromString("node2:1234"),
          TestUtils.getMockApplicationId(2), ImmutableSet.of("mapper"), null);
    } catch (InvalidAllocationTagsQueryException e) {
      caughtException = true;
    }
    Assert.assertTrue("should fail because of nodeId specified",
        caughtException);
  }
}
