/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTags;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Tests the LocalAllocationTagsManager.
 */
public class TestLocalAllocationTagsManager {

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
  public void testTempContainerAllocations()
      throws InvalidAllocationTagsQueryException {
    /**
     * Construct both TEMP and normal containers: Node1: TEMP container_1_1
     * (mapper/reducer/app_1) container_1_2 (service/app_1)
     *
     * Node2: container_1_3 (reducer/app_1) TEMP container_2_1 (service/app_2)
     */

    AllocationTagsManager atm = new AllocationTagsManager(rmContext);
    LocalAllocationTagsManager ephAtm =
        new LocalAllocationTagsManager(atm);

    // 3 Containers from app1
    ephAtm.addTempTags(NodeId.fromString("host1:123"),
        TestUtils.getMockApplicationId(1),
        ImmutableSet.of("mapper", "reducer"));

    atm.addContainer(NodeId.fromString("host1:123"),
        TestUtils.getMockContainerId(1, 2), ImmutableSet.of("service"));

    atm.addContainer(NodeId.fromString("host2:123"),
        TestUtils.getMockContainerId(1, 3), ImmutableSet.of("reducer"));

    // 1 Container from app2
    ephAtm.addTempTags(NodeId.fromString("host2:123"),
        TestUtils.getMockApplicationId(2), ImmutableSet.of("service"));

    // Expect tag mappings to be present including temp Tags
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper")),
            Long::sum));

    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("service")),
            Long::sum));

    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(2),
                ImmutableSet.of("service")),
            Long::sum));

    // Do a temp Tag cleanup on app2
    ephAtm.cleanTempContainers(TestUtils.getMockApplicationId(2));
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(2),
                ImmutableSet.of("service")),
            Long::sum));
    // Expect app1 to be unaffected
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper")),
            Long::sum));
    // Do a cleanup on app1 as well
    ephAtm.cleanTempContainers(TestUtils.getMockApplicationId(1));
    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("mapper")),
            Long::sum));

    // Non temp-tags should be unaffected
    Assert.assertEquals(1,
        atm.getNodeCardinalityByOp(NodeId.fromString("host1:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(1),
                ImmutableSet.of("service")),
            Long::sum));

    Assert.assertEquals(0,
        atm.getNodeCardinalityByOp(NodeId.fromString("host2:123"),
            AllocationTags.createSingleAppAllocationTags(
                TestUtils.getMockApplicationId(2),
                ImmutableSet.of("service")),
            Long::sum));

    // Expect app2 with no containers, and app1 with 2 containers across 2 nodes
    Assert.assertEquals(2,
        atm.getPerAppNodeMappings().get(TestUtils.getMockApplicationId(1))
            .getTypeToTagsWithCount().size());

    Assert.assertNull(
        atm.getPerAppNodeMappings().get(TestUtils.getMockApplicationId(2)));
  }

}
