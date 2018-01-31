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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.RACK;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Test the PlacementConstraint Utility class functionality.
 */
public class TestPlacementConstraintsUtil {

  private List<RMNode> rmNodes;
  private RMContext rmContext;
  private static final int GB = 1024;
  private ApplicationId appId1;
  private PlacementConstraint c1, c2, c3, c4;
  private Set<String> sourceTag1, sourceTag2;
  private Map<Set<String>, PlacementConstraint> constraintMap1, constraintMap2;
  private AtomicLong requestID = new AtomicLong(0);

  @Before
  public void setup() {
    MockRM rm = new MockRM();
    rm.start();
    MockNodes.resetHostIds();
    rmNodes = MockNodes.newNodes(2, 2, Resource.newInstance(4096, 4));
    for (RMNode rmNode : rmNodes) {
      rm.getRMContext().getRMNodes().putIfAbsent(rmNode.getNodeID(), rmNode);
    }
    rmContext = rm.getRMContext();

    // Build appIDs, constraints, source tags, and constraint map.
    long ts = System.currentTimeMillis();
    appId1 = BuilderUtils.newApplicationId(ts, 123);

    c1 = PlacementConstraints.build(targetIn(NODE, allocationTag("hbase-m")));
    c2 = PlacementConstraints.build(targetIn(RACK, allocationTag("hbase-rs")));
    c3 = PlacementConstraints
        .build(targetNotIn(NODE, allocationTag("hbase-m")));
    c4 = PlacementConstraints
        .build(targetNotIn(RACK, allocationTag("hbase-rs")));

    sourceTag1 = new HashSet<>(Arrays.asList("spark"));
    sourceTag2 = new HashSet<>(Arrays.asList("zk"));

    constraintMap1 = Stream
        .of(new AbstractMap.SimpleEntry<>(sourceTag1, c1),
            new AbstractMap.SimpleEntry<>(sourceTag2, c2))
        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey,
            AbstractMap.SimpleEntry::getValue));
    constraintMap2 = Stream
        .of(new AbstractMap.SimpleEntry<>(sourceTag1, c3),
            new AbstractMap.SimpleEntry<>(sourceTag2, c4))
        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey,
            AbstractMap.SimpleEntry::getValue));
  }

  private SchedulingRequest createSchedulingRequest(Set<String> allocationTags,
      PlacementConstraint constraint) {
    return SchedulingRequest
        .newInstance(requestID.incrementAndGet(),
            Priority.newInstance(0),
            ExecutionTypeRequest.newInstance(),
            allocationTags,
            ResourceSizing.newInstance(Resource.newInstance(1024, 3)),
            constraint);
  }

  private SchedulingRequest createSchedulingRequest(Set<String>
      allocationTags) {
    return createSchedulingRequest(allocationTags, null);
  }

  @Test
  public void testNodeAffinityAssignment()
      throws InvalidAllocationTagsQueryException {
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    // Register App1 with affinity constraint map
    pcm.registerApplication(appId1, constraintMap1);
    // No containers are running so all 'zk' and 'spark' allocations should fail
    // on every cluster NODE
    Iterator<RMNode> nodeIterator = rmNodes.iterator();
    while (nodeIterator.hasNext()) {
      RMNode currentNode = nodeIterator.next();
      FiCaSchedulerNode schedulerNode = TestUtils.getMockNode(
          currentNode.getHostName(), currentNode.getRackName(), 123, 4 * GB);
      Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
          createSchedulingRequest(sourceTag1), schedulerNode, pcm, tm));
      Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
          createSchedulingRequest(sourceTag2), schedulerNode, pcm, tm));
    }
    /**
     * Now place container:
     * Node0:123 (Rack1):
     *    container_app1_1 (hbase-m)
     */
    RMNode n0_r1 = rmNodes.get(0);
    RMNode n1_r1 = rmNodes.get(1);
    RMNode n2_r2 = rmNodes.get(2);
    RMNode n3_r2 = rmNodes.get(3);
    FiCaSchedulerNode schedulerNode0 = TestUtils
        .getMockNode(n0_r1.getHostName(), n0_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode1 = TestUtils
        .getMockNode(n1_r1.getHostName(), n1_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode2 = TestUtils
        .getMockNode(n2_r2.getHostName(), n2_r2.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode3 = TestUtils
        .getMockNode(n3_r2.getHostName(), n3_r2.getRackName(), 123, 4 * GB);
    // 1 Containers on node 0 with allocationTag 'hbase-m'
    ContainerId hbase_m = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(appId1, 0), 0);
    tm.addContainer(n0_r1.getNodeID(), hbase_m, ImmutableSet.of("hbase-m"));

    // 'spark' placement on Node0 should now SUCCEED
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode0, pcm, tm));
    // FAIL on the rest of the nodes
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode1, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode2, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode3, pcm, tm));
  }

  @Test
  public void testRackAffinityAssignment()
      throws InvalidAllocationTagsQueryException {
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    // Register App1 with affinity constraint map
    pcm.registerApplication(appId1, constraintMap1);
    /**
     * Now place container:
     * Node0:123 (Rack1):
     *    container_app1_1 (hbase-rs)
     */
    RMNode n0_r1 = rmNodes.get(0);
    RMNode n1_r1 = rmNodes.get(1);
    RMNode n2_r2 = rmNodes.get(2);
    RMNode n3_r2 = rmNodes.get(3);
    // 1 Containers on Node0-Rack1 with allocationTag 'hbase-rs'
    ContainerId hbase_m = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(appId1, 0), 0);
    tm.addContainer(n0_r1.getNodeID(), hbase_m, ImmutableSet.of("hbase-rs"));

    FiCaSchedulerNode schedulerNode0 = TestUtils
        .getMockNode(n0_r1.getHostName(), n0_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode1 = TestUtils
        .getMockNode(n1_r1.getHostName(), n1_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode2 = TestUtils
        .getMockNode(n2_r2.getHostName(), n2_r2.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode3 = TestUtils
        .getMockNode(n3_r2.getHostName(), n3_r2.getRackName(), 123, 4 * GB);
    // 'zk' placement on Rack1 should now SUCCEED
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode0, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode1, pcm, tm));

    // FAIL on the rest of the RACKs
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode2, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode3, pcm, tm));
  }

  @Test
  public void testNodeAntiAffinityAssignment()
      throws InvalidAllocationTagsQueryException {
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    // Register App1 with anti-affinity constraint map
    pcm.registerApplication(appId1, constraintMap2);
    /**
     * place container:
     * Node0:123 (Rack1):
     *    container_app1_1 (hbase-m)
     */
    RMNode n0_r1 = rmNodes.get(0);
    RMNode n1_r1 = rmNodes.get(1);
    RMNode n2_r2 = rmNodes.get(2);
    RMNode n3_r2 = rmNodes.get(3);
    FiCaSchedulerNode schedulerNode0 = TestUtils
        .getMockNode(n0_r1.getHostName(), n0_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode1 = TestUtils
        .getMockNode(n1_r1.getHostName(), n1_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode2 = TestUtils
        .getMockNode(n2_r2.getHostName(), n2_r2.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode3 = TestUtils
        .getMockNode(n3_r2.getHostName(), n3_r2.getRackName(), 123, 4 * GB);
    // 1 Containers on node 0 with allocationTag 'hbase-m'
    ContainerId hbase_m = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(appId1, 0), 0);
    tm.addContainer(n0_r1.getNodeID(), hbase_m, ImmutableSet.of("hbase-m"));

    // 'spark' placement on Node0 should now FAIL
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode0, pcm, tm));
    // SUCCEED on the rest of the nodes
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode1, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode2, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode3, pcm, tm));
  }

  @Test
  public void testRackAntiAffinityAssignment()
      throws InvalidAllocationTagsQueryException {
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    // Register App1 with anti-affinity constraint map
    pcm.registerApplication(appId1, constraintMap2);
    /**
     * Place container:
     * Node0:123 (Rack1):
     *    container_app1_1 (hbase-rs)
     */
    RMNode n0_r1 = rmNodes.get(0);
    RMNode n1_r1 = rmNodes.get(1);
    RMNode n2_r2 = rmNodes.get(2);
    RMNode n3_r2 = rmNodes.get(3);
    // 1 Containers on Node0-Rack1 with allocationTag 'hbase-rs'
    ContainerId hbase_m = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(appId1, 0), 0);
    tm.addContainer(n0_r1.getNodeID(), hbase_m, ImmutableSet.of("hbase-rs"));

    FiCaSchedulerNode schedulerNode0 = TestUtils
        .getMockNode(n0_r1.getHostName(), n0_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode1 = TestUtils
        .getMockNode(n1_r1.getHostName(), n1_r1.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode2 = TestUtils
        .getMockNode(n2_r2.getHostName(), n2_r2.getRackName(), 123, 4 * GB);
    FiCaSchedulerNode schedulerNode3 = TestUtils
        .getMockNode(n3_r2.getHostName(), n3_r2.getRackName(), 123, 4 * GB);

    // 'zk' placement on Rack1 should FAIL
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode0, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode1, pcm, tm));

    // SUCCEED on the rest of the RACKs
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode2, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode3, pcm, tm));
  }
}