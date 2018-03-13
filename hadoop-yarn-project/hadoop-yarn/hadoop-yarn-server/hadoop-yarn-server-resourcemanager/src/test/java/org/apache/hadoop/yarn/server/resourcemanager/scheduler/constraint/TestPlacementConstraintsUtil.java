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
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTagWithNamespace;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.RACK;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.maxCardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.and;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.or;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.yarn.api.records.AllocationTagNamespace;
import org.apache.hadoop.yarn.api.records.NodeId;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
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
  private PlacementConstraint c1, c2, c3, c4, c5, c6, c7;
  private Set<String> sourceTag1, sourceTag2;
  private Map<Set<String>, PlacementConstraint> constraintMap1,
      constraintMap2, constraintMap3, constraintMap4;
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
    c5 = PlacementConstraints
        .build(and(targetNotIn(NODE, allocationTag("hbase-m")),
            maxCardinality(NODE, 3, "spark")));
    c6 = PlacementConstraints
        .build(or(targetIn(NODE, allocationTag("hbase-m")),
            targetIn(NODE, allocationTag("hbase-rs"))));
    c7 = PlacementConstraints
        .build(or(targetIn(NODE, allocationTag("hbase-m")),
            and(targetIn(NODE, allocationTag("hbase-rs")),
                targetIn(NODE, allocationTag("spark")))));

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
    constraintMap3 = Stream
        .of(new AbstractMap.SimpleEntry<>(sourceTag1, c5))
        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey,
            AbstractMap.SimpleEntry::getValue));
    constraintMap4 = Stream
        .of(new AbstractMap.SimpleEntry<>(sourceTag1, c6),
            new AbstractMap.SimpleEntry<>(sourceTag2, c7))
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

  private ContainerId newContainerId(ApplicationId appId) {
    return ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(appId, 0), 0);
  }

  private SchedulerNode newSchedulerNode(String hostname, String rackName,
      NodeId nodeId) {
    SchedulerNode node = mock(SchedulerNode.class);
    when(node.getNodeName()).thenReturn(hostname);
    when(node.getRackName()).thenReturn(rackName);
    when(node.getNodeID()).thenReturn(nodeId);
    return node;
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
      SchedulerNode schedulerNode =newSchedulerNode(currentNode.getHostName(),
          currentNode.getRackName(), currentNode.getNodeID());

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
    SchedulerNode schedulerNode0 =newSchedulerNode(n0_r1.getHostName(),
        n0_r1.getRackName(), n0_r1.getNodeID());
    SchedulerNode schedulerNode1 =newSchedulerNode(n1_r1.getHostName(),
        n1_r1.getRackName(), n1_r1.getNodeID());
    SchedulerNode schedulerNode2 =newSchedulerNode(n2_r2.getHostName(),
        n2_r2.getRackName(), n2_r2.getNodeID());
    SchedulerNode schedulerNode3 =newSchedulerNode(n3_r2.getHostName(),
        n3_r2.getRackName(), n3_r2.getNodeID());

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

    SchedulerNode schedulerNode0 =newSchedulerNode(n0_r1.getHostName(),
        n0_r1.getRackName(), n0_r1.getNodeID());
    SchedulerNode schedulerNode1 =newSchedulerNode(n1_r1.getHostName(),
        n1_r1.getRackName(), n1_r1.getNodeID());
    SchedulerNode schedulerNode2 =newSchedulerNode(n2_r2.getHostName(),
        n2_r2.getRackName(), n2_r2.getNodeID());
    SchedulerNode schedulerNode3 =newSchedulerNode(n3_r2.getHostName(),
        n3_r2.getRackName(), n3_r2.getNodeID());

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

    SchedulerNode schedulerNode0 =newSchedulerNode(n0_r1.getHostName(),
        n0_r1.getRackName(), n0_r1.getNodeID());
    SchedulerNode schedulerNode1 =newSchedulerNode(n1_r1.getHostName(),
        n1_r1.getRackName(), n1_r1.getNodeID());
    SchedulerNode schedulerNode2 =newSchedulerNode(n2_r2.getHostName(),
        n2_r2.getRackName(), n2_r2.getNodeID());
    SchedulerNode schedulerNode3 =newSchedulerNode(n3_r2.getHostName(),
        n3_r2.getRackName(), n3_r2.getNodeID());

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

    SchedulerNode schedulerNode0 =newSchedulerNode(n0_r1.getHostName(),
        n0_r1.getRackName(), n0_r1.getNodeID());
    SchedulerNode schedulerNode1 =newSchedulerNode(n1_r1.getHostName(),
        n1_r1.getRackName(), n1_r1.getNodeID());
    SchedulerNode schedulerNode2 =newSchedulerNode(n2_r2.getHostName(),
        n2_r2.getRackName(), n2_r2.getNodeID());
    SchedulerNode schedulerNode3 =newSchedulerNode(n3_r2.getHostName(),
        n3_r2.getRackName(), n3_r2.getNodeID());

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

  @Test
  public void testORConstraintAssignment()
      throws InvalidAllocationTagsQueryException {
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    // Register App1 with anti-affinity constraint map.
    pcm.registerApplication(appId1, constraintMap4);
    RMNode n0r1 = rmNodes.get(0);
    RMNode n1r1 = rmNodes.get(1);
    RMNode n2r2 = rmNodes.get(2);
    RMNode n3r2 = rmNodes.get(3);

    /**
     * Place container:
     *  n0: hbase-m(1)
     *  n1: ""
     *  n2: hbase-rs(1)
     *  n3: ""
     */
    tm.addContainer(n0r1.getNodeID(),
        newContainerId(appId1), ImmutableSet.of("hbase-m"));
    tm.addContainer(n2r2.getNodeID(),
        newContainerId(appId1), ImmutableSet.of("hbase-rs"));
    Assert.assertEquals(1L, tm.getAllocationTagsWithCount(n0r1.getNodeID())
        .get("hbase-m").longValue());
    Assert.assertEquals(1L, tm.getAllocationTagsWithCount(n2r2.getNodeID())
        .get("hbase-rs").longValue());

    SchedulerNode schedulerNode0 =newSchedulerNode(n0r1.getHostName(),
        n0r1.getRackName(), n0r1.getNodeID());
    SchedulerNode schedulerNode1 =newSchedulerNode(n1r1.getHostName(),
        n1r1.getRackName(), n1r1.getNodeID());
    SchedulerNode schedulerNode2 =newSchedulerNode(n2r2.getHostName(),
        n2r2.getRackName(), n2r2.getNodeID());
    SchedulerNode schedulerNode3 =newSchedulerNode(n3r2.getHostName(),
        n3r2.getRackName(), n3r2.getNodeID());

    // n0 and n2 should be qualified for allocation as
    // they either have hbase-m or hbase-rs tag
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode0, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode1, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode2, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode3, pcm, tm));

    /**
     * Place container:
     *  n0: hbase-m(1)
     *  n1: ""
     *  n2: hbase-rs(1)
     *  n3: hbase-rs(1)
     */
    tm.addContainer(n3r2.getNodeID(),
        newContainerId(appId1), ImmutableSet.of("hbase-rs"));
    // n3 is qualified now because it is allocated with hbase-rs tag
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode3, pcm, tm));

    /**
     * Place container:
     *  n0: hbase-m(1)
     *  n1: ""
     *  n2: hbase-rs(1), spark(1)
     *  n3: hbase-rs(1)
     */
    // Place
    tm.addContainer(n2r2.getNodeID(),
        newContainerId(appId1), ImmutableSet.of("spark"));
    // According to constraint, "zk" is allowed to be placed on a node
    // has "hbase-m" tag OR a node has both "hbase-rs" and "spark" tags.
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode0, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode1, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode2, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag2), schedulerNode3, pcm, tm));
  }

  @Test
  public void testANDConstraintAssignment()
      throws InvalidAllocationTagsQueryException {
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    // Register App1 with anti-affinity constraint map.
    pcm.registerApplication(appId1, constraintMap3);
    RMNode n0r1 = rmNodes.get(0);
    RMNode n1r1 = rmNodes.get(1);
    RMNode n2r2 = rmNodes.get(2);
    RMNode n3r2 = rmNodes.get(3);

    /**
     * Place container:
     *  n0: hbase-m(1)
     *  n1: ""
     *  n2: hbase-m(1)
     *  n3: ""
     */
    tm.addContainer(n0r1.getNodeID(),
        newContainerId(appId1), ImmutableSet.of("hbase-m"));
    tm.addContainer(n2r2.getNodeID(),
        newContainerId(appId1), ImmutableSet.of("hbase-m"));
    Assert.assertEquals(1L, tm.getAllocationTagsWithCount(n0r1.getNodeID())
        .get("hbase-m").longValue());
    Assert.assertEquals(1L, tm.getAllocationTagsWithCount(n2r2.getNodeID())
        .get("hbase-m").longValue());

    SchedulerNode schedulerNode0 =newSchedulerNode(n0r1.getHostName(),
        n0r1.getRackName(), n0r1.getNodeID());
    SchedulerNode schedulerNode1 =newSchedulerNode(n1r1.getHostName(),
        n1r1.getRackName(), n1r1.getNodeID());
    SchedulerNode schedulerNode2 =newSchedulerNode(n2r2.getHostName(),
        n2r2.getRackName(), n2r2.getNodeID());
    SchedulerNode schedulerNode3 =newSchedulerNode(n3r2.getHostName(),
        n3r2.getRackName(), n3r2.getNodeID());

    // Anti-affinity with hbase-m so it should not be able to be placed
    // onto n0 and n2 as they already have hbase-m allocated.
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode0, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode1, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode2, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode3, pcm, tm));

    /**
     * Place container:
     *  n0: hbase-m(1)
     *  n1: spark(3)
     *  n2: hbase-m(1)
     *  n3: ""
     */
    for (int i=0; i<4; i++) {
      tm.addContainer(n1r1.getNodeID(),
          newContainerId(appId1), ImmutableSet.of("spark"));
    }
    Assert.assertEquals(4L, tm.getAllocationTagsWithCount(n1r1.getNodeID())
        .get("spark").longValue());

    // Violate cardinality constraint
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode0, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode1, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode2, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(appId1,
        createSchedulingRequest(sourceTag1), schedulerNode3, pcm, tm));
  }

  @Test
  public void testInterAppConstraintsByAppID()
      throws InvalidAllocationTagsQueryException {
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    rmContext.setAllocationTagsManager(tm);
    rmContext.setPlacementConstraintManager(pcm);

    long ts = System.currentTimeMillis();
    ApplicationId application1 = BuilderUtils.newApplicationId(ts, 123);

    // Register App1 with anti-affinity constraint map.
    RMNode n0r1 = rmNodes.get(0);
    RMNode n1r1 = rmNodes.get(1);
    RMNode n2r2 = rmNodes.get(2);
    RMNode n3r2 = rmNodes.get(3);

    /**
     * Place container:
     *  n0: app1/hbase-m(1)
     *  n1: ""
     *  n2: app1/hbase-m(1)
     *  n3: ""
     */
    tm.addContainer(n0r1.getNodeID(),
        newContainerId(application1), ImmutableSet.of("hbase-m"));
    tm.addContainer(n2r2.getNodeID(),
        newContainerId(application1), ImmutableSet.of("hbase-m"));
    Assert.assertEquals(1L, tm.getAllocationTagsWithCount(n0r1.getNodeID())
        .get("hbase-m").longValue());
    Assert.assertEquals(1L, tm.getAllocationTagsWithCount(n2r2.getNodeID())
        .get("hbase-m").longValue());

    SchedulerNode schedulerNode0 =newSchedulerNode(n0r1.getHostName(),
        n0r1.getRackName(), n0r1.getNodeID());
    SchedulerNode schedulerNode1 =newSchedulerNode(n1r1.getHostName(),
        n1r1.getRackName(), n1r1.getNodeID());
    SchedulerNode schedulerNode2 =newSchedulerNode(n2r2.getHostName(),
        n2r2.getRackName(), n2r2.getNodeID());
    SchedulerNode schedulerNode3 =newSchedulerNode(n3r2.getHostName(),
        n3r2.getRackName(), n3r2.getNodeID());

    AllocationTagNamespace namespace =
        new AllocationTagNamespace.AppID(application1);
    Map<Set<String>, PlacementConstraint> constraintMap = new HashMap<>();
    PlacementConstraint constraint2 = PlacementConstraints
        .targetNotIn(NODE, allocationTagWithNamespace(namespace.toString(),
            "hbase-m"))
        .build();
    Set<String> srcTags2 = new HashSet<>();
    srcTags2.add("app2");
    constraintMap.put(srcTags2, constraint2);

    ts = System.currentTimeMillis();
    ApplicationId application2 = BuilderUtils.newApplicationId(ts, 124);
    pcm.registerApplication(application2, constraintMap);

    // Anti-affinity with app1/hbase-m so it should not be able to be placed
    // onto n0 and n2 as they already have hbase-m allocated.
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(
        application2, createSchedulingRequest(srcTags2),
        schedulerNode0, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(
        application2, createSchedulingRequest(srcTags2),
        schedulerNode1, pcm, tm));
    Assert.assertFalse(PlacementConstraintsUtil.canSatisfyConstraints(
        application2, createSchedulingRequest(srcTags2),
        schedulerNode2, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil.canSatisfyConstraints(
        application2, createSchedulingRequest(srcTags2),
        schedulerNode3, pcm, tm));

    // Intra-app constraint
    // Test with default and empty namespace
    AllocationTagNamespace self = new AllocationTagNamespace.Self();
    PlacementConstraint constraint3 = PlacementConstraints
        .targetNotIn(NODE, allocationTagWithNamespace(self.toString(),
            "hbase-m"))
        .build();
    Set<String> srcTags3 = new HashSet<>();
    srcTags3.add("app3");
    constraintMap.put(srcTags3, constraint3);

    ts = System.currentTimeMillis();
    ApplicationId application3 = BuilderUtils.newApplicationId(ts, 124);
    pcm.registerApplication(application3, constraintMap);

    /**
     * Place container:
     *  n0: app1/hbase-m(1), app3/hbase-m
     *  n1: ""
     *  n2: app1/hbase-m(1)
     *  n3: ""
     */
    tm.addContainer(n0r1.getNodeID(),
        newContainerId(application3), ImmutableSet.of("hbase-m"));

    // Anti-affinity to self/hbase-m
    Assert.assertFalse(PlacementConstraintsUtil
        .canSatisfyConstraints(application3, createSchedulingRequest(srcTags3),
            schedulerNode0, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil
        .canSatisfyConstraints(application3, createSchedulingRequest(srcTags3),
            schedulerNode1, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil
        .canSatisfyConstraints(application3, createSchedulingRequest(srcTags3),
            schedulerNode2, pcm, tm));
    Assert.assertTrue(PlacementConstraintsUtil
        .canSatisfyConstraints(application3, createSchedulingRequest(srcTags3),
            schedulerNode3, pcm, tm));

    pcm.unregisterApplication(application3);
  }

  @Test
  public void testInvalidAllocationTagNamespace() {
    AllocationTagsManager tm = new AllocationTagsManager(rmContext);
    PlacementConstraintManagerService pcm =
        new MemoryPlacementConstraintManager();
    rmContext.setAllocationTagsManager(tm);
    rmContext.setPlacementConstraintManager(pcm);

    long ts = System.currentTimeMillis();
    ApplicationId application1 = BuilderUtils.newApplicationId(ts, 123);
    RMNode n0r1 = rmNodes.get(0);
    SchedulerNode schedulerNode0 = newSchedulerNode(n0r1.getHostName(),
        n0r1.getRackName(), n0r1.getNodeID());

    PlacementConstraint constraint1 = PlacementConstraints
        .targetNotIn(NODE, allocationTagWithNamespace("unknown_namespace",
            "hbase-m"))
        .build();
    Set<String> srcTags1 = new HashSet<>();
    srcTags1.add("app1");

    try {
      PlacementConstraintsUtil.canSatisfyConstraints(application1,
          createSchedulingRequest(srcTags1, constraint1), schedulerNode0,
          pcm, tm);
      Assert.fail("This should fail because we gave an invalid namespace");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidAllocationTagsQueryException);
      Assert.assertTrue(e.getMessage()
          .contains("Invalid namespace prefix: unknown_namespace"));
    }
  }
}