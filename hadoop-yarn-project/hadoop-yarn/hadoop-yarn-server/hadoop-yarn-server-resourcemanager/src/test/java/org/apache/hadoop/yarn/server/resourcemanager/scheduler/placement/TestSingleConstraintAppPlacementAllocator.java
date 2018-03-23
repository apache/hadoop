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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResoureRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.MemoryPlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.LongBinaryOperator;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test behaviors of single constraint app placement allocator.
 */
public class TestSingleConstraintAppPlacementAllocator {
  private AppSchedulingInfo appSchedulingInfo;
  private AllocationTagsManager spyAllocationTagsManager;
  private RMContext rmContext;
  private SchedulerRequestKey schedulerRequestKey;
  private SingleConstraintAppPlacementAllocator allocator;

  @Before
  public void setup() throws Exception {
    // stub app scheduling info.
    appSchedulingInfo = mock(AppSchedulingInfo.class);
    when(appSchedulingInfo.getApplicationId()).thenReturn(
        TestUtils.getMockApplicationId(1));
    when(appSchedulingInfo.getApplicationAttemptId()).thenReturn(
        TestUtils.getMockApplicationAttemptId(1, 1));

    // stub RMContext
    rmContext = TestUtils.getMockRMContext();

    // Create allocation tags manager
    AllocationTagsManager allocationTagsManager = new AllocationTagsManager(
        rmContext);
    PlacementConstraintManager placementConstraintManager =
        new MemoryPlacementConstraintManager();
    spyAllocationTagsManager = spy(allocationTagsManager);
    schedulerRequestKey = new SchedulerRequestKey(Priority.newInstance(1), 2L,
        TestUtils.getMockContainerId(1, 1));
    rmContext.setAllocationTagsManager(spyAllocationTagsManager);
    rmContext.setPlacementConstraintManager(placementConstraintManager);

    // Create allocator
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
  }

  private void assertValidSchedulingRequest(
      SchedulingRequest schedulingRequest) {
    // Create allocator to avoid fields polluted by previous runs
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
  }

  private void assertInvalidSchedulingRequest(
      SchedulingRequest schedulingRequest, boolean recreateAllocator) {
    try {
      // Create allocator
      if (recreateAllocator) {
        allocator = new SingleConstraintAppPlacementAllocator();
        allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
      }
      allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
    } catch (SchedulerInvalidResoureRequestException e) {
      // Expected
      return;
    }
    Assert.fail(
        "Expect failure for schedulingRequest=" + schedulingRequest.toString());
  }

  @Test
  public void testSchedulingRequestValidation() {
    // Valid
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    Assert.assertEquals(ImmutableSet.of("mapper", "reducer"),
        allocator.getTargetAllocationTags());
    Assert.assertEquals("", allocator.getTargetNodePartition());

    // Valid (with partition)
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition("x"))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    Assert.assertEquals(ImmutableSet.of("mapper", "reducer"),
        allocator.getTargetAllocationTags());
    Assert.assertEquals("x", allocator.getTargetNodePartition());

    // Valid (without specifying node partition)
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer")).build())
        .resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    Assert.assertEquals(ImmutableSet.of("mapper", "reducer"),
        allocator.getTargetAllocationTags());
    Assert.assertEquals("", allocator.getTargetNodePartition());

    // Valid (with application Id target)
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer")).build())
        .resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    // Allocation tags should not include application Id
    Assert.assertEquals(ImmutableSet.of("mapper", "reducer"),
        allocator.getTargetAllocationTags());
    Assert.assertEquals("", allocator.getTargetNodePartition());

    // Invalid (without sizing)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer")).build())
        .build(), true);

    // Invalid (without target tags)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE).build())
        .build(), true);

    // Invalid (with multiple allocation tags expression specified)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper"),
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build(), true);

    // Invalid (with multiple node partition target expression specified)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper"),
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp(""),
                PlacementConstraints.PlacementTargets.nodePartition("x"))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build(), true);

    // Invalid (not anti-affinity cardinality)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetCardinality(PlacementConstraints.NODE, 1, 2,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build(), true);

    // Invalid (not anti-affinity cardinality)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetCardinality(PlacementConstraints.NODE, 0, 2,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build(), true);

    // Invalid (not NODE scope)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.RACK,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build(), true);

    // Invalid (not GUARANTEED)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build(), true);
  }

  @Test
  public void testSchedulingRequestUpdate() {
    SchedulingRequest schedulingRequest =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(PlacementConstraints
                .targetNotIn(PlacementConstraints.NODE,
                    PlacementConstraints.PlacementTargets
                        .allocationTagToIntraApp("mapper", "reducer"),
                    PlacementConstraints.PlacementTargets.nodePartition(""))
                .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);

    // Update allocator with exactly same scheduling request, should succeeded.
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);

    // Update allocator with scheduling request different at #allocations,
    // should succeeded.
    schedulingRequest.getResourceSizing().setNumAllocations(10);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);

    // Update allocator with scheduling request different at resource,
    // should failed.
    schedulingRequest.getResourceSizing().setResources(
        Resource.newInstance(2048, 1));
    assertInvalidSchedulingRequest(schedulingRequest, false);

    // Update allocator with a different placement target (allocator tag),
    // should failed
    schedulingRequest = SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetCardinality(PlacementConstraints.NODE, 0, 1,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build();
    assertInvalidSchedulingRequest(schedulingRequest, false);

    // Update allocator with recover == true
    int existingNumAllocations =
        allocator.getSchedulingRequest().getResourceSizing()
            .getNumAllocations();
    schedulingRequest = SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, true);
    Assert.assertEquals(existingNumAllocations + 1,
        allocator.getSchedulingRequest().getResourceSizing()
            .getNumAllocations());
  }

  @Test
  public void testFunctionality() throws InvalidAllocationTagsQueryException {
    SchedulingRequest schedulingRequest =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(PlacementConstraints
                .targetNotIn(PlacementConstraints.NODE,
                    PlacementConstraints.PlacementTargets
                        .allocationTagToIntraApp("mapper", "reducer"),
                    PlacementConstraints.PlacementTargets.nodePartition(""))
                .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
    allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNode("host1", "/rack1", 123, 1024));
    verify(spyAllocationTagsManager, Mockito.times(1)).getNodeCardinalityByOp(
        eq(NodeId.fromString("host1:123")), eq(TestUtils.getMockApplicationId(1)),
        eq(ImmutableSet.of("mapper", "reducer")),
        any(LongBinaryOperator.class));

    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    // Valid (with partition)
    schedulingRequest = SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTagToIntraApp("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition("x"))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
    allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNode("host1", "/rack1", 123, 1024));
    verify(spyAllocationTagsManager, Mockito.atLeast(1)).getNodeCardinalityByOp(
        eq(NodeId.fromString("host1:123")),
        eq(TestUtils.getMockApplicationId(1)), eq(ImmutableSet
            .of("mapper", "reducer")), any(LongBinaryOperator.class));

    SchedulerNode node1 = mock(SchedulerNode.class);
    when(node1.getPartition()).thenReturn("x");
    when(node1.getNodeID()).thenReturn(NodeId.fromString("host1:123"));

    Assert.assertTrue(allocator
        .precheckNode(node1, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));

    SchedulerNode node2 = mock(SchedulerNode.class);
    when(node1.getPartition()).thenReturn("");
    when(node1.getNodeID()).thenReturn(NodeId.fromString("host2:123"));
    Assert.assertFalse(allocator
        .precheckNode(node2, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));
  }
}
