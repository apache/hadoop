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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRegularContainerAllocator {

  private FiCaSchedulerApp application;
  private RMContext rmContext;
  private SchedulerRequestKey schedulerKey;
  private CandidateNodeSet<FiCaSchedulerNode> candidates;
  private ResourceLimits resourceLimits;
  private AppSchedulingInfo appSchedulingInfo;

  @BeforeEach
  public void setUp() {
    application = mock(FiCaSchedulerApp.class);
    rmContext = mock(RMContext.class);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());

    schedulerKey = mock(SchedulerRequestKey.class);
    candidates = mock(CandidateNodeSet.class);
    resourceLimits = mock(ResourceLimits.class);
    appSchedulingInfo = mock(AppSchedulingInfo.class);

    when(application.getAppSchedulingInfo()).thenReturn(appSchedulingInfo);
    when(application.getPendingAsk(schedulerKey, ResourceRequest.ANY))
        .thenReturn(new PendingAsk(Resource.newInstance(1024, 1), 1));
    when(application.getOutstandingAsksCount(schedulerKey)).thenReturn(1);

    AbstractLeafQueue leafQueue = mock(AbstractLeafQueue.class);
    when(leafQueue.isReservationsContinueLooking()).thenReturn(true);
    when(application.getCSLeafQueue()).thenReturn(leafQueue);
    when(application.getAppAttemptResourceUsage()).thenReturn(new ResourceUsage());
    when(resourceLimits.getHeadroom())
        .thenReturn(Resource.newInstance(100 * 1024, 100));
    when(candidates.getPartition()).thenReturn("");
  }

  @Test
  public void testReservationFallbackUsesReservationNode() throws Exception {
    FiCaSchedulerNode reservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode skippedNode = mock(FiCaSchedulerNode.class);
    when(reservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation", 1234));
    when(skippedNode.getNodeID())
        .thenReturn(NodeId.newInstance("skipped", 1234));

    mockPlacement(reservationNode, skippedNode);
    allowNodePrecheck(reservationNode);

    ContainerAllocation reservation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.RESERVED);
    TestAllocator allocator =
        new TestAllocator(application, rmContext, reservation);

    ContainerAllocation result = allocate(allocator);

    assertEquals(AllocationState.RESERVED, result.getAllocationState());
    assertSame(reservationNode, allocator.getLastDoAllocationNode());
  }

  @Test
  public void testLaterReservationReplacesEarlierReservation()
      throws Exception {
    FiCaSchedulerNode firstReservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode secondReservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode skippedNode = mock(FiCaSchedulerNode.class);
    when(firstReservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation1", 1234));
    when(secondReservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation2", 1234));
    when(skippedNode.getNodeID())
        .thenReturn(NodeId.newInstance("skipped", 1234));

    mockPlacement(firstReservationNode, secondReservationNode, skippedNode);
    allowNodePrecheck(firstReservationNode);
    allowNodePrecheck(secondReservationNode);

    ContainerAllocation firstReservation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.RESERVED);
    ContainerAllocation secondReservation = new ContainerAllocation(null,
        Resource.newInstance(2048, 1), AllocationState.RESERVED);
    TestAllocator allocator = new TestAllocator(application, rmContext,
        firstReservation, secondReservation);

    ContainerAllocation result = allocate(allocator);

    assertSame(secondReservation, result);
    assertSame(secondReservationNode, allocator.getLastDoAllocationNode());
  }

  @Test
  public void testFailedAllocationPreventsReservationFallback()
      throws Exception {
    FiCaSchedulerNode reservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode allocationNode = mock(FiCaSchedulerNode.class);
    when(reservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation", 1234));
    when(allocationNode.getNodeID())
        .thenReturn(NodeId.newInstance("allocation", 1234));

    mockPlacement(reservationNode, allocationNode);
    allowNodePrecheck(reservationNode);
    allowNodePrecheck(allocationNode);

    ContainerAllocation reservation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.RESERVED);
    ContainerAllocation allocation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.ALLOCATED);
    TestAllocator allocator = new TestAllocator(application, rmContext,
        reservation, allocation);
    allocator.setDoAllocationResult(ContainerAllocation.APP_SKIPPED);

    ContainerAllocation result = allocate(allocator);

    assertEquals(AllocationState.APP_SKIPPED, result.getAllocationState());
    assertSame(allocationNode, allocator.getLastDoAllocationNode());
  }

  private void mockPlacement(FiCaSchedulerNode... nodes) {
    AppPlacementAllocator<FiCaSchedulerNode> placement =
        mock(AppPlacementAllocator.class);
    when(appSchedulingInfo.<FiCaSchedulerNode>getAppPlacementAllocator(
        schedulerKey)).thenReturn(placement);
    when(placement.getPreferredNodeIterator(candidates))
        .thenReturn(Arrays.asList(nodes).iterator());
  }

  private void allowNodePrecheck(FiCaSchedulerNode node) {
    when(appSchedulingInfo.precheckNode(eq(schedulerKey), eq(node), any(),
        any())).thenReturn(true);
  }

  private ContainerAllocation allocate(RegularContainerAllocator allocator) {
    return allocator.allocate(
        Resource.newInstance(8192, 8), candidates,
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, resourceLimits,
        schedulerKey, null);
  }

  private static final class TestAllocator extends RegularContainerAllocator {
    private final Iterator<ContainerAllocation> results;
    private ContainerAllocation doAllocationResult;
    private FiCaSchedulerNode lastDoAllocationNode;

    private TestAllocator(FiCaSchedulerApp application, RMContext rmContext,
        ContainerAllocation... results) {
      super(application, new DefaultResourceCalculator(), rmContext, null);
      this.results = Arrays.asList(results).iterator();
    }

    private void setDoAllocationResult(ContainerAllocation result) {
      this.doAllocationResult = result;
    }

    private FiCaSchedulerNode getLastDoAllocationNode() {
      return lastDoAllocationNode;
    }

    @Override
    ContainerAllocation tryAllocateOnNode(Resource clusterResource,
        FiCaSchedulerNode node, SchedulingMode schedulingMode,
        ResourceLimits resourceLimits, SchedulerRequestKey schedulerKey,
        RMContainer reservedContainer) {
      return results.next();
    }

    @Override
    ContainerAllocation doAllocation(ContainerAllocation allocationResult,
        FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
        RMContainer reservedContainer) {
      lastDoAllocationNode = node;
      if (doAllocationResult != null) {
        return doAllocationResult;
      }
      allocationResult.updatedContainer = reservedContainer;
      return allocationResult;
    }
  }
}
