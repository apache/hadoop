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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRegularContainerAllocator {

  @Test
  public void testReservationFallbackUsesReservationNode() throws Exception {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());

    SchedulerRequestKey schedulerKey = mock(SchedulerRequestKey.class);
    CandidateNodeSet<FiCaSchedulerNode> candidates = mock(CandidateNodeSet.class);
    ResourceLimits resourceLimits = mock(ResourceLimits.class);
    RMContainer reservedContainer = mock(RMContainer.class);

    FiCaSchedulerNode reservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode skippedNode = mock(FiCaSchedulerNode.class);
    when(reservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation", 1234));
    when(skippedNode.getNodeID())
        .thenReturn(NodeId.newInstance("skipped", 1234));

    mockPlacement(application, schedulerKey, candidates,
        reservationNode, skippedNode);
    when(application.getOutstandingAsksCount(schedulerKey)).thenReturn(1);

    ContainerAllocation reservation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.RESERVED);
    TestAllocator allocator = new TestAllocator(application, rmContext,
        reservation, ContainerAllocation.APP_SKIPPED);

    ContainerAllocation result = invokeAllocate(allocator, candidates,
        resourceLimits, schedulerKey, reservedContainer);

    assertEquals(AllocationState.RESERVED, result.getAllocationState());
    assertSame(reservationNode, allocator.getLastDoAllocationNode());
  }

  @Test
  public void testLaterReservationReplacesEarlierReservation()
      throws Exception {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());

    SchedulerRequestKey schedulerKey = mock(SchedulerRequestKey.class);
    CandidateNodeSet<FiCaSchedulerNode> candidates = mock(CandidateNodeSet.class);
    ResourceLimits resourceLimits = mock(ResourceLimits.class);
    RMContainer reservedContainer = mock(RMContainer.class);

    FiCaSchedulerNode firstReservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode secondReservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode skippedNode = mock(FiCaSchedulerNode.class);
    when(firstReservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation1", 1234));
    when(secondReservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation2", 1234));
    when(skippedNode.getNodeID())
        .thenReturn(NodeId.newInstance("skipped", 1234));

    mockPlacement(application, schedulerKey, candidates,
        firstReservationNode, secondReservationNode, skippedNode);
    when(application.getOutstandingAsksCount(schedulerKey)).thenReturn(1);

    ContainerAllocation firstReservation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.RESERVED);
    ContainerAllocation secondReservation = new ContainerAllocation(null,
        Resource.newInstance(2048, 1), AllocationState.RESERVED);
    TestAllocator allocator = new TestAllocator(application, rmContext,
        firstReservation, secondReservation, ContainerAllocation.APP_SKIPPED);

    ContainerAllocation result = invokeAllocate(allocator, candidates,
        resourceLimits, schedulerKey, reservedContainer);

    assertSame(secondReservation, result);
    assertSame(secondReservationNode, allocator.getLastDoAllocationNode());
  }

  @Test
  public void testFailedAllocationPreventsReservationFallback()
      throws Exception {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());

    SchedulerRequestKey schedulerKey = mock(SchedulerRequestKey.class);
    CandidateNodeSet<FiCaSchedulerNode> candidates = mock(CandidateNodeSet.class);
    ResourceLimits resourceLimits = mock(ResourceLimits.class);
    RMContainer reservedContainer = mock(RMContainer.class);

    FiCaSchedulerNode reservationNode = mock(FiCaSchedulerNode.class);
    FiCaSchedulerNode allocationNode = mock(FiCaSchedulerNode.class);
    when(reservationNode.getNodeID())
        .thenReturn(NodeId.newInstance("reservation", 1234));
    when(allocationNode.getNodeID())
        .thenReturn(NodeId.newInstance("allocation", 1234));

    mockPlacement(application, schedulerKey, candidates,
        reservationNode, allocationNode);
    when(application.getOutstandingAsksCount(schedulerKey)).thenReturn(1);

    ContainerAllocation reservation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.RESERVED);
    ContainerAllocation allocation = new ContainerAllocation(null,
        Resource.newInstance(1024, 1), AllocationState.ALLOCATED);
    TestAllocator allocator = new TestAllocator(application, rmContext,
        reservation, allocation);
    allocator.setDoAllocationResult(ContainerAllocation.APP_SKIPPED);

    ContainerAllocation result = invokeAllocate(allocator, candidates,
        resourceLimits, schedulerKey, reservedContainer);

    assertEquals(AllocationState.APP_SKIPPED, result.getAllocationState());
    assertSame(allocationNode, allocator.getLastDoAllocationNode());
  }

  private void mockPlacement(FiCaSchedulerApp application,
      SchedulerRequestKey schedulerKey,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      FiCaSchedulerNode... nodes) {
    AppPlacementAllocator<FiCaSchedulerNode> placement =
        mock(AppPlacementAllocator.class);
    AppSchedulingInfo appSchedulingInfo = mock(AppSchedulingInfo.class);
    when(application.getAppSchedulingInfo()).thenReturn(appSchedulingInfo);
    when(appSchedulingInfo.<FiCaSchedulerNode>getAppPlacementAllocator(
        schedulerKey)).thenReturn(placement);
    when(placement.getPreferredNodeIterator(candidates))
        .thenReturn(Arrays.asList(nodes).iterator());
  }

  private ContainerAllocation invokeAllocate(
      RegularContainerAllocator allocator,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      ResourceLimits resourceLimits,
      SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer) throws Exception {
    Method allocate = RegularContainerAllocator.class.getDeclaredMethod(
        "allocate", Resource.class, CandidateNodeSet.class,
        SchedulingMode.class, ResourceLimits.class, SchedulerRequestKey.class,
        RMContainer.class);
    allocate.setAccessible(true);
    return (ContainerAllocation) allocate.invoke(allocator,
        Resource.newInstance(8192, 8), candidates,
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, resourceLimits,
        schedulerKey, reservedContainer);
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
