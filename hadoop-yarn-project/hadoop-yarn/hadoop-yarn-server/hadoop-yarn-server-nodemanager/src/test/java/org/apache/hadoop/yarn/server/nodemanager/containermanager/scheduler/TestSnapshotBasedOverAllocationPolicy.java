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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SnapshotBasedOverAllocationPolicy}.
 */
public class TestSnapshotBasedOverAllocationPolicy {
  private final ContainersMonitor containersMonitor =
      mock(ContainersMonitor.class);
  private ResourceThresholds overAllocationThresholds =
      ResourceThresholds.newInstance(0.8f, 0.8f);

  private static final long MEMORY_CAPACITY_BYTE = 512 * 1024 * 1024 * 1024L;
  private static final int VCORE_CAPACITY = 10;

  @Before
  public void setUp() {
    // the node has an allocation of 512 GBs of memory and 10 vcores
    when(containersMonitor.getPmemAllocatedForContainers())
        .thenReturn(MEMORY_CAPACITY_BYTE);
    when(containersMonitor.getVCoresAllocatedForContainers())
        .thenReturn((long)VCORE_CAPACITY);
  }

  /**
   * Get the amount of memory in MBs given a memory utilization percentage.
   * @param memoryUtilization memory utilization in [0, 1.0]
   * @return the memory in MBs
   */
  private static int getMemoryMBs(double memoryUtilization) {
    return (int) (Math.round(memoryUtilization * MEMORY_CAPACITY_BYTE) >> 20);
  }

  @Test
  public void testNoVcoresAvailable() {
    SnapshotBasedOverAllocationPolicy overAllocationPolicy =
        new SnapshotBasedOverAllocationPolicy(
            overAllocationThresholds, containersMonitor);

    // the current cpu utilization is over the threshold, 0.8
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(0, 0, 0.9f),
            System.currentTimeMillis()));
    Resource available = overAllocationPolicy.getAvailableResources();
    Assert.assertEquals(
        "There should be no resources available for overallocation",
        Resources.none(), available);
  }

  @Test
  public void testNoMemoryAvailable() {
    SnapshotBasedOverAllocationPolicy overAllocationPolicy =
        new SnapshotBasedOverAllocationPolicy(
            overAllocationThresholds, containersMonitor);

    // the current memory utilization is over the threshold, 0.8
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(
                getMemoryMBs(0.9), 0, 0.0f),
            System.currentTimeMillis()));
    Resource available = overAllocationPolicy.getAvailableResources();
    Assert.assertEquals(
        "There should be no resources available for overallocation",
        Resources.none(), available);
  }

  @Test
  public void testNoMemoryVcoreAvailable() {
    SnapshotBasedOverAllocationPolicy overAllocationPolicy =
        new SnapshotBasedOverAllocationPolicy(
            overAllocationThresholds, containersMonitor);

    // the current memory utilization is over the threshold, 0.8
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(
                getMemoryMBs(0.9), 0, 0.9f),
            System.currentTimeMillis()));
    Resource available = overAllocationPolicy.getAvailableResources();
    Assert.assertEquals(
        "There should be no resources available for overallocation",
        Resources.none(), available);
  }

  @Test
  public void testResourcesAvailable() {
    SnapshotBasedOverAllocationPolicy overAllocationPolicy =
        new SnapshotBasedOverAllocationPolicy(
            overAllocationThresholds, containersMonitor);

    // the current memory and cpu utilization are both below the threshold
    int memoryUtilizationMBs = getMemoryMBs(0.6);
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(memoryUtilizationMBs, 0, 0.6f),
            System.currentTimeMillis()));
    Resource available = overAllocationPolicy.getAvailableResources();
    Assert.assertEquals("Unexpected resources available for overallocation",
        Resource.newInstance(
            getMemoryMBs(0.8) - memoryUtilizationMBs,
             Math.round(0.2f * VCORE_CAPACITY)),
        available);
  }

  @Test
  public void testResourcesAvailableWithZeroUtilization() {
    SnapshotBasedOverAllocationPolicy overAllocationPolicy =
        new SnapshotBasedOverAllocationPolicy(
            overAllocationThresholds, containersMonitor);

    // the current memory and cpu utilization are zeros
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(0, 0, 0.0f),
            System.currentTimeMillis()));
    Resource available = overAllocationPolicy.getAvailableResources();
    Assert.assertEquals("Unexpected resources available for overallocation",
        Resource.newInstance(
            getMemoryMBs(0.8), Math.round(0.8f * VCORE_CAPACITY)),
        available);
  }
}
