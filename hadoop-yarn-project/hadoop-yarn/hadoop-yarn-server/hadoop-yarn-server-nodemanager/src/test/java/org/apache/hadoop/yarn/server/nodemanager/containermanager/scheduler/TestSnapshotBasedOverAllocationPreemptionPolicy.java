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

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SnapshotBasedOverAllocationPreemptionPolicy}.
 */
public class TestSnapshotBasedOverAllocationPreemptionPolicy {
  // Both the CPU preemption threshold and the memory preemption threshold
  // are 75%
  private final static ResourceThresholds PREEMPTION_THRESHOLDS =
      ResourceThresholds.newInstance(0.75f, 0.75f);

  // The CPU utilization is allowed to go over the cpu preemption threshold
  // 2 times in a row before any container is preempted to reclaim cpu resources
  private final static int MAX_CPU_OVER_PREEMPTION_THRESHOLDS = 2;

  private final ContainersMonitor containersMonitor =
      mock(ContainersMonitor.class);

  @Before
  public void setUp() {
    // the node has an allocation of 2048 MB of memory
    when(containersMonitor.getPmemAllocatedForContainers()).
        thenReturn(2048 * 1024 * 1024L);
  }

  /**
   * The memory utilization goes above its preemption threshold,
   * 2048  * 0.75f = 1536 MB (the node has an allocation of 2048 MB memory).
   */
  @Test
  public void testMemoryOverPreemptionThreshold() {
    SnapshotBasedOverAllocationPreemptionPolicy preemptionPolicy =
        new SnapshotBasedOverAllocationPreemptionPolicy(PREEMPTION_THRESHOLDS,
            MAX_CPU_OVER_PREEMPTION_THRESHOLDS, containersMonitor);

    // the current memory utilization, 2000 MB is over the preemption
    // threshold, 2048 * 0.75, which is 1536 MB. The CPU utilization,
    // 0.5f is below the preemption threshold, 0.75f.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(2000, 0, 0.5f),
            Time.now()));

    // the amount of memory utilization over the preemption threshold, that is,
    // 2000 - (2048 * 0.75) = 464 MB of memory, shall be reclaimed.
    Assert.assertEquals(
        ResourceUtilization.newInstance(464, 0, 0f),
        preemptionPolicy.getResourcesToReclaim());
  }

  /**
   * The CPU utilization goes above its preemption threshold, 0.75f.
   */
  @Test
  public void testCpuOverPreemptionThreshold() {
    SnapshotBasedOverAllocationPreemptionPolicy preemptionPolicy =
        new SnapshotBasedOverAllocationPreemptionPolicy(PREEMPTION_THRESHOLDS,
            MAX_CPU_OVER_PREEMPTION_THRESHOLDS, containersMonitor);

    // the current CPU utilization, 1.0f, is over the preemption threshold,
    // 0.75f, for the first time. The memory utilization, 1000 MB is below
    // the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 1.0f),
            Time.now()));
    // no resources shall be reclaimed
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 0.5f, is below the preemption threshold,
    // 0.75f. In the meantime the memory utilization, 1000 MB is also below
    // the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 0.5f),
            Time.now()));
    // no resources shall be reclaimed
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 1.0f, is over the preemption threshold,
    // 0.75f. In the meantime the memory utilization, 1000 MB is below
    // the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 1.0f),
            Time.now()));
    // no resources shall be reclaimed because the cpu utilization is allowed
    // to go over the preemption threshold at most two times in a row. It is
    // just over the preemption threshold for the first time
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 1.0f, is again over the preemption
    // threshold, 0.75f. In the meantime the memory utilization, 1000 MB
    // is below the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 1.0f),
            Time.now()));
    // no resources shall be reclaimed because the cpu utilization is allowed
    // to go over the preemption threshold at most two times in a row. It is
    // just over the preemption threshold for the second time in a row
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 1.0f, is over the preemption threshold,
    // the third time in a row. In the meantime the memory utilization, 1000 MB
    // is below the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 1.0f),
            Time.now()));
    // the amount of cpu utilization over the preemption threshold, that is,
    // 1.0 - 0.75f = 0.25, shall be reclaimed.
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.25f),
        preemptionPolicy.getResourcesToReclaim());
  }

  /**
   * Both memory and CPU utilization go over their preemption thresholds
   * respectively.
   */
  @Test
  public void testMemoryCpuOverPreemptionThreshold() {
    SnapshotBasedOverAllocationPreemptionPolicy preemptionPolicy =
        new SnapshotBasedOverAllocationPreemptionPolicy(PREEMPTION_THRESHOLDS,
            MAX_CPU_OVER_PREEMPTION_THRESHOLDS, containersMonitor);

    // the current CPU utilization, 1.0f, is over the preemption threshold,
    // 0.75f, for the first time. The memory utilization, 1000 MB is below
    // the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 1.0f),
            Time.now()));
    // no resources shall be reclaimed because the cpu utilization is allowed
    // to go over the preemption threshold at most two times in a row. It is
    // just over the preemption threshold for the first time.
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 0.5f, is below the preemption threshold,
    // 0.75f. The memory utilization, 2000 MB, however, is above the memory
    // preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(2000, 0, 0.5f),
            Time.now()));
    // the amount of memory utilization over the preemption threshold, that is,
    // 2000 - (2048 * 0.75) = 464 MB of memory, shall be reclaimed.
    Assert.assertEquals(
        ResourceUtilization.newInstance(464, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 1.0f, is over the preemption threshold,
    // 0.75f, for the first time. The memory utilization, 1000 MB is below
    // the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 1.0f),
            Time.now()));
    // no resources shall be reclaimed because the cpu utilization is allowed
    // to go over the preemption threshold at most two times in a row. It is
    // just over the preemption threshold for the first time.
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 1.0f, is again over the preemption
    // threshold, 0.75f. In the meantime the memory utilization, 1000 MB
    // is still below the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 1.0f),
            Time.now()));
    // no resources shall be reclaimed because the cpu utilization is allowed
    // to go over the preemption threshold at most two times in a row. It is
    // just over the preemption threshold for the second time in a row.
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0.0f),
        preemptionPolicy.getResourcesToReclaim());

    // the current CPU utilization, 1.0f, is over the CPU preemption threshold,
    // 0.75f, the third time in a row. In the meantime, the memory utilization,
    // 2000 MB, is also over the memory preemption threshold,
    // 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(2000, 0, 1.0f),
            Time.now()));
    // the amount of memory utilization over the preemption threshold, that is,
    // 2000 - (2048 * 0.75) = 464 MB of memory, and the amount of cpu
    // utilization over the preemption threshold, that is, 1.0f - 0.75f = 0.25f,
    // shall be reclaimed.
    Assert.assertEquals(
        ResourceUtilization.newInstance(464, 0, 0.25f),
        preemptionPolicy.getResourcesToReclaim());
  }

  /**
   * Both memory and CPU utilization are under their preemption thresholds.
   */
  @Test
  public void testBothMemoryAndCpuUnderPreemptionThreshold() {
    SnapshotBasedOverAllocationPreemptionPolicy preemptionPolicy =
        new SnapshotBasedOverAllocationPreemptionPolicy(PREEMPTION_THRESHOLDS,
            MAX_CPU_OVER_PREEMPTION_THRESHOLDS, containersMonitor);

    // the current CPU utilization, 0.5f, is below the preemption threshold,
    // 0.75f. In the meantime the memory utilization, 1000 MB is also below
    // the memory preemption threshold, 2048 * 0.75 = 1536 MB.
    when(containersMonitor.getContainersUtilization(anyBoolean())).thenReturn(
        new ContainersMonitor.ContainersResourceUtilization(
            ResourceUtilization.newInstance(1000, 0, 0.5f),
            Time.now()));
    // no resources shall be reclaimed because both CPU and memory utilization
    // are under the preemption threshold
    Assert.assertEquals(
        ResourceUtilization.newInstance(0, 0, 0f),
        preemptionPolicy.getResourcesToReclaim());
  }
}
