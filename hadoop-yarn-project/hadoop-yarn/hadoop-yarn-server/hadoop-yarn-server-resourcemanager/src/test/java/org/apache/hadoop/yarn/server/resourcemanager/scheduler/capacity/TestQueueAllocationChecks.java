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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueAllocationChecks.AllocationRange;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueAllocationChecks.LegacyMaximumAllocationInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueAllocationChecks.MaximumAllocationInput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestQueueAllocationChecks {
  private static final int UNDEFINED =
      (int) CapacitySchedulerConfiguration.UNDEFINED;
  private static final String MEMORY_MESSAGE =
      "Invalid resource scheduler memory allocation configuration,"
          + " yarn.scheduler.minimum-allocation-mb=%d,"
          + " yarn.scheduler.maximum-allocation-mb=%d,"
          + " min and max should be greater than 0,"
          + " max should be no smaller than min.";
  private static final String VCORES_MESSAGE =
      "Invalid resource scheduler vcores allocation configuration,"
          + " yarn.scheduler.minimum-allocation-vcores=%d,"
          + " yarn.scheduler.maximum-allocation-vcores=%d,"
          + " min and max should be greater than 0,"
          + " max should be no smaller than min.";

  @Test
  public void testMemoryAllocation() {
    assertNull(QueueAllocationChecks.checkMemoryAllocation(
        new AllocationRange(1024, 8192)));
    assertNull(QueueAllocationChecks.checkMemoryAllocation(
        new AllocationRange(1024, 1024)));
    assertEquals(String.format(MEMORY_MESSAGE, 0, 8192),
        QueueAllocationChecks.checkMemoryAllocation(
            new AllocationRange(0, 8192)));
    assertEquals(String.format(MEMORY_MESSAGE, 2048, 1024),
        QueueAllocationChecks.checkMemoryAllocation(
            new AllocationRange(2048, 1024)));
  }

  @Test
  public void testVcoresAllocation() {
    assertNull(QueueAllocationChecks.checkVcoresAllocation(
        new AllocationRange(1, 4)));
    assertEquals(String.format(VCORES_MESSAGE, -1, 4),
        QueueAllocationChecks.checkVcoresAllocation(
            new AllocationRange(-1, 4)));
    assertEquals(String.format(VCORES_MESSAGE, 8, 4),
        QueueAllocationChecks.checkVcoresAllocation(
            new AllocationRange(8, 4)));
  }

  @Test
  public void testValidatorThrowsYarnRuntimeException() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 4096);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 2048);
    YarnRuntimeException e = assertThrows(YarnRuntimeException.class,
        () -> CapacitySchedulerConfigValidator.validateMemoryAllocation(conf));
    assertEquals(String.format(MEMORY_MESSAGE, 4096, 2048), e.getMessage());

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 0);
    e = assertThrows(YarnRuntimeException.class,
        () -> CapacitySchedulerConfigValidator.validateVCores(conf));
    assertEquals(String.format(VCORES_MESSAGE, 0, 4), e.getMessage());
  }

  @Test
  public void testLegacyQueueMaximumAllocation() {
    Resource cluster = Resource.newInstance(8192, 4);
    assertNull(QueueAllocationChecks.checkLegacyQueueMaximumAllocation(
        new LegacyMaximumAllocationInput("root.a", UNDEFINED, UNDEFINED,
            cluster, Resource.newInstance(8192, 4))));
    assertNull(QueueAllocationChecks.checkLegacyQueueMaximumAllocation(
        new LegacyMaximumAllocationInput("root.a", 8192, 4, cluster,
            Resource.newInstance(8192, 4))));
    assertEquals("Queue maximum allocation cannot be larger than the cluster"
            + " setting for queue root.a max allocation per queue:"
            + " <memory:16384, vCores:2> cluster setting:"
            + " <memory:8192, vCores:4>",
        QueueAllocationChecks.checkLegacyQueueMaximumAllocation(
            new LegacyMaximumAllocationInput("root.a", 16384, UNDEFINED,
                cluster, Resource.newInstance(16384, 2))));
    assertEquals("Queue maximum allocation cannot be larger than the cluster"
            + " setting for queue root.b max allocation per queue:"
            + " <memory:4096, vCores:5> cluster setting:"
            + " <memory:8192, vCores:4>",
        QueueAllocationChecks.checkLegacyQueueMaximumAllocation(
            new LegacyMaximumAllocationInput("root.b", UNDEFINED, 5,
                cluster, Resource.newInstance(4096, 5))));
  }

  @Test
  public void testQueueMaximumAllocation() {
    Resource cluster = Resource.newInstance(8192, 4);
    assertNull(QueueAllocationChecks.checkQueueMaximumAllocation(
        new MaximumAllocationInput("root.a", Resource.newInstance(8192, 4),
            cluster)));
    assertEquals("Queue maximum allocation cannot be larger than the cluster"
            + " setting for queue root.a max allocation per queue:"
            + " <memory:4096, vCores:8> cluster setting:"
            + " <memory:8192, vCores:4>",
        QueueAllocationChecks.checkQueueMaximumAllocation(
            new MaximumAllocationInput("root.a",
                Resource.newInstance(4096, 8), cluster)));
  }

  @Test
  public void testQueueAllocationSettingsThrowsIllegalArgumentException() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 8192);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
    QueuePath path = new QueuePath("root");
    conf.setQueueMaximumAllocation(path, "memory-mb=4096,vcores=8");
    QueueAllocationSettings settings =
        new QueueAllocationSettings(Resource.newInstance(1024, 1));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> settings.setupMaximumAllocation(conf, path, null));
    assertEquals("Queue maximum allocation cannot be larger than the cluster"
        + " setting for queue root max allocation per queue:"
        + " <memory:4096, vCores:8> cluster setting:"
        + " <memory:8192, vCores:4>", e.getMessage());

    CapacitySchedulerConfiguration legacy = new CapacitySchedulerConfiguration();
    legacy.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 8192);
    legacy.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
    legacy.setLong(QueuePrefixes.getQueuePrefix(path) + "maximum-allocation-mb",
        16384);
    e = assertThrows(IllegalArgumentException.class,
        () -> settings.setupMaximumAllocation(legacy, path, null));
    assertEquals("Queue maximum allocation cannot be larger than the cluster"
        + " setting for queue root max allocation per queue:"
        + " <memory:16384, vCores:4> cluster setting:"
        + " <memory:8192, vCores:4>", e.getMessage());
  }

  @Test
  public void testMaximumAllocationNotDecreased() {
    Resource current = Resource.newInstance(4096, 4);
    assertNull(QueueAllocationChecks.checkMaximumAllocationNotDecreased(
        "root.a", current, Resource.newInstance(4096, 4)));
    assertNull(QueueAllocationChecks.checkMaximumAllocationNotDecreased(
        "root.a", current, Resource.newInstance(8192, 8)));
    assertEquals("Trying to reinitialize root.a the maximum allocation size"
            + " can not be decreased! Current setting: <memory:4096, vCores:4>,"
            + " trying to set it to: <memory:8192, vCores:2>",
        QueueAllocationChecks.checkMaximumAllocationNotDecreased("root.a",
            current, Resource.newInstance(8192, 2)));
  }
}
