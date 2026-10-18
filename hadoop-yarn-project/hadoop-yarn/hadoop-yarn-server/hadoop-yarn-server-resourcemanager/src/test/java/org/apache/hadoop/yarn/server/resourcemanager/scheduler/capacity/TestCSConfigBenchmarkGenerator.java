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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NodeAttributeTestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sanity checks for {@link CSConfigBenchmarkGenerator}: the generated configs
 * must start a real CapacityScheduler and survive the legacy validation and
 * reinitialize paths the benchmark measures.
 */
public class TestCSConfigBenchmarkGenerator {

  private MockRM rm;

  @AfterEach
  public void tearDown() {
    if (rm != null) {
      rm.stop();
      rm = null;
    }
    QueueMetrics.clearQueueMetrics();
  }

  @Test
  public void testMixedModeConfigStartsAndValidates() throws Exception {
    CSConfigBenchmarkGenerator.GeneratedConfig gen =
        CSConfigBenchmarkGenerator.generate(60);
    assertTrue(gen.getQueueCount() >= 60, "should generate at least 60 queues");
    assertNotNull(gen.getMutationLeafA());
    assertNotNull(gen.getMutationLeafB());
    assertTrue(gen.getLabels().contains("blue"));

    YarnConfiguration conf = NodeAttributeTestUtils.getRandomDirConf(gen.getConf());
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    // root is included by the queue manager, generator count excludes it
    assertEquals(gen.getQueueCount() + 1,
        cs.getCapacitySchedulerQueueManager().getQueues().size(),
        "live queue count should match the generated count");
    for (String leaf : gen.getLeafPaths()) {
      assertNotNull(cs.getQueue(leaf), "missing generated leaf " + leaf);
    }

    Configuration mutated =
        CSConfigBenchmarkGenerator.createMutatedCopy(conf, gen);
    String capacityKey = CapacitySchedulerConfiguration.PREFIX
        + gen.getMutationLeafA() + ".capacity";
    assertNotEquals(conf.get(capacityKey), mutated.get(capacityKey),
        "mutated copy should differ from the base config");

    assertTrue(CapacitySchedulerConfigValidator.validateCSConfiguration(
        conf, mutated, rm.getRMContext()), "mutated config should validate");
    cs.reinitialize(mutated, rm.getRMContext());
    assertEquals(gen.getQueueCount() + 1,
        cs.getCapacitySchedulerQueueManager().getQueues().size());
  }

  @Test
  public void testAqcV2ParentsCanCreateQueues() throws Exception {
    // 60 leaves p1 with a single sub-parent, 120 gives p1 all five.
    for (int size : new int[]{60, 120}) {
      CSConfigBenchmarkGenerator.GeneratedConfig gen =
          CSConfigBenchmarkGenerator.generate(size);
      YarnConfiguration conf = NodeAttributeTestUtils.getRandomDirConf(gen.getConf());
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);
      rm.start();

      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      for (String parent : gen.getAqcV2Parents()) {
        QueuePath dynamic = new QueuePath(parent + ".dyn");
        cs.getCapacitySchedulerQueueManager().createQueue(dynamic);
        assertNotNull(cs.getQueue(dynamic.getFullPath()),
            "dynamic queue not created under " + parent);
      }
      if (size == 120) {
        assertFalse(gen.getAqcV2Parents().isEmpty(),
            "a full weight subtree should carry an AQC v2 template");
      }
      rm.stop();
      rm = null;
      QueueMetrics.clearQueueMetrics();
    }
  }

  @Test
  public void testNonPositiveQueueCountRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> CSConfigBenchmarkGenerator.generate(0));
  }

  @Test
  public void testAbsoluteModeConfigStarts() throws Exception {
    CSConfigBenchmarkGenerator.GeneratedConfig gen =
        CSConfigBenchmarkGenerator.generate(30,
            CSConfigBenchmarkGenerator.CapacityMode.ALL_ABSOLUTE);
    assertTrue(gen.getQueueCount() >= 30);

    YarnConfiguration conf = NodeAttributeTestUtils.getRandomDirConf(gen.getConf());
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    assertEquals(gen.getQueueCount() + 1,
        cs.getCapacitySchedulerQueueManager().getQueues().size());
  }
}
