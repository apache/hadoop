/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestWeightToWeightConverter extends WeightConverterTestBase {
  private WeightToWeightConverter converter;
  private Configuration config;

  @BeforeEach
  public void setup() {
    converter = new WeightToWeightConverter();
    config = new Configuration(false);
  }

  @Test
  void testNoChildQueueConversion() {
    FSQueue root = createFSQueues();
    converter.convertWeightsForChildQueues(root, config);

    assertEquals("1.0w", config.get(PREFIX + "root.capacity"),
        "root weight");
    assertEquals(2, config.getPropsWithPrefix(PREFIX).size(),
        "Converted items");
  }

  @Test
  void testSingleWeightConversion() {
    FSQueue root = createFSQueues(1);
    converter.convertWeightsForChildQueues(root, config);

    assertEquals("1.0w", config.get(PREFIX + "root.capacity"),
        "root weight");
    assertEquals("1.0w", config.get(PREFIX + "root.a.capacity"),
        "root.a weight");
    assertEquals(3, config.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
  }

  @Test
  void testMultiWeightConversion() {
    FSQueue root = createFSQueues(1, 2, 3);

    converter.convertWeightsForChildQueues(root, config);

    assertEquals(5, config.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
    assertEquals("1.0w", config.get(PREFIX + "root.capacity"),
        "root weight");
    assertEquals("1.0w", config.get(PREFIX + "root.a.capacity"),
        "root.a weight");
    assertEquals("2.0w", config.get(PREFIX + "root.b.capacity"),
        "root.b weight");
    assertEquals("3.0w", config.get(PREFIX + "root.c.capacity"),
        "root.c weight");
  }

  @Test
  void testAutoCreateV2FlagOnParent() {
    FSQueue root = createFSQueues(1);
    converter.convertWeightsForChildQueues(root, config);

    assertTrue(config.getBoolean(PREFIX + "root.auto-queue-creation-v2.enabled",
            false),
        "root autocreate v2 enabled");
  }

  @Test
  void testAutoCreateV2FlagOnParentWithoutChildren() {
    FSQueue root = createParent(new ArrayList<>());
    converter.convertWeightsForChildQueues(root, config);

    assertEquals(2, config.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
    assertTrue(config.getBoolean(PREFIX + "root.auto-queue-creation-v2.enabled",
            false),
        "root autocreate v2 enabled");
  }
}
