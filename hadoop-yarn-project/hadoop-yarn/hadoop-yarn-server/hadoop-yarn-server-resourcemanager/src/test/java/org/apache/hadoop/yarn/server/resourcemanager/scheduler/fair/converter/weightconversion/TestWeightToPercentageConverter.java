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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestWeightToPercentageConverter
    extends WeightConverterTestBase {
  private WeightToPercentConverter converter;
  private Configuration config;

  @BeforeEach
  public void setup() {
    converter = new WeightToPercentConverter();
    config = new Configuration(false);
  }

  @Test
  void testSingleWeightConversion() {
    FSQueue root = createFSQueues(1);
    converter.convertWeightsForChildQueues(root, config);

    assertFalse(config.getBoolean(PREFIX + "root.allow-zero-capacity-sum",
            false),
        "Capacity zerosum allowed");
    assertEquals("100.000", config.get(PREFIX + "root.a.capacity"),
        "root.a capacity");
  }

  @Test
  void testNoChildQueueConversion() {
    FSQueue root = createFSQueues();
    converter.convertWeightsForChildQueues(root, config);

    assertEquals(0, config.getPropsWithPrefix(PREFIX).size(),
        "Converted items");
  }

  @Test
  void testMultiWeightConversion() {
    FSQueue root = createFSQueues(1, 2, 3);

    converter.convertWeightsForChildQueues(root, config);

    assertEquals(3, config.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
    // this is no fixing - it's the result of BigDecimal rounding
    assertEquals("16.667", config.get(PREFIX + "root.a.capacity"),
        "root.a capacity");
    assertEquals("33.333", config.get(PREFIX + "root.b.capacity"),
        "root.b capacity");
    assertEquals("50.000", config.get(PREFIX + "root.c.capacity"),
        "root.c capacity");
  }

  @Test
  void testMultiWeightConversionWhenOfThemIsZero() {
    FSQueue root = createFSQueues(0, 1, 1);

    converter.convertWeightsForChildQueues(root, config);

    assertFalse(config.getBoolean(PREFIX + "root.allow-zero-capacity-sum",
            false),
        "Capacity zerosum allowed");
    assertEquals(3, config.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
    assertEquals("0.000", config.get(PREFIX + "root.a.capacity"),
        "root.a capacity");
    assertEquals("50.000", config.get(PREFIX + "root.b.capacity"),
        "root.b capacity");
    assertEquals("50.000", config.get(PREFIX + "root.c.capacity"),
        "root.c capacity");
  }

  @Test
  void testMultiWeightConversionWhenAllOfThemAreZero() {
    FSQueue root = createFSQueues(0, 0, 0);

    converter.convertWeightsForChildQueues(root, config);

    assertEquals(4, config.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
    assertTrue(config.getBoolean(PREFIX + "root.allow-zero-capacity-sum",
            false),
        "Capacity zerosum allowed");
    assertEquals("0.000", config.get(PREFIX + "root.a.capacity"),
        "root.a capacity");
    assertEquals("0.000", config.get(PREFIX + "root.b.capacity"),
        "root.b capacity");
    assertEquals("0.000", config.get(PREFIX + "root.c.capacity"),
        "root.c capacity");
  }

  @Test
  void testCapacityFixingWithThreeQueues() {
    FSQueue root = createFSQueues(1, 1, 1);

    converter.convertWeightsForChildQueues(root, config);

    assertEquals(3, config.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
    assertEquals("33.334", config.get(PREFIX + "root.a.capacity"),
        "root.a capacity");
    assertEquals("33.333", config.get(PREFIX + "root.b.capacity"),
        "root.b capacity");
    assertEquals("33.333", config.get(PREFIX + "root.c.capacity"),
        "root.c capacity");
  }

  @Test
  void testCapacityFixingWhenTotalCapacityIsGreaterThanHundred() {
    Map<String, BigDecimal> capacities = new HashMap<>();
    capacities.put("root.a", new BigDecimal("50.001"));
    capacities.put("root.b", new BigDecimal("25.500"));
    capacities.put("root.c", new BigDecimal("25.500"));

    testCapacityFixing(capacities, new BigDecimal("100.001"));
  }

  @Test
  void testCapacityFixWhenTotalCapacityIsLessThanHundred() {
    Map<String, BigDecimal> capacities = new HashMap<>();
    capacities.put("root.a", new BigDecimal("49.999"));
    capacities.put("root.b", new BigDecimal("25.500"));
    capacities.put("root.c", new BigDecimal("25.500"));

    testCapacityFixing(capacities, new BigDecimal("99.999"));
  }

  private void testCapacityFixing(Map<String, BigDecimal> capacities,
      BigDecimal total) {
    // Note: we call fixCapacities() directly because it makes
    // testing easier
    boolean needCapacityValidationRelax =
        converter.fixCapacities(capacities,
            total);

    assertFalse(needCapacityValidationRelax, "Capacity zerosum allowed");
    assertEquals(new BigDecimal("50.000"), capacities.get("root.a"),
        "root.a capacity");
    assertEquals(new BigDecimal("25.500"), capacities.get("root.b"),
        "root.b capacity");
    assertEquals(new BigDecimal("25.500"), capacities.get("root.c"),
        "root.c capacity");
  }
}
