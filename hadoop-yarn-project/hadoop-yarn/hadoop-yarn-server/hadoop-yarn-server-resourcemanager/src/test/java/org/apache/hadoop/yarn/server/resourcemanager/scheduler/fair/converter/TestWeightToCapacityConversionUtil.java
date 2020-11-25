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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestWeightToCapacityConversionUtil {
  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  @Test
  public void testSingleWeightConversion() {
    List<FSQueue> queues = createFSQueues(1);
    Pair<Map<String, BigDecimal>, Boolean> conversion =
        WeightToCapacityConversionUtil.getCapacities(1, queues, ruleHandler);

    assertFalse("Capacity zerosum allowed", conversion.getRight());
    assertEquals("Capacity", new BigDecimal("100.000"),
        conversion.getLeft().get("root.a"));
  }

  @Test
  public void testNoChildQueueConversion() {
    List<FSQueue> queues = new ArrayList<>();
    Pair<Map<String, BigDecimal>, Boolean> conversion =
        WeightToCapacityConversionUtil.getCapacities(1, queues, ruleHandler);

    assertEquals("Converted items", 0, conversion.getLeft().size());
  }

  @Test
  public void testMultiWeightConversion() {
    List<FSQueue> queues = createFSQueues(1, 2, 3);

    Pair<Map<String, BigDecimal>, Boolean> conversion =
        WeightToCapacityConversionUtil.getCapacities(6, queues, ruleHandler);

    Map<String, BigDecimal> capacities = conversion.getLeft();

    assertEquals("Number of queues", 3, capacities.size());
    // this is no fixing - it's the result of BigDecimal rounding
    assertEquals("root.a capacity", new BigDecimal("16.667"),
        capacities.get("root.a"));
    assertEquals("root.b capacity", new BigDecimal("33.333"),
        capacities.get("root.b"));
    assertEquals("root.c capacity", new BigDecimal("50.000"),
        capacities.get("root.c"));
  }

  @Test
  public void testMultiWeightConversionWhenOfThemIsZero() {
    List<FSQueue> queues = createFSQueues(0, 1, 1);

    Pair<Map<String, BigDecimal>, Boolean> conversion =
        WeightToCapacityConversionUtil.getCapacities(2, queues, ruleHandler);

    Map<String, BigDecimal> capacities = conversion.getLeft();

    assertFalse("Capacity zerosum allowed", conversion.getRight());
    assertEquals("Number of queues", 3, capacities.size());
    assertEquals("root.a capacity", new BigDecimal("0.000"),
        capacities.get("root.a"));
    assertEquals("root.b capacity", new BigDecimal("50.000"),
        capacities.get("root.b"));
    assertEquals("root.c capacity", new BigDecimal("50.000"),
        capacities.get("root.c"));
  }

  @Test
  public void testMultiWeightConversionWhenAllOfThemAreZero() {
    List<FSQueue> queues = createFSQueues(0, 0, 0);

    Pair<Map<String, BigDecimal>, Boolean> conversion =
        WeightToCapacityConversionUtil.getCapacities(0, queues, ruleHandler);

    Map<String, BigDecimal> capacities = conversion.getLeft();

    assertEquals("Number of queues", 3, capacities.size());
    assertTrue("Capacity zerosum allowed", conversion.getRight());
    assertEquals("root.a capacity", new BigDecimal("0.000"),
        capacities.get("root.a"));
    assertEquals("root.b capacity", new BigDecimal("0.000"),
        capacities.get("root.b"));
    assertEquals("root.c capacity", new BigDecimal("0.000"),
        capacities.get("root.c"));
  }

  @Test
  public void testCapacityFixingWithThreeQueues() {
    List<FSQueue> queues = createFSQueues(1, 1, 1);

    Pair<Map<String, BigDecimal>, Boolean> conversion =
        WeightToCapacityConversionUtil.getCapacities(3, queues, ruleHandler);

    Map<String, BigDecimal> capacities = conversion.getLeft();
    assertEquals("Number of queues", 3, capacities.size());
    assertEquals("root.a capacity", new BigDecimal("33.334"),
        capacities.get("root.a"));
    assertEquals("root.b capacity", new BigDecimal("33.333"),
        capacities.get("root.b"));
    assertEquals("root.c capacity", new BigDecimal("33.333"),
        capacities.get("root.c"));
  }

  @Test
  public void testCapacityFixingWhenTotalCapacityIsGreaterThanHundred() {
    Map<String, BigDecimal> capacities = new HashMap<>();
    capacities.put("root.a", new BigDecimal("50.001"));
    capacities.put("root.b", new BigDecimal("25.500"));
    capacities.put("root.c", new BigDecimal("25.500"));

    testCapacityFixing(capacities, new BigDecimal("100.001"));
  }

  @Test
  public void testCapacityFixWhenTotalCapacityIsLessThanHundred() {
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
        WeightToCapacityConversionUtil.fixCapacities(capacities,
            total);

    assertFalse("Capacity zerosum allowed", needCapacityValidationRelax);
    assertEquals("root.a capacity", new BigDecimal("50.000"),
        capacities.get("root.a"));
    assertEquals("root.b capacity", new BigDecimal("25.500"),
        capacities.get("root.b"));
    assertEquals("root.c capacity", new BigDecimal("25.500"),
        capacities.get("root.c"));
  }

  private List<FSQueue> createFSQueues(int... weights){
    char current = 'a';

    List<FSQueue> queues = new ArrayList<>();

    for (int w : weights) {
      FSQueue queue = mock(FSQueue.class);
      when(queue.getWeight()).thenReturn((float)w);
      when(queue.getName()).thenReturn(
          "root." + new String(new char[] {current}));
      when(queue.getMinShare()).thenReturn(Resources.none());
      current++;
      queues.add(queue);
    }

    return queues;
  }
}
