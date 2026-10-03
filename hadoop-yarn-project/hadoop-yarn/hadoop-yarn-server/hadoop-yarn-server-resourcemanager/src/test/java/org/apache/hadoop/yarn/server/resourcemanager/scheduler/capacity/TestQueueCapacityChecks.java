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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityChecks.LabelCapacity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityChecks.QueueCapacityInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestQueueCapacityChecks {
  private static final ResourceCalculator DRC = new DominantResourceCalculator();
  private static final ResourceCalculator DEFAULT_RC = new DefaultResourceCalculator();
  private static final Resource CLUSTER = Resource.newInstance(100 * 1024, 100);
  private static final Set<String> NO_LABEL = Collections.singleton("");

  private static QueueCapacityInput queue(String path, float capacity, float weight,
      boolean absolute) {
    Map<String, LabelCapacity> byLabel = new HashMap<>();
    byLabel.put("", new LabelCapacity(capacity, weight, absolute));
    return new QueueCapacityInput(path, byLabel);
  }

  private static QueueCapacityInput pct(String path, float capacity) {
    return queue(path, capacity, -1f, false);
  }

  private static QueueCapacityInput weight(String path, float weight) {
    return queue(path, 0f, weight, false);
  }

  private static QueueCapacityInput abs(String path) {
    return queue(path, 0.3f, -1f, true);
  }

  @Test
  public void testRootCapacity() {
    assertNull(QueueCapacityChecks.checkRootCapacity("root", true, 100f));
    assertNull(QueueCapacityChecks.checkRootCapacity("a", false, 50f));
    assertEquals("Illegal capacity of 50.0 for queue root. Must be 100.0",
        QueueCapacityChecks.checkRootCapacity("root", true, 50f));
  }

  @Test
  public void testMaxResourceWithinParent() {
    Resource parentMax = Resource.newInstance(10240, 10);
    assertNull(QueueCapacityChecks.checkMaxResourceWithinParent("root.a",
        Resource.newInstance(10240, 10), parentMax, CLUSTER, DRC));
    // Unset parent max never fails
    assertNull(QueueCapacityChecks.checkMaxResourceWithinParent("root.a",
        Resource.newInstance(20480, 20), Resources.none(), CLUSTER, DRC));
    assertEquals("Max resource configuration <memory:20480, vCores:20> is greater than"
            + " parents max value:<memory:10240, vCores:10> in queue:root.a",
        QueueCapacityChecks.checkMaxResourceWithinParent("root.a",
            Resource.newInstance(20480, 20), parentMax, CLUSTER, DRC));
    // The default calculator only compares memory
    assertNull(QueueCapacityChecks.checkMaxResourceWithinParent("root.a",
        Resource.newInstance(10240, 20), parentMax, CLUSTER, DEFAULT_RC));
  }

  @Test
  public void testInheritMaxResourceFromParent() {
    Resource min = Resource.newInstance(1024, 1);
    Resource max = Resource.newInstance(4096, 4);
    Resource parentMax = Resource.newInstance(8192, 8);
    assertSame(max, QueueCapacityChecks.inheritMaxResourceFromParent(min, max, parentMax));
    assertEquals(parentMax, QueueCapacityChecks.inheritMaxResourceFromParent(min,
        Resources.none(), parentMax));
    assertEquals(Resources.none(), QueueCapacityChecks.inheritMaxResourceFromParent(
        Resources.none(), Resources.none(), parentMax));
    assertEquals(Resources.none(), QueueCapacityChecks.inheritMaxResourceFromParent(
        min, Resources.none(), Resources.none()));
  }

  @Test
  public void testMinResourceWithinMax() {
    assertNull(QueueCapacityChecks.checkMinResourceWithinMax("root.a",
        Resource.newInstance(1024, 1), Resource.newInstance(2048, 2), CLUSTER, DRC));
    assertNull(QueueCapacityChecks.checkMinResourceWithinMax("root.a",
        Resource.newInstance(4096, 4), Resources.none(), CLUSTER, DRC));
    assertEquals("Min resource configuration <memory:4096, vCores:4> is greater than its"
            + " max value:<memory:2048, vCores:2> in queue:root.a",
        QueueCapacityChecks.checkMinResourceWithinMax("root.a",
            Resource.newInstance(4096, 4), Resource.newInstance(2048, 2), CLUSTER, DRC));
  }

  @Test
  public void testCapacityConfigTypeConsistent() {
    assertNull(QueueCapacityChecks.checkCapacityConfigTypeConsistent("root.a", false, true,
        CapacityConfigType.PERCENTAGE, CapacityConfigType.PERCENTAGE));
    assertNull(QueueCapacityChecks.checkCapacityConfigTypeConsistent("root", true, true,
        CapacityConfigType.PERCENTAGE, CapacityConfigType.ABSOLUTE_RESOURCE));
    assertNull(QueueCapacityChecks.checkCapacityConfigTypeConsistent("root.a", false, false,
        CapacityConfigType.PERCENTAGE, CapacityConfigType.ABSOLUTE_RESOURCE));
    assertEquals("Queue 'root.a' should use either percentage based capacity configuration"
            + " or absolute resource.",
        QueueCapacityChecks.checkCapacityConfigTypeConsistent("root.a", false, true,
            CapacityConfigType.PERCENTAGE, CapacityConfigType.ABSOLUTE_RESOURCE));
  }

  @Test
  public void testCapacityTypesNotMixed() {
    assertNull(QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
        Arrays.asList(pct("root.a", 0.4f), pct("root.b", 0.6f))));
    assertNull(QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
        Collections.emptyList()));
    // 0% does not count as percentage
    assertNull(QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
        Arrays.asList(pct("root.a", 0f), weight("root.b", 1f))));
    // An absolute queue clears the percentage flag of the queues before it
    assertNull(QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
        Arrays.asList(pct("root.a", 0.5f), abs("root.b"))));
    assertEquals("Parent queue 'root' have children queue used mixed of  weight mode,"
            + " percentage and absolute mode, it is not allowed, please double check,"
            + " details:{Queue=root.a, label= uses absolute mode}. "
            + "{Queue=root.b, label= uses percentage mode}. ",
        QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
            Arrays.asList(abs("root.a"), pct("root.b", 0.5f))));
    assertEquals("Parent queue 'root' have children queue used mixed of  weight mode,"
            + " percentage and absolute mode, it is not allowed, please double check,"
            + " details:{Queue=root.a, label= uses percentage mode}. "
            + "{Queue=root.b, label= uses weight mode}. "
            + "{Queue=root.b, label= uses percentage mode}. ",
        QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
            Arrays.asList(pct("root.a", 0.5f), weight("root.b", 1f))));
    // Skipped when the first queue is root
    assertNull(QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
        Arrays.asList(pct("root", 1f), weight("root.b", 1f))));
  }

  @Test
  public void testCapacityTypesNotMixedOnlyChecksGivenLabels() {
    Map<String, LabelCapacity> byLabel = new HashMap<>();
    byLabel.put("", new LabelCapacity(0.5f, -1f, false));
    byLabel.put("x", new LabelCapacity(0f, 1f, false));
    QueueCapacityInput mixed = new QueueCapacityInput("root.a", byLabel);
    assertNull(QueueCapacityChecks.checkCapacityTypesNotMixed("root", NO_LABEL,
        Collections.singletonList(mixed)));
    Set<String> labels = new LinkedHashSet<>(Arrays.asList("", "x"));
    assertEquals("Parent queue 'root.a' have children queue used mixed of  weight mode,"
            + " percentage and absolute mode, it is not allowed, please double check,"
            + " details:{Queue=root.a, label= uses percentage mode}. "
            + "{Queue=root.a, label=x uses weight mode}. "
            + "{Queue=root.a, label=x uses percentage mode}. ",
        QueueCapacityChecks.checkCapacityTypesNotMixed("root.a", labels,
            Collections.singletonList(mixed)));
    // A label missing from the input reads as unset
    assertEquals(LabelCapacity.UNSET, mixed.get("y"));
  }

  @Test
  public void testCapacityConfigurationType() {
    assertEquals(QueueCapacityType.WEIGHT, QueueCapacityChecks.getCapacityConfigurationType(
        NO_LABEL, Collections.emptyList()));
    assertEquals(QueueCapacityType.WEIGHT, QueueCapacityChecks.getCapacityConfigurationType(
        NO_LABEL, Arrays.asList(pct("root.a", 0f), weight("root.b", 1f))));
    assertEquals(QueueCapacityType.ABSOLUTE_RESOURCE,
        QueueCapacityChecks.getCapacityConfigurationType(NO_LABEL,
            Arrays.asList(pct("root.a", 0.5f), abs("root.b"))));
    assertEquals(QueueCapacityType.PERCENT, QueueCapacityChecks.getCapacityConfigurationType(
        NO_LABEL, Arrays.asList(pct("root.a", 0.5f), pct("root.b", 0.5f))));
  }

  @Test
  public void testAbsoluteResourceUsedByParentAndChildren() {
    assertNull(QueueCapacityChecks.checkAbsoluteResourceUsedByParentAndChildren("root.a",
        QueueCapacityType.ABSOLUTE_RESOURCE, QueueCapacityType.ABSOLUTE_RESOURCE));
    assertNull(QueueCapacityChecks.checkAbsoluteResourceUsedByParentAndChildren("root.a",
        QueueCapacityType.PERCENT, QueueCapacityType.WEIGHT));
    assertNull(QueueCapacityChecks.checkAbsoluteResourceUsedByParentAndChildren("root",
        QueueCapacityType.PERCENT, QueueCapacityType.ABSOLUTE_RESOURCE));
    String expected = "Parent=root.a: When absolute minResource is used, we must make sure"
        + " both parent and child all use absolute minResource";
    assertEquals(expected, QueueCapacityChecks.checkAbsoluteResourceUsedByParentAndChildren(
        "root.a", QueueCapacityType.PERCENT, QueueCapacityType.ABSOLUTE_RESOURCE));
    assertEquals(expected, QueueCapacityChecks.checkAbsoluteResourceUsedByParentAndChildren(
        "root.a", QueueCapacityType.ABSOLUTE_RESOURCE, QueueCapacityType.WEIGHT));
  }

  @Test
  public void testChildrenMinResourceWithinParent() {
    List<Resource> children = Arrays.asList(Resource.newInstance(2048, 2),
        Resource.newInstance(3072, 3));
    assertNull(QueueCapacityChecks.checkChildrenMinResourceWithinParent("a",
        Resource.newInstance(5120, 5), children, CLUSTER, DRC));
    assertNull(QueueCapacityChecks.checkChildrenMinResourceWithinParent("a",
        Resources.none(), children, CLUSTER, DRC));
    assertEquals("Parent Queues capacity: <memory:4096, vCores:4> is less than to its"
            + " children:<memory:5120, vCores:5> for queue:a",
        QueueCapacityChecks.checkChildrenMinResourceWithinParent("a",
            Resource.newInstance(4096, 4), children, CLUSTER, DRC));
  }

  @Test
  public void testChildrenCapacitySum() {
    List<QueueCapacityInput> full = Arrays.asList(pct("root.a.x", 0.4f),
        pct("root.a.y", 0.6f));
    List<QueueCapacityInput> zero = Arrays.asList(pct("root.a.x", 0f),
        pct("root.a.y", 0f));
    List<QueueCapacityInput> partial = Arrays.asList(pct("root.a.x", 0.4f),
        pct("root.a.y", 0.5f));

    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.PERCENT, 0.5f, false, full));
    // Within precision
    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.PERCENT, 0.5f, false,
        Arrays.asList(pct("root.a.x", 0.4f), pct("root.a.y", 0.5996f))));

    // C12
    assertEquals("Illegal capacity sum of 0.9 for children of queue a for label=."
            + " It should be either 0 or 1.0",
        QueueCapacityChecks.checkChildrenCapacitySum("a", "",
            QueueCapacityType.PERCENT, 0.5f, true, partial));

    // C13
    assertEquals("Illegal capacity sum of 0.0 for children of queue a for label=x."
            + " It is set to 0, but parent percent != 0, and doesn't allow children"
            + " capacity to set to 0",
        QueueCapacityChecks.checkChildrenCapacitySum("a", "x",
            QueueCapacityType.PERCENT, 0.5f, false, Arrays.asList(
                labeled("root.a.x", "x", 0f), labeled("root.a.y", "x", 0f))));
    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.PERCENT, 0.5f, true, zero));
    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.PERCENT, 0f, false, zero));
    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.PERCENT, 0.0004f, false, zero));
    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.WEIGHT, 0.5f, false, zero));

    // C14
    assertEquals("Illegal capacity sum of 1.0 for children of queue a for label=."
            + " queue=a has zero capacity, but childqueues have positive capacities",
        QueueCapacityChecks.checkChildrenCapacitySum("a", "",
            QueueCapacityType.PERCENT, 0f, false, full));
    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.PERCENT, 0f, true, full));
    assertNull(QueueCapacityChecks.checkChildrenCapacitySum("a", "",
        QueueCapacityType.WEIGHT, 0f, false, full));
  }

  private static QueueCapacityInput labeled(String path, String label, float capacity) {
    Map<String, LabelCapacity> byLabel = new HashMap<>();
    byLabel.put(label, new LabelCapacity(capacity, -1f, false));
    return new QueueCapacityInput(path, byLabel);
  }

  @Test
  public void testCapacityConfigTypeOf() {
    assertEquals(CapacityConfigType.ABSOLUTE_RESOURCE,
        QueueCapacityChecks.capacityConfigTypeOf(true));
    assertEquals(CapacityConfigType.PERCENTAGE,
        QueueCapacityChecks.capacityConfigTypeOf(false));
    assertEquals(CapacityConfigType.ABSOLUTE_RESOURCE,
        QueueCapacityChecks.capacityConfigTypeOf(
            QueueCapacityVector.of(1024, ResourceUnitCapacityType.ABSOLUTE)));
    assertEquals(CapacityConfigType.PERCENTAGE,
        QueueCapacityChecks.capacityConfigTypeOf(
            QueueCapacityVector.of(50, ResourceUnitCapacityType.PERCENTAGE)));
    assertEquals(CapacityConfigType.PERCENTAGE,
        QueueCapacityChecks.capacityConfigTypeOf(
            QueueCapacityVector.of(2, ResourceUnitCapacityType.WEIGHT)));
    assertEquals(CapacityConfigType.PERCENTAGE,
        QueueCapacityChecks.capacityConfigTypeOf(mixedVector()));
  }

  @Test
  public void testUniformWeightOf() {
    assertEquals(Float.valueOf(5f), QueueCapacityChecks.uniformWeightOf(
        QueueCapacityVector.of(5, ResourceUnitCapacityType.WEIGHT)));
    assertNull(QueueCapacityChecks.uniformWeightOf(
        QueueCapacityVector.of(50, ResourceUnitCapacityType.PERCENTAGE)));
    assertNull(QueueCapacityChecks.uniformWeightOf(mixedVector()));
    QueueCapacityVector differentWeights = QueueCapacityVector.newInstance();
    differentWeights.setResource("memory", 2, ResourceUnitCapacityType.WEIGHT);
    differentWeights.setResource("vcores", 3, ResourceUnitCapacityType.WEIGHT);
    assertNull(QueueCapacityChecks.uniformWeightOf(differentWeights));
  }

  private static QueueCapacityVector mixedVector() {
    QueueCapacityVector vector = QueueCapacityVector.newInstance();
    vector.setResource("memory", 1024, ResourceUnitCapacityType.ABSOLUTE);
    vector.setResource("vcores", 50, ResourceUnitCapacityType.PERCENTAGE);
    return vector;
  }
}
