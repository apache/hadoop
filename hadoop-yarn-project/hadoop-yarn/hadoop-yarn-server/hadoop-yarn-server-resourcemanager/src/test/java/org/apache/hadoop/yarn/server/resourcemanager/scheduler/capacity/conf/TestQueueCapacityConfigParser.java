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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestQueueMetricsForCustomResources.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

public class TestQueueCapacityConfigParser {

  private static final String ALL_RESOURCE_TEMPLATE = "[memory-mb=%s, vcores=%s, yarn.io/gpu=%s]";
  private static final String MEMORY_VCORE_TEMPLATE = "[memory-mb=%s, vcores=%s]";

  private static final String MEMORY_ABSOLUTE = "12Gi";
  private static final float VCORE_ABSOLUTE = 6;
  private static final float GPU_ABSOLUTE = 10;

  private static final float PERCENTAGE_VALUE = 50f;
  private static final float MEMORY_MIXED = 1024;
  private static final float WEIGHT_VALUE = 6;

  private static final String QUEUE = "root.test";
  private static final QueuePath QUEUE_PATH = new QueuePath("root.test");
  private static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);

  private static final String ABSOLUTE_RESOURCE = String.format(
      ALL_RESOURCE_TEMPLATE, MEMORY_ABSOLUTE, VCORE_ABSOLUTE, GPU_ABSOLUTE);
  private static final String ABSOLUTE_RESOURCE_MEMORY_VCORE = String.format(
      MEMORY_VCORE_TEMPLATE, MEMORY_ABSOLUTE, VCORE_ABSOLUTE);
  private static final String MIXED_RESOURCE = String.format(
      ALL_RESOURCE_TEMPLATE, MEMORY_MIXED, PERCENTAGE_VALUE + "%", WEIGHT_VALUE + "w");
  private static final String RESOURCE_TYPES = GPU_URI;

  public static final String NONEXISTINGSUFFIX = "50nonexistingsuffix";
  public static final String EMPTY_BRACKET = "[]";
  public static final String INVALID_CAPACITY_BRACKET = "[invalid]";
  public static final String INVALID_CAPACITY_FORMAT = "[memory-100,vcores-60]";

  private final QueueCapacityConfigParser capacityConfigParser
      = new QueueCapacityConfigParser();

  @Test
  public void testPercentageCapacityConfig() {
    QueueCapacityVector percentageCapacityVector =
        capacityConfigParser.parse(Float.toString(PERCENTAGE_VALUE), QUEUE_PATH);
    QueueCapacityVectorEntry memory = percentageCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcore = percentageCapacityVector.getResource(VCORES_URI);

    assertEquals(ResourceUnitCapacityType.PERCENTAGE, memory.getVectorResourceType());
    assertEquals(PERCENTAGE_VALUE, memory.getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.PERCENTAGE, vcore.getVectorResourceType());
    assertEquals(PERCENTAGE_VALUE, vcore.getResourceValue(), EPSILON);

    QueueCapacityVector rootCapacityVector =
        capacityConfigParser.parse(Float.toString(PERCENTAGE_VALUE), ROOT);

    QueueCapacityVectorEntry memoryRoot = rootCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcoreRoot = rootCapacityVector.getResource(VCORES_URI);

    assertEquals(ResourceUnitCapacityType.PERCENTAGE, memoryRoot.getVectorResourceType());
    assertEquals(100f, memoryRoot.getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.PERCENTAGE, vcoreRoot.getVectorResourceType());
    assertEquals(100f, vcoreRoot.getResourceValue(), EPSILON);
  }

  @Test
  public void testWeightCapacityConfig() {
    QueueCapacityVector weightCapacityVector = capacityConfigParser.parse(WEIGHT_VALUE + "w",
        QUEUE_PATH);

    QueueCapacityVectorEntry memory = weightCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcore = weightCapacityVector.getResource(VCORES_URI);

    assertEquals(ResourceUnitCapacityType.WEIGHT, memory.getVectorResourceType());
    assertEquals(WEIGHT_VALUE, memory.getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.WEIGHT, vcore.getVectorResourceType());
    assertEquals(WEIGHT_VALUE, vcore.getResourceValue(), EPSILON);
  }

  @Test
  public void testAbsoluteCapacityVectorConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(getQueuePrefix(QUEUE_PATH) + CapacitySchedulerConfiguration.CAPACITY,
        ABSOLUTE_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector absoluteCapacityVector = capacityConfigParser.parse(ABSOLUTE_RESOURCE,
        QUEUE_PATH);

    assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        absoluteCapacityVector.getResource(MEMORY_URI).getVectorResourceType());
    assertEquals(12 * GB, absoluteCapacityVector.getResource(MEMORY_URI)
        .getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        absoluteCapacityVector.getResource(VCORES_URI).getVectorResourceType());
    assertEquals(VCORE_ABSOLUTE, absoluteCapacityVector.getResource(VCORES_URI)
        .getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        absoluteCapacityVector.getResource(GPU_URI).getVectorResourceType());
    assertEquals(GPU_ABSOLUTE, absoluteCapacityVector.getResource(GPU_URI)
        .getResourceValue(), EPSILON);

    QueueCapacityVector withoutGpuVector = capacityConfigParser
        .parse(ABSOLUTE_RESOURCE_MEMORY_VCORE, QUEUE_PATH);

    assertEquals(3, withoutGpuVector.getResourceCount());
    assertEquals(0f, withoutGpuVector.getResource(GPU_URI).getResourceValue(), EPSILON);
  }

  @Test
  public void testMixedCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector mixedCapacityVector =
        capacityConfigParser.parse(MIXED_RESOURCE, QUEUE_PATH);

    assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        mixedCapacityVector.getResource(MEMORY_URI).getVectorResourceType());
    assertEquals(MEMORY_MIXED, mixedCapacityVector.getResource(MEMORY_URI)
        .getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.PERCENTAGE,
        mixedCapacityVector.getResource(VCORES_URI).getVectorResourceType());
    assertEquals(PERCENTAGE_VALUE,
        mixedCapacityVector.getResource(VCORES_URI).getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.WEIGHT,
        mixedCapacityVector.getResource(GPU_URI).getVectorResourceType());
    assertEquals(WEIGHT_VALUE,
        mixedCapacityVector.getResource(GPU_URI).getResourceValue(), EPSILON);

    // Test undefined capacity type default value
    QueueCapacityVector mixedCapacityVectorWithGpuUndefined =
        capacityConfigParser.parse(ABSOLUTE_RESOURCE_MEMORY_VCORE, QUEUE_PATH);
    assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        mixedCapacityVectorWithGpuUndefined.getResource(MEMORY_URI).getVectorResourceType());
    assertEquals(0, mixedCapacityVectorWithGpuUndefined.getResource(GPU_URI)
        .getResourceValue(), EPSILON);

  }

  @Test
  public void testInvalidCapacityConfigs() {
    QueueCapacityVector capacityVectorWithInvalidSuffix =
        capacityConfigParser.parse(NONEXISTINGSUFFIX, QUEUE_PATH);
    List<QueueCapacityVectorEntry> entriesWithInvalidSuffix =
        Lists.newArrayList(capacityVectorWithInvalidSuffix.iterator());
    assertEquals(0, entriesWithInvalidSuffix.size());

    QueueCapacityVector invalidDelimiterCapacityVector =
        capacityConfigParser.parse(INVALID_CAPACITY_FORMAT, QUEUE_PATH);
    List<QueueCapacityVectorEntry> invalidDelimiterEntries =
        Lists.newArrayList(invalidDelimiterCapacityVector.iterator());
    assertEquals(0, invalidDelimiterEntries.size());

    QueueCapacityVector invalidCapacityVector =
        capacityConfigParser.parse(INVALID_CAPACITY_BRACKET, QUEUE_PATH);
    List<QueueCapacityVectorEntry> resources =
        Lists.newArrayList(invalidCapacityVector.iterator());
    assertEquals(0, resources.size());

    QueueCapacityVector emptyBracketCapacityVector =
        capacityConfigParser.parse(EMPTY_BRACKET, QUEUE_PATH);
    List<QueueCapacityVectorEntry> emptyEntries =
        Lists.newArrayList(emptyBracketCapacityVector.iterator());
    assertEquals(0, emptyEntries.size());

    QueueCapacityVector emptyCapacity =
        capacityConfigParser.parse("", QUEUE_PATH);
    List<QueueCapacityVectorEntry> emptyResources =
        Lists.newArrayList(emptyCapacity.iterator());
    assertEquals(emptyResources.size(), 0);

    QueueCapacityVector nonSetCapacity =
        capacityConfigParser.parse(null, QUEUE_PATH);
    List<QueueCapacityVectorEntry> nonSetResources =
        Lists.newArrayList(nonSetCapacity.iterator());
    assertEquals(nonSetResources.size(), 0);
  }

  @Test
  public void testZeroAbsoluteCapacityConfig() {
    QueueCapacityVector weightCapacityVector =
        capacityConfigParser.parse(String.format(MEMORY_VCORE_TEMPLATE, 0, 0), QUEUE_PATH);

    QueueCapacityVectorEntry memory = weightCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcore = weightCapacityVector.getResource(VCORES_URI);

    assertEquals(ResourceUnitCapacityType.ABSOLUTE, memory.getVectorResourceType());
    assertEquals(0, memory.getResourceValue(), EPSILON);

    assertEquals(ResourceUnitCapacityType.ABSOLUTE, vcore.getVectorResourceType());
    assertEquals(0, vcore.getResourceValue(), EPSILON);
  }
}
